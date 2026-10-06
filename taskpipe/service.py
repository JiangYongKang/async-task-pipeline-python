"""任务服务：幂等提交、容量控制、取消与查询。

容量规则：
* 被系统持有的未完成任务（``PENDING`` 排队中 + ``RETRY_WAITING`` 等待重试）
  总数达到 ``max_pending`` 时，新请求立即被拒绝（原因 ``OVER_CAPACITY``），
  不会无限排队；等待重试的任务始终保留自己的名额，不会被新请求挤掉；
* 相同 ``idempotency_key`` 的重复提交永远返回首次创建的任务，
  不受容量限制，也不产生重复任务（即使两次携带不同重试策略，
  也以首次提交为准）。
"""
from __future__ import annotations

import logging
import threading
import time
import uuid

from .config import Settings
from .models import AttemptRecord, RejectReason, RetryPolicy, Task, TaskState
from .state_machine import validate_transition
from .store import TaskStore

logger = logging.getLogger("taskpipe.service")


class ServiceRejected(Exception):
    """服务层拒绝异常，携带可区分的拒绝原因。"""

    def __init__(self, reason: RejectReason, message: str) -> None:
        super().__init__(message)
        self.reason = reason


class TaskService:
    """任务提交与状态管理入口。"""

    def __init__(self, store: TaskStore, settings: Settings) -> None:
        self._store = store
        self._settings = settings
        # 提交串行化，保证"容量判定 + 插入"作为整体原子执行
        self._submit_lock = threading.Lock()

    # ------------------------------------------------------------------ 提交
    def submit(
        self,
        idempotency_key: str,
        payload: dict,
        *,
        retry_policy: RetryPolicy | None = None,
        request_id: str | None = None,
    ) -> tuple[Task, bool]:
        """提交任务，可附带失败自动重试策略。

        不带 ``retry_policy``（或 ``max_retries=0``）的任务保持与旧版本
        完全一致的行为：失败即最终失败，不进入等待重试。

        :returns: ``(task, duplicated)``；``duplicated=True`` 表示命中
            幂等键并返回既有任务（重复提交，不新建、忽略新策略）。
        :raises ServiceRejected: 容量已满，原因 ``OVER_CAPACITY``。
        """
        request_id = request_id or str(uuid.uuid4())
        with self._submit_lock:
            existing = self._store.get_by_idempotency_key(idempotency_key)
            if existing is not None:
                logger.info(
                    "request=%s idempotency_key=%s decision=DEDUP "
                    "task=%s state=%s attempt=%s (重复提交，返回既有任务，"
                    "不占用容量，重试策略以首次为准)",
                    request_id, idempotency_key, existing.task_id,
                    existing.state.value, existing.attempt_no,
                )
                return existing, True

            held = self._count_held()
            if held >= self._settings.max_pending:
                logger.info(
                    "request=%s idempotency_key=%s decision=REJECT "
                    "reason=%s held=%s max_pending=%s (超出承载能力，"
                    "含等待重试任务保留的名额)",
                    request_id, idempotency_key,
                    RejectReason.OVER_CAPACITY.value, held,
                    self._settings.max_pending,
                )
                raise ServiceRejected(
                    RejectReason.OVER_CAPACITY,
                    f"at capacity: held(pending+retry_waiting)={held} >= "
                    f"max_pending={self._settings.max_pending}",
                )

            now = time.time()
            task = Task(
                task_id=str(uuid.uuid4()),
                idempotency_key=idempotency_key,
                payload=payload,
                created_at=now,
                updated_at=now,
                retry_policy=retry_policy,
                remaining_retries=(
                    retry_policy.max_retries if retry_policy is not None else 0
                ),
            )
            if not self._store.insert(task):
                # 极端并发下与重复提交竞争：以库内既有任务为准
                existing = self._store.get_by_idempotency_key(idempotency_key)
                assert existing is not None
                logger.info(
                    "request=%s idempotency_key=%s decision=DEDUP_AFTER_RACE "
                    "task=%s (插入冲突，返回既有任务)",
                    request_id, idempotency_key, existing.task_id,
                )
                return existing, True
            logger.info(
                "request=%s idempotency_key=%s decision=ACCEPT task=%s "
                "state=PENDING held_after=%s retry_policy=%s "
                "(容量判定通过)",
                request_id, idempotency_key, task.task_id, held + 1,
                retry_policy.to_dict() if retry_policy is not None else None,
            )
            return task, False

    # ------------------------------------------------------------------ 查询
    def get(self, task_id: str) -> Task | None:
        return self._store.get(task_id)

    def get_attempts(self, task_id: str) -> list[AttemptRecord]:
        """列出任务的全部尝试记录（第几次、何时跑、成功或失败分类、
        是否重试、下一次预计开始时间）。

        :raises ServiceRejected: 任务不存在，原因 ``TASK_NOT_FOUND``。
        """
        if self._store.get(task_id) is None:
            raise ServiceRejected(
                RejectReason.TASK_NOT_FOUND, f"task {task_id} not found"
            )
        return self._store.list_attempts(task_id)

    # ------------------------------------------------------------------ 取消
    def cancel(self, task_id: str, *, request_id: str | None = None) -> Task:
        """取消任务。

        * PENDING -> CANCELLED：直接迁移，执行器不会再领取；
        * RETRY_WAITING -> CANCELLED：等待重试期间可取消，定时器到期时
          领取（乐观锁）必定失败，不会再执行；
        * RUNNING -> CANCELLED：与执行器的完成/安排重试迁移竞争同一把
          乐观锁，取消成功则对方迁移必定失败，反之亦然，最终结论唯一；
        * 已在终态：抛出 ``TransitionError``（``TERMINAL_STATE``）。
        """
        request_id = request_id or str(uuid.uuid4())
        task = self._store.get(task_id)
        if task is None:
            logger.info(
                "request=%s task=%s decision=REJECT reason=%s",
                request_id, task_id, RejectReason.TASK_NOT_FOUND.value,
            )
            raise ServiceRejected(
                RejectReason.TASK_NOT_FOUND, f"task {task_id} not found"
            )
        # 校验迁移合法性（终态/非法迁移在此给出可区分原因）
        validate_transition(task.state, TaskState.CANCELLED)
        updated = self._store.transition(
            task_id, task.version, TaskState.CANCELLED
        )
        if updated is None:
            current = self._store.get(task_id)
            logger.info(
                "request=%s task=%s attempt=%s decision=CANCEL_LOST_RACE "
                "state=%s (乐观锁竞争失败，以对方迁移结果为准)",
                request_id, task_id, task.attempt_no,
                current.state.value if current else "?",
            )
            assert current is not None
            return current
        logger.info(
            "request=%s task=%s attempt=%s decision=CANCEL_ACCEPTED %s -> "
            "CANCELLED (取消迁移赢得乐观锁，最终结论唯一)",
            request_id, task_id, task.attempt_no, task.state.value,
        )
        return updated

    # ----------------------------------------------------------------- helpers
    def _count_held(self) -> int:
        """系统当前持有的名额：排队中 + 等待重试中的任务。

        RUNNING 占用的是并发槽（``max_concurrency``）而非队列名额；
        等待重试的任务保留名额，不被新提交挤掉。
        """
        return sum(
            1
            for t in self._store.list_all()
            if t.state in (TaskState.PENDING, TaskState.RETRY_WAITING)
        )
