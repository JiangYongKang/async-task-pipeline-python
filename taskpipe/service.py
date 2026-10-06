"""任务服务：幂等提交、容量控制、取消、尝试追溯与查询。

容量规则：
* 处于"未完成且未执行中"的任务（PENDING 与等待重试 WAITING_RETRY）
  共同占用队列容量。等待重试的任务保留自己的位置：不会被新请求挤掉，
  也不会被容量判定丢弃；
* 这类占用数达到 ``max_pending`` 时，**新请求**立即被拒绝
  （原因 ``OVER_CAPACITY``），不会无限排队；
* 重试是已有任务的后续，不是新请求，进入/离开 WAITING_RETRY 都不再
  经过容量判定，因此重试任务永远不会把自己挤掉；
* 相同 ``idempotency_key`` 的重复提交永远返回首次创建的任务，
  不受容量限制，也不产生重复任务。
"""
from __future__ import annotations

import logging
import threading
import time
import uuid

from .config import Settings
from .models import (
    Attempt,
    BackoffKind,
    RejectReason,
    RetryPolicy,
    Task,
    TaskState,
    TransitionError,
)
from .state_machine import validate_transition
from .store import TaskStore

logger = logging.getLogger("taskpipe.service")

# 占用队列容量的状态：排队中 + 等待重试中
_SLOT_STATES: frozenset[TaskState] = frozenset(
    {TaskState.PENDING, TaskState.WAITING_RETRY}
)


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
        request_id: str | None = None,
        retry_policy: RetryPolicy | None = None,
    ) -> tuple[Task, bool]:
        """提交任务。

        :param retry_policy: 可选的失败自动重试策略（最多重试次数与间隔）。
            None 或 ``max_retries=0`` 表示不带策略，失败即最终失败，
            行为与原有逻辑完全一致。重试策略随任务持久化；相同幂等键的
            重复提交仍只对应首次创建的那个任务（连同其策略）。
        :returns: ``(task, duplicated)``；``duplicated=True`` 表示命中
            幂等键并返回既有任务（重复提交，不新建）。
        :raises ServiceRejected: 容量已满，原因 ``OVER_CAPACITY``。
        """
        request_id = request_id or str(uuid.uuid4())
        if retry_policy is not None:
            if retry_policy.max_retries < 0:
                raise ValueError("retry_policy.max_retries 不能为负")
            if retry_policy.interval < 0:
                raise ValueError("retry_policy.interval 不能为负")
        with self._submit_lock:
            existing = self._store.get_by_idempotency_key(idempotency_key)
            if existing is not None:
                logger.info(
                    "request=%s idempotency_key=%s decision=DEDUP "
                    "task=%s state=%s attempt=%s (重复提交，返回既有任务，"
                    "不占用容量，策略沿用首次提交)",
                    request_id, idempotency_key, existing.task_id,
                    existing.state.value, existing.attempt_count,
                )
                return existing, True

            pending = self._count_slots()
            if pending >= self._settings.max_pending:
                logger.info(
                    "request=%s idempotency_key=%s decision=REJECT "
                    "reason=%s occupied=%s max_pending=%s (超出承载能力，"
                    "PENDING+WAITING_RETRY 共同计容量)",
                    request_id, idempotency_key,
                    RejectReason.OVER_CAPACITY.value, pending,
                    self._settings.max_pending,
                )
                raise ServiceRejected(
                    RejectReason.OVER_CAPACITY,
                    f"at capacity: occupied(pending+waiting_retry)={pending} "
                    f">= max_pending={self._settings.max_pending}",
                )

            now = time.time()
            policy = retry_policy
            task = Task(
                task_id=str(uuid.uuid4()),
                idempotency_key=idempotency_key,
                payload=payload,
                created_at=now,
                updated_at=now,
                max_retries=policy.max_retries if policy else 0,
                retry_interval=policy.interval if policy else 0.0,
                backoff=(
                    policy.backoff if policy else BackoffKind.FIXED
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
                "state=PENDING retry=(max=%s interval=%s backoff=%s) "
                "occupied_after=%s (容量判定通过)",
                request_id, idempotency_key, task.task_id,
                task.max_retries, task.retry_interval, task.backoff.value,
                pending + 1,
            )
            return task, False

    # ------------------------------------------------------------------ 查询
    def get(self, task_id: str) -> Task | None:
        return self._store.get(task_id)

    def get_attempts(self, task_id: str) -> list[Attempt]:
        """返回任务的全部尝试记录（按尝试次序升序）。

        :raises ServiceRejected: 任务不存在，原因 ``TASK_NOT_FOUND``。
        """
        task = self._store.get(task_id)
        if task is None:
            raise ServiceRejected(
                RejectReason.TASK_NOT_FOUND, f"task {task_id} not found"
            )
        return self._store.list_attempts(task_id)

    # ------------------------------------------------------------------ 取消
    def cancel(self, task_id: str, *, request_id: str | None = None) -> Task:
        """取消任务。

        * PENDING / WAITING_RETRY -> CANCELLED：直接迁移，执行器不会再
          领取（等待重试中的任务也可在等待期间取消）；
        * RUNNING -> CANCELLED：与执行器的完成/重试迁移竞争同一把乐观锁，
          并把进行中的那一次尝试一并收尾为 CANCELLED；取消成功则执行器
          的收尾必定失败，反之亦然，最终结论唯一；
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
        updated = self._store.cancel(task_id, task.version)
        if updated is None:
            current = self._store.get(task_id)
            logger.info(
                "request=%s task=%s decision=CANCEL_LOST_RACE state=%s "
                "attempt=%s (乐观锁竞争失败，以对方迁移结果为准)",
                request_id, task_id, current.state.value if current else "?",
                current.attempt_count if current else "?",
            )
            assert current is not None
            return current
        logger.info(
            "request=%s task=%s decision=CANCEL_ACCEPTED %s -> CANCELLED "
            "attempt=%s (取消迁移赢得乐观锁，最终结论唯一)",
            request_id, task_id, task.state.value, updated.attempt_count,
        )
        return updated

    # ----------------------------------------------------------------- helpers
    def _count_slots(self) -> int:
        """统计占用队列容量的任务数：PENDING + WAITING_RETRY。"""
        return sum(
            1 for t in self._store.list_all() if t.state in _SLOT_STATES
        )
