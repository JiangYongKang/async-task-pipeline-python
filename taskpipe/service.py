"""任务服务：幂等提交、容量控制、取消与查询。

容量规则：
* 队列中 PENDING 任务数达到 ``max_pending`` 时，新请求立即被拒绝
  （原因 ``OVER_CAPACITY``），不会无限排队；
* 相同 ``idempotency_key`` 的重复提交永远返回首次创建的任务，
  不受容量限制，也不产生重复任务。
"""
from __future__ import annotations

import logging
import threading
import time
import uuid

from .config import Settings
from .models import RejectReason, Task, TaskState, TransitionError
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
        request_id: str | None = None,
    ) -> tuple[Task, bool]:
        """提交任务。

        :returns: ``(task, duplicated)``；``duplicated=True`` 表示命中
            幂等键并返回既有任务（重复提交，不新建）。
        :raises ServiceRejected: 容量已满，原因 ``OVER_CAPACITY``。
        """
        request_id = request_id or str(uuid.uuid4())
        with self._submit_lock:
            existing = self._store.get_by_idempotency_key(idempotency_key)
            if existing is not None:
                logger.info(
                    "request=%s idempotency_key=%s decision=DEDUP "
                    "task=%s state=%s (重复提交，返回既有任务，不占用容量)",
                    request_id, idempotency_key, existing.task_id,
                    existing.state.value,
                )
                return existing, True

            pending = self._count_pending()
            if pending >= self._settings.max_pending:
                logger.info(
                    "request=%s idempotency_key=%s decision=REJECT "
                    "reason=%s pending=%s max_pending=%s (超出承载能力)",
                    request_id, idempotency_key,
                    RejectReason.OVER_CAPACITY.value, pending,
                    self._settings.max_pending,
                )
                raise ServiceRejected(
                    RejectReason.OVER_CAPACITY,
                    f"at capacity: pending={pending} >= "
                    f"max_pending={self._settings.max_pending}",
                )

            now = time.time()
            task = Task(
                task_id=str(uuid.uuid4()),
                idempotency_key=idempotency_key,
                payload=payload,
                created_at=now,
                updated_at=now,
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
                "state=PENDING pending_after=%s (容量判定通过)",
                request_id, idempotency_key, task.task_id, pending + 1,
            )
            return task, False

    # ------------------------------------------------------------------ 查询
    def get(self, task_id: str) -> Task | None:
        return self._store.get(task_id)

    # ------------------------------------------------------------------ 取消
    def cancel(self, task_id: str, *, request_id: str | None = None) -> Task:
        """取消任务。

        * PENDING -> CANCELLED：直接迁移，执行器不会再领取；
        * RUNNING -> CANCELLED：与执行器的完成迁移竞争同一把乐观锁，
          取消成功则完成迁移必定失败，反之亦然，最终结论唯一；
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
                "request=%s task=%s decision=CANCEL_LOST_RACE state=%s "
                "(乐观锁竞争失败，以对方迁移结果为准)",
                request_id, task_id, current.state.value if current else "?",
            )
            assert current is not None
            return current
        logger.info(
            "request=%s task=%s decision=CANCEL_ACCEPTED %s -> CANCELLED "
            "(取消迁移赢得乐观锁)",
            request_id, task_id, task.state.value,
        )
        return updated

    # ----------------------------------------------------------------- helpers
    def _count_pending(self) -> int:
        return sum(
            1 for t in self._store.list_all() if t.state is TaskState.PENDING
        )
