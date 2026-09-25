"""执行器：有界并发地执行任务，并将失败归一化为可区分的原因。

* 并发度由 ``max_concurrency`` 限定（线程池大小）；
* 领取任务时执行 PENDING -> RUNNING 的乐观锁迁移，若任务在排队期间
  已被取消，则迁移失败、直接跳过，不会执行已取消的任务；
* 完成/失败迁移同样走乐观锁：与取消竞争时只有一个生效，
  保证"取消与完成几乎同时发生"时最终结论唯一；
* 处理器抛出的任何异常都被归一化为 ``HANDLER_ERROR``，超时被归一化为
  ``HANDLER_TIMEOUT``，内部异常细节只进服务端日志，不写入任务记录。
"""
from __future__ import annotations

import logging
import threading
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor

from .config import Settings
from .models import FailureReason, Task, TaskState
from .store import TaskStore

logger = logging.getLogger("taskpipe.executor")

Handler = Callable[[dict], object]


class Executor:
    """有界并发执行器。"""

    def __init__(
        self,
        store: TaskStore,
        settings: Settings,
        handler: Handler,
    ) -> None:
        self._store = store
        self._settings = settings
        self._handler = handler
        self._pool = ThreadPoolExecutor(
            max_workers=settings.max_concurrency,
            thread_name_prefix="taskpipe-worker",
        )

    # ------------------------------------------------------------------ 调度
    def dispatch(self, task: Task) -> None:
        """将任务交给执行池（非阻塞；池满时由调用方的容量控制兜底）。"""
        self._pool.submit(self._run, task.task_id)

    def dispatch_pending(self) -> int:
        """启动时重新派发所有仍处于 PENDING 的任务（中断恢复的一部分）。"""
        pending = [
            t for t in self._store.list_all() if t.state is TaskState.PENDING
        ]
        for task in pending:
            logger.info(
                "task=%s recovery: 重新派发 PENDING 任务", task.task_id
            )
            self.dispatch(task)
        return len(pending)

    def shutdown(self, wait: bool = True) -> None:
        self._pool.shutdown(wait=wait)

    # ------------------------------------------------------------------ 执行
    def _run(self, task_id: str) -> None:
        task = self._store.get(task_id)
        if task is None:
            logger.warning("task=%s 执行前未找到任务记录，跳过", task_id)
            return
        # 领取：PENDING -> RUNNING（乐观锁；排队期间被取消则领取失败）
        claimed = self._store.transition(
            task_id, task.version, TaskState.RUNNING
        )
        if claimed is None:
            current = self._store.get(task_id)
            logger.info(
                "task=%s decision=SKIP_EXECUTION state=%s "
                "(领取失败：任务在排队期间已离开 PENDING)",
                task_id, current.state.value if current else "?",
            )
            return

        result, error = self._invoke_with_timeout(task_id, claimed.payload)

        if error is None:
            done = self._store.transition(
                task_id, claimed.version, TaskState.SUCCEEDED,
                result=result, set_result=True,
            )
            if done is None:
                logger.info(
                    "task=%s decision=COMPLETE_LOST_RACE "
                    "(完成迁移与取消竞争失败，以取消为准，终态=CANCELLED)",
                    task_id,
                )
            return

        reason, log_exc = error
        if log_exc is not None:
            # 内部异常细节仅留在服务端日志，不写入任务记录、不向外透出
            logger.warning(
                "task=%s handler 内部异常（不对外透出）: %r", task_id, log_exc
            )
        failed = self._store.transition(
            task_id, claimed.version, TaskState.FAILED, failure_reason=reason,
        )
        if failed is None:
            logger.info(
                "task=%s decision=FAIL_LOST_RACE "
                "(失败迁移与取消竞争失败，以取消为准，终态=CANCELLED)",
                task_id,
            )

    def _invoke_with_timeout(
        self, task_id: str, payload: dict
    ) -> tuple[object, tuple[FailureReason, BaseException | None] | None]:
        """在超时约束下调用处理器，返回 (result, error)。

        error 为 ``(归一化原因, 原始异常或 None)``；超时线程为 daemon，
        超时后任务槽位立即释放，不会拖垮执行池。
        """
        box: dict[str, object] = {}

        def target() -> None:
            try:
                box["result"] = self._handler(payload)
            except BaseException as exc:  # noqa: BLE001 - 归一化边界
                box["exc"] = exc

        worker = threading.Thread(target=target, daemon=True)
        worker.start()
        worker.join(self._settings.handler_timeout)
        if worker.is_alive():
            logger.warning(
                "task=%s 执行超时 timeout=%ss -> FAILED/HANDLER_TIMEOUT",
                task_id, self._settings.handler_timeout,
            )
            return None, (FailureReason.HANDLER_TIMEOUT, None)
        if "exc" in box:
            return None, (FailureReason.HANDLER_ERROR, box["exc"])  # type: ignore[return-value]
        return box.get("result"), None
