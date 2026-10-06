"""执行器：有界并发地执行任务，失败归一化，并按策略自动重试。

* 并发度由 ``max_concurrency`` 限定（线程池大小）；
* 领取任务时执行 PENDING/WAITING_RETRY -> RUNNING 的**原子领取**
  （状态迁移 + attempt_count 自增 + attempts 行同一事务），排队/等待
  重试期间被取消则领取失败、不会执行，也不会凭空多记一次尝试；
* 一次尝试结束后原子收尾：成功 -> SUCCEEDED；失败且策略允许 ->
  WAITING_RETRY（由调度线程到期重新领取）；失败且不再重试 -> FAILED；
* 完成/失败/重试与取消竞争同一把乐观锁：只有一个迁移生效，
  取消正好撞上某一次重试时最终结论也唯一；
* 处理器抛出的任何异常都被归一化：``NonRetryableError`` ->
  ``HANDLER_FATAL``（默认不可重试），其它异常 -> ``HANDLER_ERROR``，
  超时 -> ``HANDLER_TIMEOUT``，内部异常细节只进服务端日志。
"""
from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor

from .config import Settings
from .models import FailureReason, Task, TaskState
from .retry import NonRetryableError, delay_for, should_retry
from .store import TaskStore

logger = logging.getLogger("taskpipe.executor")

Handler = Callable[[dict], object]

# 调度线程轮询到点重试任务的间隔（秒）
_SCHEDULER_TICK = 0.02


class Executor:
    """有界并发执行器，含到期重试调度线程。"""

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
        # 已交给线程池、尚未结束（claim 成功/确认跳过）的任务，
        # 防止调度线程把同一 WAITING_RETRY 任务重复提交到池队列
        self._inflight: set[str] = set()
        self._inflight_lock = threading.Lock()
        self._stop = threading.Event()
        self._scheduler = threading.Thread(
            target=self._schedule_loop,
            name="taskpipe-retry-scheduler",
            daemon=True,
        )
        self._scheduler.start()

    # ------------------------------------------------------------------ 调度
    def dispatch(self, task: Task) -> None:
        """将任务交给执行池（非阻塞；池满时由调用方的容量控制兜底）。"""
        with self._inflight_lock:
            if task.task_id in self._inflight:
                return
            self._inflight.add(task.task_id)
        self._pool.submit(self._run, task.task_id)

    def _schedule_loop(self) -> None:
        """到期重试调度：周期性领取已到点的 WAITING_RETRY 任务重新执行。

        只负责 WAITING_RETRY；PENDING 仍由提交/恢复路径显式派发。
        """
        while not self._stop.wait(_SCHEDULER_TICK):
            try:
                due = self._store.list_due_retries()
            except Exception:  # 调度线程不能因查询异常退出
                logger.exception("retry scheduler: 查询到期任务失败，下轮重试")
                continue
            for task in due:
                if self._stop.is_set():
                    break
                self.dispatch(task)

    def retry_decider(
        self, task: Task, reason: FailureReason
    ) -> tuple[bool, float]:
        """启动恢复时的重试判定：被打断的那一次已计为一次尝试。

        :returns: ``(是否继续重试, 重试等待秒数)``。继续重试时恢复路径
            把任务置为 WAITING_RETRY 并由调度线程到期接着跑。
        """
        policy = task.to_policy()
        if should_retry(policy, task.attempt_count, reason):
            assert policy is not None
            return True, delay_for(policy, task.attempt_count)
        return False, 0.0

    def dispatch_pending(self) -> int:
        """启动时重新派发所有仍处于 PENDING 的任务（中断恢复的一部分）。

        WAITING_RETRY 任务无需在此派发：调度线程会按各自 next_run_at
        自动领取（到期点已过则下一轮立即执行）。
        """
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
        self._stop.set()
        self._pool.shutdown(wait=wait)

    # ------------------------------------------------------------------ 执行
    def _run(self, task_id: str) -> None:
        try:
            self._run_once(task_id)
        finally:
            with self._inflight_lock:
                self._inflight.discard(task_id)

    def _run_once(self, task_id: str) -> None:
        task = self._store.get(task_id)
        if task is None:
            logger.warning("task=%s 执行前未找到任务记录，跳过", task_id)
            return
        # 原子领取：PENDING/WAITING_RETRY -> RUNNING，并登记本次尝试。
        # 排队/等待期间被取消（版本变化）则整体失败，不执行也不计数。
        claimed = self._store.claim(task_id, task.version)
        if claimed is None:
            current = self._store.get(task_id)
            logger.info(
                "task=%s attempt=? decision=SKIP_EXECUTION state=%s "
                "(领取失败：任务在排队/等待重试期间已离开可领取状态)",
                task_id, current.state.value if current else "?",
            )
            return

        attempt_no = claimed.attempt_count
        result, error = self._invoke_with_timeout(
            task_id, claimed.payload, attempt_no
        )

        if error is None:
            done = self._store.finish_attempt(
                task_id, claimed.version, TaskState.SUCCEEDED,
                result=result, set_result=True,
            )
            if done is None:
                logger.info(
                    "task=%s attempt=%s decision=COMPLETE_LOST_RACE "
                    "(完成迁移与取消竞争失败，以取消为准，终态=CANCELLED)",
                    task_id, attempt_no,
                )
            return

        reason, log_exc = error
        if log_exc is not None:
            # 内部异常细节仅留在服务端日志，不写入任务记录、不向外透出
            logger.warning(
                "task=%s attempt=%s handler 内部异常（不对外透出）: %r",
                task_id, attempt_no, log_exc,
            )
        self._handle_failure(claimed, reason, attempt_no)

    def _handle_failure(
        self, task: Task, reason: FailureReason, attempt_no: int
    ) -> None:
        """失败归一化后的决策：继续等待重试，或落为最终失败。"""
        policy = task.to_policy()
        retry = should_retry(policy, attempt_no, reason)
        if retry:
            assert policy is not None
            delay = delay_for(policy, attempt_no)
            next_run_at = time.time() + delay
            waiting = self._store.finish_attempt(
                task.task_id, task.version, TaskState.WAITING_RETRY,
                failure_reason=reason, next_run_at=next_run_at,
            )
            if waiting is None:
                logger.info(
                    "task=%s attempt=%s decision=RETRY_WAIT_LOST_RACE "
                    "reason=%s (进入等待重试与取消竞争失败，以取消为准，"
                    "终态=CANCELLED)",
                    task.task_id, attempt_no, reason.value,
                )
                return
            logger.info(
                "task=%s attempt=%s decision=RETRY_WAIT reason=%s "
                "delay=%.3fs next_run_at=%.3f remaining=%s "
                "(失败可重试，进入等待，位置保留不占执行槽)",
                task.task_id, attempt_no, reason.value, delay,
                next_run_at, task.max_retries - attempt_no,
            )
            return

        failed = self._store.finish_attempt(
            task.task_id, task.version, TaskState.FAILED,
            failure_reason=reason,
        )
        if failed is None:
            logger.info(
                "task=%s attempt=%s decision=FAIL_LOST_RACE reason=%s "
                "(失败迁移与取消竞争失败，以取消为准，终态=CANCELLED)",
                task.task_id, attempt_no, reason.value,
            )
            return
        logger.info(
            "task=%s attempt=%s decision=FINAL_FAIL reason=%s "
            "(无策略/失败不可重试/次数已尽，落为最终失败)",
            task.task_id, attempt_no, reason.value,
        )

    def _invoke_with_timeout(
        self,
        task_id: str,
        payload: dict,
        attempt_no: int,
    ) -> tuple[object, tuple[FailureReason, BaseException | None] | None]:
        """在超时约束下调用处理器，返回 (result, error)。

        error 为 ``(归一化原因, 原始异常或 None)``；超时线程为 daemon，
        超时后任务槽位立即释放，不会拖垮执行池。
        """
        box: dict[str, object] = {}

        def target() -> None:
            try:
                box["result"] = self._handler(payload)
            except NonRetryableError as exc:
                box["fatal"] = exc
            except BaseException as exc:  # noqa: BLE001 - 归一化边界
                box["exc"] = exc

        worker = threading.Thread(target=target, daemon=True)
        worker.start()
        worker.join(self._settings.handler_timeout)
        if worker.is_alive():
            logger.warning(
                "task=%s attempt=%s 执行超时 timeout=%ss -> HANDLER_TIMEOUT",
                task_id, attempt_no, self._settings.handler_timeout,
            )
            return None, (FailureReason.HANDLER_TIMEOUT, None)
        if "fatal" in box:
            logger.warning(
                "task=%s attempt=%s handler 显式不可重试 -> HANDLER_FATAL",
                task_id, attempt_no,
            )
            return None, (FailureReason.HANDLER_FATAL, box["fatal"])  # type: ignore[return-value]
        if "exc" in box:
            return None, (FailureReason.HANDLER_ERROR, box["exc"])  # type: ignore[return-value]
        return box.get("result"), None
