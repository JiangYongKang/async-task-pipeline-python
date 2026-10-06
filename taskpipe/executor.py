"""执行器：有界并发地执行任务，失败归一化，并按策略自动重试。

* 并发度由 ``max_concurrency`` 限定（线程池大小）；
* 每次"领取一次尝试"都执行 (PENDING|RETRY_WAITING) -> RUNNING 的乐观锁
  迁移并落一条 IN_PROGRESS 尝试记录；排队或等待重试期间已被取消的任务，
  领取失败、直接跳过，不会执行已取消的任务；
* 一次尝试结束后：成功 -> SUCCEEDED；失败先按归一化原因与重试策略裁决，
  仍有预算且原因可重试 -> RETRY_WAITING 并挂定时器，到期后自动再跑；
  预算用尽或不可重试 -> 最终 FAILED；
* 完成/失败/安排重试迁移同样走乐观锁：与取消竞争时只有一个生效，
  保证"取消撞上某一次重试"时最终结论唯一；
* 处理器抛出的任何异常都被归一化为 ``HANDLER_ERROR``，超时被归一化为
  ``HANDLER_TIMEOUT``，内部异常细节只进服务端日志，不写入任务记录；
* 等待重试只持有一个轻量定时器（daemon 线程），不占用执行并发槽；
  重启后所有 RETRY_WAITING 任务按原 ``next_run_at`` 重新挂定时器。
"""
from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor

from .config import Settings
from .models import FailureReason, RetryPolicy, Task, TaskState
from .store import TaskStore

logger = logging.getLogger("taskpipe.executor")

Handler = Callable[[dict], object]


class Executor:
    """有界并发执行器，带失败自动重试。"""

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
        # task_id -> 等待重试定时器；只用于优雅关闭时清理，所有裁决仍以
        # 存储层乐观锁为准（定时器到期领取失败即安全放弃）。
        self._timers: dict[str, threading.Timer] = {}
        self._timers_lock = threading.Lock()
        self._stopping = False

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

    def schedule_retry_waiting(self, *, now: float | None = None) -> int:
        """启动恢复：为所有 RETRY_WAITING 任务按原计划重新挂重试定时器。

        间隔在停止期间已经过去的，定时器立即触发（错过的间隔不补等）。
        """
        now = time.time() if now is None else now
        waiting = self._store.list_retry_waiting()
        for task in waiting:
            assert task.next_run_at is not None
            delay = max(0.0, task.next_run_at - now)
            logger.info(
                "task=%s attempt=%s recovery: 重新挂等待重试定时器 "
                "next_run_at=%.3f delay=%.3fs remaining_retries=%s",
                task.task_id, task.attempt_no, task.next_run_at, delay,
                task.remaining_retries,
            )
            self._arm_timer(task.task_id, delay)
        return len(waiting)

    def shutdown(self, wait: bool = True) -> None:
        self._stopping = True
        with self._timers_lock:
            for timer in self._timers.values():
                timer.cancel()
            self._timers.clear()
        self._pool.shutdown(wait=wait)

    # -------------------------------------------------------------- 重试定时
    def _arm_timer(self, task_id: str, delay: float) -> None:
        """在 ``delay`` 秒后把下一次尝试提交进执行池（不占并发槽直到到期）。"""
        timer = threading.Timer(
            max(0.0, delay), self._timer_fired, args=(task_id,)
        )
        timer.daemon = True
        with self._timers_lock:
            if self._stopping:
                timer.cancel()
                return
            old = self._timers.get(task_id)
            if old is not None:
                old.cancel()
            self._timers[task_id] = timer
        timer.start()

    def _timer_fired(self, task_id: str) -> None:
        with self._timers_lock:
            self._timers.pop(task_id, None)
        if self._stopping:
            return
        task = self._store.get(task_id)
        if task is None or task.state is not TaskState.RETRY_WAITING:
            logger.info(
                "task=%s decision=SKIP_RETRY state=%s "
                "(重试定时器到期时任务已不在等待重试)",
                task_id, task.state.value if task else "?",
            )
            return
        logger.info(
            "task=%s attempt=%s decision=RETRY_DUE (等待结束，提交下一次尝试)",
            task_id, task.attempt_no,
        )
        self._pool.submit(self._run, task_id)

    # ------------------------------------------------------------------ 执行
    def _run(self, task_id: str) -> None:
        task = self._store.get(task_id)
        if task is None:
            logger.warning("task=%s 执行前未找到任务记录，跳过", task_id)
            return
        # 领取下一次尝试：(PENDING|RETRY_WAITING) -> RUNNING（乐观锁；
        # 排队/等待重试期间被取消则领取失败，且不会落尝试记录）
        claimed = self._store.start_attempt(task_id, task.version)
        if claimed is None:
            current = self._store.get(task_id)
            logger.info(
                "task=%s decision=SKIP_EXECUTION state=%s "
                "(领取失败：任务在排队/等待重试期间已离开可领取状态，"
                "通常为已取消或已被其他尝试领取)",
                task_id, current.state.value if current else "?",
            )
            return

        attempt_no = claimed.attempt_no
        result, error = self._invoke_with_timeout(
            task_id, attempt_no, claimed.payload
        )

        if error is None:
            done = self._store.finish_attempt_success(
                task_id, claimed.version, result
            )
            if done is None:
                logger.info(
                    "task=%s attempt=%s decision=COMPLETE_LOST_RACE "
                    "(成功迁移与取消竞争失败，以取消为准，终态=CANCELLED)",
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
        self._decide_after_failure(claimed, attempt_no, reason)

    def _decide_after_failure(
        self, task: Task, attempt_no: int, reason: FailureReason
    ) -> None:
        """一次尝试失败后的统一裁决：安排重试或落最终失败。

        与取消的竞争全部交给存储层乐观锁：安排重试/最终失败迁移失败时，
        说明取消已赢，最终结论唯一为 CANCELLED，不再挂任何定时器。
        """
        policy: RetryPolicy | None = task.retry_policy
        retryable = (
            policy is not None
            and policy.max_retries > 0
            and policy.is_retryable(reason)
            and task.remaining_retries > 0
        )
        if retryable:
            assert policy is not None
            upcoming = attempt_no + 1
            delay = policy.delay_for(upcoming)
            next_run_at = time.time() + delay
            scheduled = self._store.schedule_retry(
                task.task_id, task.version, reason,
                next_run_at=next_run_at, consume_retry=True,
            )
            if scheduled is None:
                logger.info(
                    "task=%s attempt=%s decision=RETRY_LOST_RACE "
                    "(安排重试与取消竞争失败，以取消为准，终态=CANCELLED)",
                    task.task_id, attempt_no,
                )
                return
            logger.info(
                "task=%s attempt=%s decision=RETRY_WAIT reason=%s "
                "upcoming_attempt=%s delay=%.3fs remaining_retries=%s "
                "判定依据=失败类型默认可重试且预算未用尽",
                task.task_id, attempt_no, reason.value, upcoming, delay,
                scheduled.remaining_retries,
            )
            self._arm_timer(task.task_id, delay)
            return

        basis = (
            "未携带重试策略，保持失败即终态" if policy is None
            else (
                "失败类型不在默认可重试范围" if not policy.is_retryable(reason)
                else "重试次数已用尽"
            )
        )
        failed = self._store.fail_attempt_final(
            task.task_id, task.version, reason
        )
        if failed is None:
            logger.info(
                "task=%s attempt=%s decision=FAIL_LOST_RACE "
                "(最终失败迁移与取消竞争失败，以取消为准，终态=CANCELLED)",
                task.task_id, attempt_no,
            )
            return
        logger.info(
            "task=%s attempt=%s decision=FINAL_FAILED reason=%s "
            "-> FAILED 判定依据=%s",
            task.task_id, attempt_no, reason.value, basis,
        )

    def _invoke_with_timeout(
        self, task_id: str, attempt_no: int, payload: dict
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

        worker = threading.Thread(
            target=target, daemon=True,
            name=f"taskpipe-handler-{task_id[:8]}-a{attempt_no}",
        )
        worker.start()
        worker.join(self._settings.handler_timeout)
        if worker.is_alive():
            logger.warning(
                "task=%s attempt=%s 执行超时 timeout=%ss -> HANDLER_TIMEOUT",
                task_id, attempt_no, self._settings.handler_timeout,
            )
            return None, (FailureReason.HANDLER_TIMEOUT, None)
        if "exc" in box:
            return None, (FailureReason.HANDLER_ERROR, box["exc"])  # type: ignore[return-value]
        return box.get("result"), None
