"""任务编排服务。

并发正确性的三条主线：

1. 提交（幂等 + 容量）：全局锁内完成 handler 校验 → 幂等键查重 →
   容量检查 → 持久化 → 入队。要么拿到既有任务（replayed=True），
   要么要么满容量被明确拒绝，绝不会产生重复任务，也不会无限排队。
2. 状态迁移（唯一裁决）：所有迁移走 repository 的 CAS 条件更新。
   取消与完成同时发生时，数据库只允许一条迁移提交：
   - QUEUED 任务取消成功：worker 出队后发现已终态，空转释放容量；
   - RUNNING 上完成与取消竞争：expected={RUNNING} 的两条 CAS，先到者赢。
3. 中断恢复：initialize 时把残留 RUNNING 归为 FAILED/INTERRUPTED，
   残留 QUEUED 重新入队。重启后每个任务状态确定且可查询。
"""

from __future__ import annotations

import logging
import threading
import uuid

from .enums import FailureReason, RejectReason, TaskState
from .errors import RejectedError, TaskNotFoundError
from .executor import BoundedExecutor, CapacityFullError
from .handlers import get_handler
from .logging_setup import get_logger
from .models import Task
from .repository import TaskRepository
from .settings import Settings

_MAX_ERROR_MESSAGE_CHARS = 200


class TaskService:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._repo = TaskRepository(settings)
        self._executor = BoundedExecutor(
            settings.max_concurrency, settings.max_capacity
        )
        self._lock = threading.Lock()
        # task_id -> cancel_event：RUNNING 任务的协作式取消信号
        self._cancel_events: dict[str, threading.Event] = {}
        self._log = get_logger("pipeline.service")
        self._stopping = False
        self._shutdown_done = False

    # ---------- 生命周期 ----------
    def initialize(self) -> None:
        self._repo.initialize()
        self._executor.start()
        self._recover_on_startup()

    def shutdown(self) -> None:
        """优雅停机：先把所有 QUEUED 任务置为 CANCELLED（worker 出队即跳过），
        再通知 RUNNING 任务协作退出，最后等在执行的任务走完终态，
        保证不残留悬空状态。"""
        if self._shutdown_done:
            return
        self._shutdown_done = True
        with self._lock:
            self._stopping = True
        self._cancel_active_on_shutdown()
        self._log.info("shutdown: waiting in-flight tasks to settle")
        self._executor.shutdown(wait=True)
        self._repo.close()

    def _cancel_active_on_shutdown(self) -> None:
        for task in self._repo.list_interrupted_on_startup():
            event = None
            with self._lock:
                event = self._cancel_events.get(task.task_id)
            try:
                self._repo.cas_state(
                    task.task_id,
                    frozenset({TaskState.QUEUED, TaskState.RUNNING}),
                    TaskState.CANCELLED,
                )
                self._log.info(
                    "shutdown task=%s transition=%s->CANCELLED"
                    " reason=graceful-stop",
                    task.task_id, task.state.value,
                )
            except RejectedError:
                pass  # 已在终态
            if event is not None:
                event.set()

    # ---------- 提交 ----------
    def submit(
        self,
        handler: str,
        payload: str,
        idempotency_key: str | None = None,
        request_id: str = "-",
    ) -> tuple[Task, bool]:
        """提交任务，返回 ``(task, replayed)``。

        replayed=True 表示幂等键命中既有任务（不产生重复任务）。
        """
        entry = get_handler(handler)
        if entry is None:
            self._log.warning(
                "submit rejected req=%s handler=%r reason=%s",
                request_id, handler, RejectReason.INVALID_HANDLER.value,
            )
            raise RejectedError(
                RejectReason.INVALID_HANDLER,
                f"handler {handler!r} is not registered",
            )
        if not isinstance(payload, str):
            raise RejectedError(
                RejectReason.INVALID_PAYLOAD, "payload must be a string"
            )

        with self._lock:
            if self._stopping:
                raise RejectedError(
                    RejectReason.CAPACITY_FULL, "service is shutting down"
                )

            if idempotency_key:
                existing = self._repo.get_by_idempotency_key(idempotency_key)
                if existing is not None:
                    self._log.info(
                        "submit idempotent-hit req=%s task=%s idem=%s state=%s",
                        request_id, existing.task_id, idempotency_key,
                        existing.state.value,
                    )
                    return existing, True

            # 锁内“先持久化后入队”，每个在执行/排队任务都已是 QUEUED/RUNNING
            # 持久记录，因此仓库计数即权威在制数；恢复时重新入队的任务也已计入。
            active = self._repo.count_active()
            if active >= self._settings.max_capacity:
                self._log.warning(
                    "submit rejected req=%s handler=%s reason=%s active=%d cap=%d",
                    request_id, handler, RejectReason.CAPACITY_FULL.value,
                    active, self._settings.max_capacity,
                )
                raise RejectedError(
                    RejectReason.CAPACITY_FULL,
                    f"active={active} capacity={self._settings.max_capacity}",
                )

            task_id = uuid.uuid4().hex
            task = self._repo.create_task(task_id, idempotency_key, handler, payload)
            if task is None:
                # 理论上已在锁内查重；兜底：唯一键冲突时重读
                existing = self._repo.get_by_idempotency_key(idempotency_key or "")
                assert existing is not None
                self._log.info(
                    "submit idempotent-hit(race) req=%s task=%s idem=%s",
                    request_id, existing.task_id, idempotency_key,
                )
                return existing, True

            try:
                self._executor.submit(self._execute, task_id)
            except CapacityFullError:
                self._log.error(
                    "submit accepted but executor full task=%s; failing it", task_id
                )
                task = self._repo.cas_state(
                    task_id,
                    frozenset({TaskState.QUEUED}),
                    TaskState.FAILED,
                    failure_reason=FailureReason.HANDLER_ERROR,
                    error_message="executor rejected after persist (should not happen)",
                )
                return task, False

            self._log.info(
                "submit accepted req=%s task=%s idem=%s handler=%s"
                " transition=->QUEUED reason=capacity-ok active=%d",
                request_id, task_id, idempotency_key, handler,
                self._repo.count_active(),
            )
            return task, False

    # ---------- 取消 ----------
    def cancel(self, task_id: str, request_id: str = "-") -> Task:
        task = self.get(task_id)  # 不存在 -> TASK_NOT_FOUND

        with self._lock:
            event = self._cancel_events.get(task_id)

        try:
            updated = self._repo.cas_state(
                task_id,
                frozenset({TaskState.QUEUED, TaskState.RUNNING}),
                TaskState.CANCELLED,
            )
        except RejectedError as exc:
            self._log.info(
                "cancel rejected req=%s task=%s state=%s reason=%s",
                request_id, task_id, task.state.value, exc.reason.value,
            )
            raise

        if event is not None:
            event.set()
        self._log.info(
            "cancel accepted req=%s task=%s transition=%s->CANCELLED"
            " reason=cas-won",
            request_id, task_id, task.state.value,
        )
        return updated

    # ---------- 查询 ----------
    def get(self, task_id: str) -> Task:
        task = self._repo.get(task_id)
        if task is None:
            raise TaskNotFoundError(task_id)
        return task

    # ---------- 执行（工作线程） ----------
    def _execute(self, task_id: str) -> None:
        # 出队时可能已被取消（QUEUED -> CANCELLED）：跳过执行，容量由
        # executor 的 finally 释放。
        task = self._repo.get(task_id)
        if task is None or task.state.is_terminal:
            self._log.info(
                "worker skip task=%s state=%s reason=already-terminal-before-run",
                task_id, task.state.value if task else "missing",
            )
            return
        try:
            running = self._repo.cas_state(
                task_id, frozenset({TaskState.QUEUED}), TaskState.RUNNING
            )
        except RejectedError as exc:
            self._log.info(
                "worker cannot-start task=%s reason=%s", task_id, exc.reason.value
            )
            return

        entry = get_handler(running.handler)
        cancel_event = threading.Event()
        with self._lock:
            self._cancel_events[task_id] = cancel_event
        timeout = (
            entry.timeout_seconds if entry and entry.timeout_seconds is not None
            else self._settings.default_timeout_seconds
        )

        result_box: dict[str, object] = {}
        done_event = threading.Event()

        def runner() -> None:
            try:
                result_box["result"] = entry.fn(  # type: ignore[union-attr]
                    running.payload, cancel_event
                )
            except BaseException as exc:  # noqa: BLE001 - 归一化所有内部异常
                result_box["error"] = exc
            finally:
                done_event.set()

        worker = threading.Thread(
            target=runner, name=f"handler-{task_id[:8]}", daemon=True
        )
        worker.start()
        finished = done_event.wait(timeout)

        try:
            if not finished:
                # 超时：硬超时，handler 线程为 daemon 不阻塞进程退出
                self._finish_failed(
                    task_id,
                    FailureReason.TIMEOUT,
                    f"execution exceeded {timeout}s",
                )
                self._log.warning(
                    "task=%s transition=RUNNING->FAILED reason=timeout %ss",
                    task_id, timeout,
                )
                return

            if "error" in result_box:
                exc = result_box["error"]
                self._finish_failed(
                    task_id,
                    FailureReason.HANDLER_ERROR,
                    f"{type(exc).__name__}: {str(exc)[:_MAX_ERROR_MESSAGE_CHARS]}",
                    log_exc=exc,
                )
                return

            try:
                final = self._repo.cas_state(
                    task_id,
                    frozenset({TaskState.RUNNING}),
                    TaskState.SUCCEEDED,
                    result=str(result_box.get("result", "")),
                )
                self._log.info(
                    "task=%s transition=RUNNING->SUCCEEDED reason=cas-won", task_id
                )
                return final
            except RejectedError as exc:
                # 与取消竞争失败：取消已提交，结论 CANCELLED 唯一且稳定
                self._log.info(
                    "task=%s completion lost against cancel reason=%s",
                    task_id, exc.reason.value,
                )
                return
        finally:
            with self._lock:
                self._cancel_events.pop(task_id, None)

    def _finish_failed(
        self,
        task_id: str,
        reason: FailureReason,
        message: str,
        *,
        log_exc: BaseException | None = None,
    ) -> Task:
        try:
            task = self._repo.cas_state(
                task_id,
                frozenset({TaskState.RUNNING}),
                TaskState.FAILED,
                failure_reason=reason,
                error_message=message,
            )
            self._log.warning(
                "task=%s transition=RUNNING->FAILED reason=%s detail=%s",
                task_id, reason.value, message,
                exc_info=log_exc,
            )
            return task
        except RejectedError as exc:
            # 与取消竞争失败：取消结论稳定
            self._log.info(
                "task=%s failure lost against cancel reason=%s",
                task_id, exc.reason.value,
            )
            return self.get(task_id)

    # ---------- 启动恢复 ----------
    def _recover_on_startup(self) -> None:
        stale = self._repo.list_interrupted_on_startup()
        requeued, interrupted = 0, 0
        for task in stale:
            if task.state is TaskState.QUEUED:
                try:
                    self._executor.submit(self._execute, task.task_id)
                    requeued += 1
                    self._log.info(
                        "recovery task=%s state=QUEUED action=requeue", task.task_id
                    )
                except CapacityFullError:
                    # capacity >= concurrency，刚启动不可能满；兜底标记失败
                    self._repo.cas_state(
                        task.task_id,
                        frozenset({TaskState.QUEUED}),
                        TaskState.FAILED,
                        failure_reason=FailureReason.INTERRUPTED,
                        error_message="could not requeue on recovery",
                    )
            else:  # RUNNING：进程被杀死，结果不可知 -> 确定终态
                self._repo.cas_state(
                    task.task_id,
                    frozenset({TaskState.RUNNING}),
                    TaskState.FAILED,
                    failure_reason=FailureReason.INTERRUPTED,
                    error_message="worker was interrupted by process stop",
                )
                interrupted += 1
                self._log.warning(
                    "recovery task=%s transition=RUNNING->FAILED"
                    " reason=INTERRUPTED detail=process-stop",
                    task.task_id,
                )
        self._log.info(
            "recovery complete requeued=%d interrupted=%d", requeued, interrupted
        )
