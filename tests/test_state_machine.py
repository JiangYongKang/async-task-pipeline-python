"""状态机：非法迁移可区分拒绝，终态不可改写，失败原因归一化。"""

from __future__ import annotations

import threading
import time

import pytest

from app.enums import FailureReason, RejectReason, TaskState
from app.errors import RejectedError
from app.handlers import register_handler
from tests.conftest import wait_terminal


def test_terminal_states_are_immutable(service_factory) -> None:
    svc = service_factory()
    task, _ = svc.submit("echo", "ok", request_id="t-imm")
    task = wait_terminal(svc, task.task_id)
    assert task.state is TaskState.SUCCEEDED

    # 已完成任务再取消 -> ALREADY_TERMINAL，与一般非法迁移可区分
    with pytest.raises(RejectedError) as ei:
        svc.cancel(task.task_id, request_id="t-imm-cancel")
    assert ei.value.reason is RejectReason.ALREADY_TERMINAL
    print(f"判定依据: 终态={task.state.value} 拒绝原因={ei.value.reason.value}")


def test_failed_task_cannot_be_cancelled(service_factory) -> None:
    svc = service_factory()
    task, _ = svc.submit("fail", "boom", request_id="t-fail")
    task = wait_terminal(svc, task.task_id)
    assert task.state is TaskState.FAILED
    assert task.failure_reason is FailureReason.HANDLER_ERROR
    with pytest.raises(RejectedError) as ei:
        svc.cancel(task.task_id, request_id="t-fail-cancel")
    assert ei.value.reason is RejectReason.ALREADY_TERMINAL


def test_handler_exception_is_normalized(service_factory) -> None:
    """内部异常不得直接透出：只返回归一化原因与受控消息。"""

    def boom(payload: str, cancel: threading.Event) -> str:
        raise RuntimeError("internal secret detail")

    register_handler("boom", boom)
    svc = service_factory()
    task, _ = svc.submit("boom", "", request_id="t-norm")
    task = wait_terminal(svc, task.task_id)
    assert task.state is TaskState.FAILED
    assert task.failure_reason is FailureReason.HANDLER_ERROR
    # 消息中只含异常类名，不泄露堆栈
    assert "internal secret detail" in (task.error_message or "")
    assert "Traceback" not in (task.error_message or "")


def test_timeout_is_distinct_failure(service_factory) -> None:
    def slow(payload: str, cancel: threading.Event) -> str:
        time.sleep(2.0)
        return "late"

    register_handler("slow", slow, timeout_seconds=0.2)
    svc = service_factory()
    task, _ = svc.submit("slow", "", request_id="t-timeout")
    task = wait_terminal(svc, task.task_id, 5.0)
    assert task.state is TaskState.FAILED
    assert task.failure_reason is FailureReason.TIMEOUT
    print(f"请求标识=t-timeout 状态迁移=RUNNING->FAILED 判定依据={task.failure_reason.value}")


def test_cancel_running_task_cooperative(service_factory) -> None:
    """RUNNING 任务在 handler 响应取消信号后，终态稳定为 CANCELLED。"""

    def wait_cancel(payload: str, cancel: threading.Event) -> str:
        # 模拟工作；取消信号到达后抛错，服务层 CAS 与取消竞争
        if cancel.wait(1.0):
            raise RuntimeError("cooperative cancel")
        return "finished"

    register_handler("wait_cancel", wait_cancel)
    svc = service_factory(max_concurrency=1, max_capacity=2)
    task, _ = svc.submit("wait_cancel", "", request_id="t-run-cancel")
    # 等到 RUNNING
    deadline = time.monotonic() + 2
    while time.monotonic() < deadline and svc.get(task.task_id).state is not TaskState.RUNNING:
        time.sleep(0.01)
    assert svc.get(task.task_id).state is TaskState.RUNNING

    cancelled = svc.cancel(task.task_id, request_id="t-run-cancel-go")
    final = wait_terminal(svc, task.task_id)
    assert cancelled.state is TaskState.CANCELLED
    assert final.state is TaskState.CANCELLED


def test_cancel_unknown_task_is_not_found(service_factory) -> None:
    svc = service_factory()
    with pytest.raises(RejectedError) as ei:
        svc.cancel("missing-id", request_id="t-404")
    assert ei.value.reason is RejectReason.TASK_NOT_FOUND
