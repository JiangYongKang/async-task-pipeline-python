"""执行与失败归一化测试：异常归类、内部细节不透出、有界并发、终态不可改写。"""
from __future__ import annotations

import logging
import threading
import time

import pytest

from taskpipe.config import Settings
from taskpipe.models import FailureReason, RejectReason, TaskState, TransitionError
from tests.conftest import wait_until

logger = logging.getLogger("taskpipe.test")


def test_handler_exception_normalized_not_exposed(make_stack, request_id):
    def handler(payload):
        raise RuntimeError("db password leaked: s3cret")

    store, service, executor, _ = make_stack(
        Settings(handler_timeout=5), handler=handler
    )
    task, _ = service.submit("k1", {}, request_id=request_id)
    executor.dispatch(task)
    wait_until(lambda: store.get(task.task_id).state is TaskState.FAILED)
    final = store.get(task.task_id)
    assert final.failure_reason is FailureReason.HANDLER_ERROR
    assert final.result is None
    # 内部异常细节不得出现在任务记录中
    record_text = repr(final)
    assert "s3cret" not in record_text and "password" not in record_text
    logger.info(
        "request=%s decision verified: 异常归一化为 %s，内部细节未透出 task=%s",
        request_id, final.failure_reason.value, task.task_id,
    )


def test_handler_timeout_normalized(make_stack, request_id):
    def handler(payload):
        time.sleep(5)

    store, service, executor, _ = make_stack(
        Settings(handler_timeout=0.2), handler=handler
    )
    task, _ = service.submit("k1", {}, request_id=request_id)
    executor.dispatch(task)
    wait_until(lambda: store.get(task.task_id).state is TaskState.FAILED)
    final = store.get(task.task_id)
    assert final.failure_reason is FailureReason.HANDLER_TIMEOUT
    logger.info(
        "request=%s decision verified: 超时归一化为 %s task=%s",
        request_id, final.failure_reason.value, task.task_id,
    )


def test_success_result_recorded(make_stack, request_id):
    store, service, executor, _ = make_stack(Settings(handler_timeout=5))
    task, _ = service.submit("k1", {"n": 7}, request_id=request_id)
    executor.dispatch(task)
    wait_until(lambda: store.get(task.task_id).state is TaskState.SUCCEEDED)
    final = store.get(task.task_id)
    assert final.result == {"echo": {"n": 7}}


def test_bounded_concurrency_respected(make_stack, request_id):
    current = 0
    peak = 0
    lock = threading.Lock()

    def handler(payload):
        nonlocal current, peak
        with lock:
            current += 1
            peak = max(peak, current)
        time.sleep(0.05)
        with lock:
            current -= 1
        return None

    store, service, executor, _ = make_stack(
        Settings(max_concurrency=2, max_pending=32, handler_timeout=5),
        handler=handler,
    )
    tasks = [
        service.submit(f"k{i}", {}, request_id=request_id)[0] for i in range(8)
    ]
    for t in tasks:
        executor.dispatch(t)
    executor.shutdown(wait=True)
    assert peak == 2  # 并发度恰好被限制在 max_concurrency
    for t in tasks:
        assert store.get(t.task_id).state is TaskState.SUCCEEDED
    logger.info(
        "request=%s decision verified: 峰值并发=%s 未超过 max_concurrency=2",
        request_id, peak,
    )


def test_terminal_state_cannot_be_overwritten(make_stack, request_id):
    store, service, executor, _ = make_stack(Settings(handler_timeout=5))
    task, _ = service.submit("k1", {}, request_id=request_id)
    executor.dispatch(task)
    wait_until(lambda: store.get(task.task_id).state is TaskState.SUCCEEDED)
    version = store.get(task.task_id).version

    # 终态后取消：被拒绝且原因可区分
    with pytest.raises(TransitionError) as exc:
        service.cancel(task.task_id, request_id=request_id)
    assert exc.value.reason is RejectReason.TERMINAL_STATE
    # 终态记录未被改写（状态与版本均不变）
    final = store.get(task.task_id)
    assert final.state is TaskState.SUCCEEDED
    assert final.version == version
    logger.info(
        "request=%s decision verified: 终态改写被拒绝 reason=%s version=%s 未变",
        request_id, exc.value.reason.value, version,
    )
