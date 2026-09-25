"""取消测试：排队取消、执行中取消、终态拒绝、取消与完成竞态的唯一性。"""
from __future__ import annotations

import logging
import threading
import time

import pytest

from taskpipe.config import Settings
from taskpipe.models import RejectReason, TaskState, TransitionError
from taskpipe.service import ServiceRejected
from tests.conftest import wait_until

logger = logging.getLogger("taskpipe.test")


def test_cancel_pending_task_is_never_executed(make_stack, request_id):
    executed: list[str] = []

    def handler(payload):
        executed.append("ran")
        return None

    store, service, executor, _ = make_stack(
        Settings(handler_timeout=5), handler=handler
    )
    task, _ = service.submit("k1", {}, request_id=request_id)
    cancelled = service.cancel(task.task_id, request_id=request_id)
    assert cancelled.state is TaskState.CANCELLED
    executor.dispatch(task)  # 即便被派发，领取时也必须跳过
    executor.shutdown(wait=True)
    assert executed == []
    assert store.get(task.task_id).state is TaskState.CANCELLED
    logger.info(
        "request=%s decision verified: 排队中取消后执行器跳过执行 task=%s",
        request_id, task.task_id,
    )


def test_cancel_terminal_task_rejected_with_terminal_reason(
    make_stack, request_id
):
    store, service, executor, _ = make_stack(Settings(handler_timeout=5))
    task, _ = service.submit("k1", {}, request_id=request_id)
    executor.dispatch(task)
    wait_until(lambda: store.get(task.task_id).state is TaskState.SUCCEEDED)
    with pytest.raises(TransitionError) as exc:
        service.cancel(task.task_id, request_id=request_id)
    assert exc.value.reason is RejectReason.TERMINAL_STATE
    # 终态未被改写
    assert store.get(task.task_id).state is TaskState.SUCCEEDED


def test_cancel_unknown_task_rejected(make_stack, request_id):
    _, service, _, _ = make_stack()
    with pytest.raises(ServiceRejected) as exc:
        service.cancel("no-such-task", request_id=request_id)
    assert exc.value.reason is RejectReason.TASK_NOT_FOUND


def test_cancel_running_task_wins_race(make_stack, request_id):
    # 处理器阻塞在事件上：取消先完成，随后处理器才返回，
    # 完成迁移必须输掉竞态，终态唯一为 CANCELLED。
    release = threading.Event()

    def handler(payload):
        release.wait(5)
        return {"done": True}

    store, service, executor, _ = make_stack(
        Settings(handler_timeout=5), handler=handler
    )
    task, _ = service.submit("k1", {}, request_id=request_id)
    executor.dispatch(task)
    wait_until(lambda: store.get(task.task_id).state is TaskState.RUNNING)

    cancelled = service.cancel(task.task_id, request_id=request_id)
    assert cancelled.state is TaskState.CANCELLED

    release.set()  # 处理器此刻才返回，完成迁移应输掉竞态
    executor.shutdown(wait=True)
    final = store.get(task.task_id)
    assert final.state is TaskState.CANCELLED
    assert final.result is None
    logger.info(
        "request=%s decision verified: 取消赢得竞态，终态唯一=CANCELLED task=%s",
        request_id, task.task_id,
    )


def test_complete_then_cancel_loses_race(make_stack, request_id):
    # 处理器立即完成：完成迁移先赢得乐观锁，取消必须输掉竞态，
    # 最终结论唯一为 SUCCEEDED。
    store, service, executor, _ = make_stack(Settings(handler_timeout=5))
    task, _ = service.submit("k1", {}, request_id=request_id)
    executor.dispatch(task)
    wait_until(lambda: store.get(task.task_id).state is TaskState.SUCCEEDED)

    # 任务已达终态：取消被明确拒绝（TERMINAL_STATE），终态不被改写
    with pytest.raises(TransitionError) as exc:
        service.cancel(task.task_id, request_id=request_id)
    assert exc.value.reason is RejectReason.TERMINAL_STATE
    assert store.get(task.task_id).state is TaskState.SUCCEEDED
    logger.info(
        "request=%s decision verified: 完成赢得竞态，终态唯一=SUCCEEDED task=%s",
        request_id, task.task_id,
    )


def test_concurrent_cancel_and_complete_single_outcome(make_stack, request_id):
    # 取消与完成几乎同时发生：无论谁先赢得乐观锁，最终结论必须唯一且稳定。
    for round_no in range(10):
        gate = threading.Event()

        def handler(payload):
            gate.wait(2)
            return {"round": round_no}

        store, service, executor, _ = make_stack(
            Settings(handler_timeout=5), handler=handler
        )
        task, _ = service.submit(f"k-{round_no}", {}, request_id=request_id)
        executor.dispatch(task)
        wait_until(lambda: store.get(task.task_id).state is TaskState.RUNNING)

        results: dict[str, str] = {}

        def do_cancel():
            try:
                t = service.cancel(task.task_id, request_id=request_id)
                results["cancel_view"] = t.state.value
            except TransitionError:
                # 取消请求落在终态之后：以库内最终状态为准
                results["cancel_view"] = store.get(task.task_id).state.value

        cancel_thread = threading.Thread(target=do_cancel)
        cancel_thread.start()
        gate.set()  # 同时放行处理器完成
        cancel_thread.join()
        executor.shutdown(wait=True)

        final = store.get(task.task_id)
        assert final.state in (TaskState.CANCELLED, TaskState.SUCCEEDED)
        # 结论稳定：再次读取不会变化，取消方观察到的结论与最终一致
        assert store.get(task.task_id).state is final.state
        assert results["cancel_view"] == final.state.value
        logger.info(
            "request=%s round=%s decision verified: 竞态最终结论唯一 state=%s",
            request_id, round_no, final.state.value,
        )
        store.close()
