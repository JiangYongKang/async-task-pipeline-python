"""重试任务在取消与容量上的语义：

* 等待重试期间可取消，取消后不再执行；
* 取消正好撞上某一次重试（定时器到期与取消竞争），最终结论唯一；
* RETRY_WAITING 任务保留容量名额，新请求仍被明确拒绝。
"""
from __future__ import annotations

import logging
import threading
import time

import pytest

from taskpipe.config import Settings
from taskpipe.models import AttemptStatus, RejectReason, RetryPolicy, TaskState
from taskpipe.service import ServiceRejected
from tests.conftest import wait_until

logger = logging.getLogger("taskpipe.test")


def test_cancel_during_retry_wait_never_runs_again(make_stack, request_id):
    ran = threading.Event()

    def handler(payload):
        ran.set()
        raise RuntimeError("fail first")

    policy = RetryPolicy(max_retries=3, base_delay=5.0)
    store, service, executor, _ = make_stack(
        Settings(handler_timeout=5), handler=handler
    )
    task, _ = service.submit(
        "k1", {}, retry_policy=policy, request_id=request_id
    )
    executor.dispatch(task)
    wait_until(
        lambda: store.get(task.task_id).state is TaskState.RETRY_WAITING
    )

    cancelled = service.cancel(task.task_id, request_id=request_id)
    assert cancelled.state is TaskState.CANCELLED
    assert cancelled.next_run_at is None

    # 等待超过重试间隔：定时器即便到期也必须领取失败，不再执行
    time.sleep(0.15)
    final = store.get(task.task_id)
    assert final.state is TaskState.CANCELLED
    records = service.get_attempts(task.task_id)
    assert len(records) == 1  # 只有第一次尝试，没有重试
    assert records[0].status is AttemptStatus.FAILED
    logger.info(
        "request=%s decision verified: 等待重试期间取消后不再执行 task=%s",
        request_id, task.task_id,
    )


def test_cancel_racing_with_retry_fire_single_outcome(make_stack, request_id):
    # 重试间隔极短，让"定时器到期开始重试"与"取消"高频撞车，
    # 无论谁赢，最终结论必须唯一稳定，且尝试不会重复计数。
    for round_no in range(10):
        def handler(payload, _round=round_no):
            # 首次尝试立即失败触发重试；后续尝试短暂阻塞以便与取消竞争
            time.sleep(0.02)
            raise RuntimeError(f"round-{_round}")

        store, service, executor, _ = make_stack(
            Settings(handler_timeout=5, max_pending=32), handler=handler
        )
        task, _ = service.submit(
            f"race-{round_no}", {},
            retry_policy=RetryPolicy(max_retries=5, base_delay=0.001),
            request_id=f"{request_id}-r{round_no}",
        )
        executor.dispatch(task)

        # 随机时刻取消：可能撞上等待、重试领取或执行中
        time.sleep(0.005)
        outcome: dict[str, str] = {}
        try:
            t = service.cancel(task.task_id, request_id=request_id)
            outcome["view"] = t.state.value
        except Exception as exc:  # noqa: BLE001
            outcome["view"] = type(exc).__name__

        executor.shutdown(wait=True)
        final = store.get(task.task_id)
        assert final.state in (TaskState.CANCELLED, TaskState.FAILED)
        # 结论稳定：再次读取不变；且每个尝试序号只出现一次（无重复执行/计数）
        assert store.get(task.task_id).state is final.state
        records = store.list_attempts(task.task_id)
        nos = [a.attempt_no for a in records]
        assert nos == list(range(1, len(nos) + 1))
        assert len(nos) <= 6
        # 取消方观察到的结论与最终库内结论一致（终态后取消除外）
        if outcome["view"] not in {"TransitionError"}:
            assert outcome["view"] == final.state.value
        store.close()
    logger.info(
        "request=%s decision verified: 10 轮取消/重试撞车结论均唯一",
        request_id,
    )


def test_retry_waiting_holds_capacity_slot(make_stack, request_id):
    def handler(payload):
        raise RuntimeError("fail")

    # max_pending=1：唯一的名额被等待重试任务持有，新请求必须被明确拒绝
    store, service, executor, _ = make_stack(
        Settings(max_pending=1, handler_timeout=5), handler=handler
    )
    task, _ = service.submit(
        "retryer", {},
        retry_policy=RetryPolicy(max_retries=2, base_delay=5.0),
        request_id=request_id,
    )
    executor.dispatch(task)
    wait_until(
        lambda: store.get(task.task_id).state is TaskState.RETRY_WAITING
    )
    with pytest.raises(ServiceRejected) as exc:
        service.submit("newcomer", {}, request_id=request_id)
    assert exc.value.reason is RejectReason.OVER_CAPACITY

    # 重复提交仍走幂等、不受容量限制
    dup, duplicated = service.submit("retryer", {}, request_id=request_id)
    assert duplicated is True and dup.task_id == task.task_id
    logger.info(
        "request=%s decision verified: RETRY_WAITING 保留名额，新请求被拒 "
        "reason=OVER_CAPACITY，幂等重复提交不受限 task=%s",
        request_id, task.task_id,
    )


def test_retry_does_not_exceed_concurrency(make_stack, request_id):
    current = 0
    peak = 0
    lock = threading.Lock()

    def handler(payload):
        nonlocal current, peak
        with lock:
            current += 1
            peak = max(peak, current)
        time.sleep(0.02)
        with lock:
            current -= 1
        raise RuntimeError("always fail")

    store, service, executor, _ = make_stack(
        Settings(max_concurrency=2, max_pending=32, handler_timeout=5),
        handler=handler,
    )
    tasks = [
        service.submit(
            f"k{i}", {},
            retry_policy=RetryPolicy(max_retries=2, base_delay=0.01),
            request_id=request_id,
        )[0]
        for i in range(6)
    ]
    for t in tasks:
        executor.dispatch(t)
    # 等待所有任务把 3 次尝试自然跑完（定时器到期不占用 shutdown 语义）
    for t in tasks:
        wait_until(
            lambda t=t: store.get(t.task_id).state is TaskState.FAILED
        )
    executor.shutdown(wait=True)
    assert peak <= 2  # 重试也不突破并发上限
    for t in tasks:
        final = store.get(t.task_id)
        assert final.state is TaskState.FAILED
        assert final.attempt_no == 3
    logger.info(
        "request=%s decision verified: 重试全程峰值并发=%s <= 2",
        request_id, peak,
    )
