"""幂等提交与有界并发测试：重复提交不产生重复任务，超容量立即拒绝。"""
from __future__ import annotations

import logging
import threading
from concurrent.futures import ThreadPoolExecutor

import pytest

from taskpipe.config import Settings
from taskpipe.models import RejectReason, TaskState
from taskpipe.service import ServiceRejected

logger = logging.getLogger("taskpipe.test")


def test_duplicate_submit_returns_same_task(make_stack, request_id):
    store, service, _, _ = make_stack()
    first, dup1 = service.submit("key-1", {"v": 1}, request_id=request_id)
    assert dup1 is False and first.state is TaskState.PENDING
    second, dup2 = service.submit("key-1", {"v": 2}, request_id=request_id)
    assert dup2 is True
    assert second.task_id == first.task_id
    assert store.list_all() and len(store.list_all()) == 1  # 无重复任务
    logger.info(
        "request=%s decision verified: 相同幂等键仅产生一个任务 task=%s",
        request_id, first.task_id,
    )


def test_over_capacity_is_rejected_immediately(make_stack, request_id):
    store, service, _, _ = make_stack(Settings(max_pending=2))
    service.submit("k1", {}, request_id=request_id)
    service.submit("k2", {}, request_id=request_id)
    with pytest.raises(ServiceRejected) as exc:
        service.submit("k3", {}, request_id=request_id)
    assert exc.value.reason is RejectReason.OVER_CAPACITY
    logger.info(
        "request=%s decision verified: 容量满时第 3 个请求被拒绝 reason=%s",
        request_id, RejectReason.OVER_CAPACITY.value,
    )


def test_completed_task_frees_queue_capacity(make_stack, request_id):
    store, service, executor, settings = make_stack(
        Settings(max_pending=1, handler_timeout=5)
    )
    t, _ = service.submit("k1", {}, request_id=request_id)
    executor.dispatch(t)
    from tests.conftest import wait_until
    wait_until(lambda: store.get(t.task_id).state is TaskState.SUCCEEDED)
    # 已完成任务不再占据队列容量，新请求可提交
    t2, duplicated = service.submit("k2", {}, request_id=request_id)
    assert duplicated is False
    executor.dispatch(t2)
    wait_until(lambda: store.get(t2.task_id).state is TaskState.SUCCEEDED)


def test_concurrent_submit_same_idempotency_key_creates_one_task(
    make_stack, request_id
):
    store, service, _, _ = make_stack(Settings(max_pending=200))
    outcomes: dict[str, int] = {"accepted": 0, "duplicated": 0}
    lock = threading.Lock()

    def worker(i: int) -> None:
        _, duplicated = service.submit(
            "hot-key", {"i": i}, request_id=f"{request_id}-w{i}"
        )
        with lock:
            outcomes["duplicated" if duplicated else "accepted"] += 1

    with ThreadPoolExecutor(max_workers=16) as pool:
        list(pool.map(worker, range(64)))

    assert outcomes == {"accepted": 1, "duplicated": 63}
    tasks = store.list_all()
    assert len(tasks) == 1
    logger.info(
        "request=%s decision verified: 64 个并发同键提交 accepted=1 duplicated=63",
        request_id,
    )


def test_concurrent_submit_at_capacity_bound(make_stack, request_id):
    # 容量上限 max_pending=10：32 个不同键并发提交，接受数必须恰好等于容量，
    # 其余以 OVER_CAPACITY 被明确拒绝，而不是排队。
    store, service, _, _ = make_stack(Settings(max_pending=10))
    accepted, rejected = [], []
    lock = threading.Lock()

    def worker(i: int) -> None:
        try:
            task, _ = service.submit(f"key-{i}", {}, request_id=f"{request_id}-w{i}")
            with lock:
                accepted.append(task.task_id)
        except ServiceRejected as exc:
            assert exc.reason is RejectReason.OVER_CAPACITY
            with lock:
                rejected.append(i)

    with ThreadPoolExecutor(max_workers=16) as pool:
        list(pool.map(worker, range(32)))

    assert len(accepted) == 10
    assert len(rejected) == 22
    assert len(store.list_all()) == 10
    logger.info(
        "request=%s decision verified: 32 个并发异键提交 accepted=%s rejected=%s "
        "reason=%s（无无限排队）",
        request_id, len(accepted), len(rejected),
        RejectReason.OVER_CAPACITY.value,
    )

