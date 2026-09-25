"""幂等提交与有界并发。

* 相同幂等键的并发提交只产生一个任务，其他请求拿到同一任务（replayed）；
* 在制任务数到达容量上限时明确拒绝（CAPACITY_FULL），不排队；
* 任务终态后容量释放，可继续提交。
"""

from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor

import pytest

from app.enums import RejectReason, TaskState
from app.errors import RejectedError
from app.handlers import register_handler
from tests.conftest import wait_terminal


@pytest.fixture
def blocking_handler():
    """可控制放行的 handler；每次进入会在 entered 上计数。"""

    release = threading.Event()
    entered = threading.Event()
    entered_count = {"n": 0}
    lock = threading.Lock()

    def fn(payload: str, cancel: threading.Event) -> str:
        with lock:
            entered_count["n"] += 1
        entered.set()
        release.wait(5)
        return "released"

    register_handler("blocking", fn)
    yield release, entered_count


def test_concurrent_same_idempotency_key_single_task(service_factory) -> None:
    svc = service_factory(max_concurrency=4, max_capacity=32)
    results: list[tuple[object, bool]] = []
    errors: list[RejectedError] = []
    barrier = threading.Barrier(8)

    def worker(i: int) -> None:
        barrier.wait()
        try:
            task, replayed = svc.submit(
                "echo", f"payload-{i}", idempotency_key="same-key",
                request_id=f"idem-{i}",
            )
            results.append((task.task_id, replayed))
        except RejectedError as exc:
            errors.append(exc)

    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(worker, range(8)))

    assert not errors
    task_ids = {r[0] for r in results}
    assert len(task_ids) == 1, f"重复任务: {task_ids}"
    replayed_count = sum(1 for _, replay in results if replay)
    assert replayed_count == 7, f"应有 7 次幂等重放，实际 {replayed_count}"
    task_id = next(iter(task_ids))
    final = wait_terminal(svc, task_id)
    assert final.state is TaskState.SUCCEEDED
    # 首个提交的 payload 被保留
    assert final.payload.startswith("payload-")
    print(f"请求标识=idem-0..7 唯一任务={task_id} 判定依据=idempotency_key 唯一约束+锁内查重")


def test_capacity_full_is_rejected_not_queued(service_factory, blocking_handler) -> None:
    release, entered_count = blocking_handler
    svc = service_factory(max_concurrency=2, max_capacity=4)

    accepted = []
    for i in range(4):
        task, _ = svc.submit("blocking", "", request_id=f"cap-{i}")
        accepted.append(task.task_id)

    # 等前两个进入执行，确保容量槽位真实被占
    release_wait_cycles = 0
    while entered_count["n"] < 2 and release_wait_cycles < 200:
        release_wait_cycles += 1
        __import__("time").sleep(0.01)
    assert entered_count["n"] == 2

    with pytest.raises(RejectedError) as ei:
        svc.submit("echo", "", request_id="cap-overflow")
    assert ei.value.reason is RejectReason.CAPACITY_FULL
    print(f"请求标识=cap-overflow 判定依据=active(4)>=capacity(4) 拒绝原因=CAPACITY_FULL")

    # 放行使其终态，容量释放后可再提交
    release.set()
    for tid in accepted:
        wait_terminal(svc, tid)

    task, replayed = svc.submit("echo", "after-drain", request_id="cap-drained")
    assert replayed is False
    final = wait_terminal(svc, task.task_id)
    assert final.result == "after-drain"


def test_different_idempotency_keys_create_separate_tasks(service_factory) -> None:
    svc = service_factory()
    t1, r1 = svc.submit("echo", "a", idempotency_key="key-a", request_id="sep-a")
    t2, r2 = svc.submit("echo", "b", idempotency_key="key-b", request_id="sep-b")
    assert t1.task_id != t2.task_id
    assert r1 is False and r2 is False


def test_invalid_handler_rejected_with_distinct_reason(service_factory) -> None:
    svc = service_factory()
    with pytest.raises(RejectedError) as ei:
        svc.submit("not-registered", "", request_id="bad-handler")
    assert ei.value.reason is RejectReason.INVALID_HANDLER


def test_idempotent_replay_returns_terminal_state(service_factory) -> None:
    """任务终态后用相同幂等键再提交，仍返回同一终态任务（不会重跑）。"""
    svc = service_factory()
    t1, _ = svc.submit("echo", "once", idempotency_key="once-key", request_id="once-1")
    wait_terminal(svc, t1.task_id)
    t2, replayed = svc.submit(
        "echo", "again", idempotency_key="once-key", request_id="once-2"
    )
    assert replayed is True
    assert t2.task_id == t1.task_id
    assert t2.state is TaskState.SUCCEEDED
    assert t2.result == "once"
