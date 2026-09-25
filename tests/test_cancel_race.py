"""取消与完成几乎同时发生时，最终结论必须唯一且稳定。

覆盖：
* QUEUED 任务被取消后，worker 出队不得把它“复活”为 RUNNING/成功；
* RUNNING 任务上取消与完成的 CAS 竞争，多轮并发下只有一方赢，终态稳定；
* 终态后重复取消得到可区分原因 ALREADY_TERMINAL。
"""

from __future__ import annotations

import threading
import time
from concurrent.futures import ThreadPoolExecutor

import pytest

from app.enums import RejectReason, TaskState
from app.errors import RejectedError
from app.handlers import register_handler
from app.repository import TaskRepository
from app.settings import Settings
from tests.conftest import wait_terminal


def test_queued_cancel_is_not_resurrected(service_factory, tmp_path) -> None:
    """占满 worker 后提交的任务处于 QUEUED；此时取消，
    worker 稍后取到它时必须跳过执行，任务永远停在 CANCELLED。"""
    release = threading.Event()

    def blocker(payload: str, cancel: threading.Event) -> str:
        release.wait(5)
        return "ok"

    def quick(payload: str, cancel: threading.Event) -> str:
        time.sleep(0.05)
        return "should-not-happen"

    register_handler("blocker", blocker)
    register_handler("quick", quick)
    svc = service_factory(max_concurrency=1, max_capacity=4)

    svc.submit("blocker", "", request_id="q-hold")
    time.sleep(0.1)  # 让 blocker 进入 RUNNING
    queued, _ = svc.submit("quick", "", request_id="q-wait")
    assert svc.get(queued.task_id).state is TaskState.QUEUED

    cancelled = svc.cancel(queued.task_id, request_id="q-cancel")
    assert cancelled.state is TaskState.CANCELLED

    release.set()
    time.sleep(0.4)  # 等 worker 把队列里的取消任务取走

    final = svc.get(queued.task_id)
    assert final.state is TaskState.CANCELLED, f"终态被改写: {final.state}"
    assert final.result is None
    print(f"请求标识=q-cancel 状态迁移=QUEUED->CANCELLED 判定依据=worker出队见终态跳过")


@pytest.mark.parametrize("round_no", range(20))
def test_cas_race_cancel_vs_complete_has_unique_winner(
    tmp_path, round_no: int
) -> None:
    """直接压测裁决原语：同一 RUNNING 任务上 4 个取消 vs 4 个完成并发 CAS，
    20 轮中每轮都必须恰好一个赢家，且终态之后不再被改写。"""
    repo = TaskRepository(Settings(db_path=tmp_path / f"race-{round_no}.db"))
    repo.initialize()
    repo.create_task("race-task", None, "echo", "", state=TaskState.RUNNING)

    outcomes = []

    def try_cancel() -> None:
        try:
            repo.cas_state(
                "race-task", frozenset({TaskState.RUNNING}), TaskState.CANCELLED
            )
            outcomes.append("CANCELLED")
        except RejectedError as exc:
            outcomes.append(f"lose:{exc.reason.value}")

    def try_complete() -> None:
        try:
            repo.cas_state(
                "race-task", frozenset({TaskState.RUNNING}),
                TaskState.SUCCEEDED, result="done",
            )
            outcomes.append("SUCCEEDED")
        except RejectedError as exc:
            outcomes.append(f"lose:{exc.reason.value}")

    with ThreadPoolExecutor(max_workers=8) as pool:
        futs = [pool.submit(try_cancel) for _ in range(4)]
        futs += [pool.submit(try_complete) for _ in range(4)]
        for f in futs:
            f.result()

    winners = [o for o in outcomes if not o.startswith("lose:")]
    assert len(winners) == 1, f"第{round_no}轮赢家不唯一: {outcomes}"
    final = repo.get("race-task")
    assert final is not None and final.state.value == winners[0]
    # 输家的拒绝原因必须可区分（终态已被赢家锁定）
    assert all(
        o in (f"lose:{RejectReason.ALREADY_TERMINAL.value}",) for o in outcomes
        if o.startswith("lose:")
    )
    repo.close()


def test_service_level_cancel_and_complete_consistency(service_factory) -> None:
    """服务层：极短任务与大量并发取消同时进行，终态必须一致且可重复查询。"""

    def tiny(payload: str, cancel: threading.Event) -> str:
        # 不主动检查 cancel：由服务层 CAS 在完成/取消间裁决
        for _ in range(500):
            pass
        return "done"

    register_handler("tiny", tiny)
    svc = service_factory(max_concurrency=4, max_capacity=32)

    task, _ = svc.submit("tiny", "", request_id="svc-race")
    decision: list[str] = []

    def cancel_once() -> None:
        try:
            t = svc.cancel(task.task_id, request_id="svc-race-c")
            decision.append(f"cancel:{t.state.value}")
        except RejectedError as exc:
            decision.append(f"reject:{exc.reason.value}")

    with ThreadPoolExecutor(max_workers=6) as pool:
        futs = [pool.submit(cancel_once) for _ in range(6)]
        for f in futs:
            f.result()

    final = wait_terminal(svc, task.task_id)
    assert final.state in (TaskState.CANCELLED, TaskState.SUCCEEDED)
    # 再查一次，结论稳定
    assert svc.get(task.task_id).state is final.state
    accepted = [d for d in decision if d.startswith("cancel:")]
    assert len(accepted) <= 1, f"取消被接受多次: {decision}"
    print(
        f"请求标识=svc-race(-c) 终态={final.state.value} "
        f"判定依据=expected=RUNNING 的 CAS 仅一条提交成功 outcomes={decision}"
    )


def test_repeated_cancel_distinguishable_reason(service_factory) -> None:
    release = threading.Event()

    def wait_all(payload: str, cancel: threading.Event) -> str:
        cancel.wait(2)
        return "cancelled"

    register_handler("wait_all", wait_all)
    svc = service_factory(max_concurrency=1, max_capacity=2)
    task, _ = svc.submit("wait_all", "", request_id="rc-1")
    time.sleep(0.1)
    svc.cancel(task.task_id, request_id="rc-2")
    final = wait_terminal(svc, task.task_id)
    assert final.state is TaskState.CANCELLED

    with pytest.raises(RejectedError) as ei:
        svc.cancel(task.task_id, request_id="rc-3")
    assert ei.value.reason is RejectReason.ALREADY_TERMINAL
