"""HTTP 端到端测试：提交/查询/取消/幂等/容量拒绝/重启恢复。"""
from __future__ import annotations

import logging
import threading
import time

from fastapi.testclient import TestClient

from taskpipe.api import create_app
from taskpipe.config import Settings
from tests.conftest import wait_until

logger = logging.getLogger("taskpipe.test")


def _wait_state(client: TestClient, task_id: str, state: str) -> dict:
    def probe():
        body = client.get(f"/tasks/{task_id}").json()
        return body if body.get("state") == state else None

    return wait_until(probe)


def test_submit_query_cancel_flow(tmp_db, request_id):
    app = create_app(Settings(db_path=tmp_db, handler_timeout=5))
    with TestClient(app) as client:
        r = client.post(
            "/tasks",
            json={"idempotency_key": "k1", "payload": {"n": 1},
                  "request_id": request_id},
        )
        assert r.status_code == 200
        body = r.json()
        assert body["state"] == "PENDING" and body["duplicated"] is False
        assert "X-Request-ID" in r.headers
        task_id = body["task_id"]

        final = _wait_state(client, task_id, "SUCCEEDED")
        assert final["result"] == {"echo": {"n": 1}}

        # 终态后取消被拒绝，原因可区分
        r = client.post(f"/tasks/{task_id}/cancel")
        assert r.status_code == 409
        assert r.json()["error"]["reason"] == "TERMINAL_STATE"
        logger.info(
            "request=%s decision verified: 终态取消被拒绝 reason=TERMINAL_STATE",
            request_id,
        )


def test_cancel_pending_via_http(tmp_db, request_id):
    release = threading.Event()

    def blocker(payload):
        release.wait(5)
        return None

    settings = Settings(db_path=tmp_db, max_concurrency=1, handler_timeout=5)
    app = create_app(settings, handler=blocker)
    with TestClient(app) as client:
        t1 = client.post("/tasks", json={"idempotency_key": "busy"}).json()
        t2 = client.post("/tasks", json={"idempotency_key": "queued"}).json()
        wait_until(
            lambda: client.get(f"/tasks/{t1['task_id']}").json()["state"]
            == "RUNNING"
        )
        r = client.post(f"/tasks/{t2['task_id']}/cancel")
        assert r.status_code == 200 and r.json()["state"] == "CANCELLED"
        release.set()
        # 被取消的排队任务不会被执行
        time.sleep(0.2)
        assert client.get(f"/tasks/{t2['task_id']}").json()["state"] == "CANCELLED"
        logger.info(
            "request=%s decision verified: 排队任务取消后不再执行 task=%s",
            request_id, t2["task_id"],
        )


def test_duplicate_submit_via_http(tmp_db, request_id):
    app = create_app(Settings(db_path=tmp_db, handler_timeout=5))
    with TestClient(app) as client:
        r1 = client.post("/tasks", json={"idempotency_key": "dup"})
        r2 = client.post("/tasks", json={"idempotency_key": "dup"})
        assert r1.json()["duplicated"] is False
        assert r2.json()["duplicated"] is True
        assert r1.json()["task_id"] == r2.json()["task_id"]
        assert len(client.get("/tasks").json()["tasks"]) == 1


def test_over_capacity_rejected_via_http(tmp_db, request_id):
    release = threading.Event()

    def blocker(payload):
        release.wait(5)
        return None

    settings = Settings(
        db_path=tmp_db, max_concurrency=1, max_pending=1, handler_timeout=5
    )
    app = create_app(settings, handler=blocker)
    with TestClient(app) as client:
        assert client.post("/tasks", json={"idempotency_key": "a"}).status_code == 200
        assert client.post("/tasks", json={"idempotency_key": "b"}).status_code == 200
        r = client.post("/tasks", json={"idempotency_key": "c"})
        assert r.status_code == 503
        assert r.json()["error"]["reason"] == "OVER_CAPACITY"
        logger.info(
            "request=%s decision verified: 超容量请求被明确拒绝 "
            "status=503 reason=OVER_CAPACITY",
            request_id,
        )
        release.set()


def test_restart_recovers_interrupted_task(tmp_db, request_id):
    """进程停止时 RUNNING 的任务：重启后状态确定（FAILED/INTERRUPTED）且可查询。

    通过直接向库中写入 RUNNING 任务并关闭存储来模拟“进程中断”现场，
    随后以新应用实例（同一数据库）验证启动恢复。
    """
    from taskpipe.models import TaskState
    from taskpipe.service import TaskService
    from taskpipe.store import TaskStore

    settings = Settings(db_path=tmp_db, handler_timeout=5)
    store = TaskStore(tmp_db)
    service = TaskService(store, settings)
    task, _ = service.submit("k1", {}, request_id=request_id)
    store.transition(task.task_id, task.version, TaskState.RUNNING)
    store.close()  # 模拟进程停止：RUNNING 任务遗留在库中
    logger.info(
        "request=%s 模拟进程中断：task=%s 停止时状态=RUNNING",
        request_id, task.task_id,
    )

    # 重启：同一数据库，新应用实例
    app2 = create_app(settings, handler=lambda p: {"echo": p})
    with TestClient(app2) as client2:
        r = client2.get(f"/tasks/{task.task_id}")
        assert r.status_code == 200
        assert r.json()["state"] == "FAILED"
        assert r.json()["failure_reason"] == "INTERRUPTED"
        logger.info(
            "request=%s decision verified: 重启后 task=%s state=FAILED "
            "reason=INTERRUPTED（状态确定且可查询）",
            request_id, task.task_id,
        )
