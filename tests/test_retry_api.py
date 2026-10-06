"""HTTP 端到端：带重试策略提交、任务视图展示尝试进度、逐次尝试查询、
等待中取消。"""
from __future__ import annotations

import logging

from fastapi.testclient import TestClient

from taskpipe.api import create_app
from taskpipe.config import Settings
from tests.conftest import wait_until

logger = logging.getLogger("taskpipe.test")


def test_submit_with_retry_retries_then_succeeds(tmp_db, request_id):
    calls = {"n": 0}

    def flaky(payload):
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("transient")
        return {"ok": True}

    settings = Settings(db_path=tmp_db, handler_timeout=5)
    app = create_app(settings, handler=flaky)
    with TestClient(app) as client:
        r = client.post(
            "/tasks",
            json={
                "idempotency_key": "k1",
                "request_id": request_id,
                "retry": {"max_retries": 2, "base_delay": 0.02},
            },
        )
        assert r.status_code == 200
        body = r.json()
        assert body["state"] == "PENDING"
        assert body["attempt_no"] == 0
        assert body["remaining_retries"] == 2
        assert body["next_run_at"] is None
        task_id = body["task_id"]

        final = wait_until(
            lambda: (
                b if (b := client.get(f"/tasks/{task_id}").json())["state"]
                == "SUCCEEDED" else None
            )
        )
        assert final["result"] == {"ok": True}
        assert final["attempt_no"] == 2
        assert final["remaining_retries"] == 1

        r = client.get(f"/tasks/{task_id}/attempts")
        attempts = r.json()["attempts"]
        assert [a["attempt_no"] for a in attempts] == [1, 2]
        assert attempts[0]["is_retry"] is False
        assert attempts[0]["status"] == "FAILED"
        assert attempts[0]["failure_reason"] == "HANDLER_ERROR"
        assert attempts[0]["next_run_at"] is not None
        assert attempts[1]["is_retry"] is True
        assert attempts[1]["status"] == "SUCCEEDED"
        logger.info(
            "request=%s decision verified: HTTP 重试成功并可逐次追溯 task=%s",
            request_id, task_id,
        )


def test_retry_waiting_cancelled_via_http(tmp_db, request_id):
    def always_fail(payload):
        raise RuntimeError("boom")

    settings = Settings(db_path=tmp_db, handler_timeout=5)
    app = create_app(settings, handler=always_fail)
    with TestClient(app) as client:
        r = client.post(
            "/tasks",
            json={
                "idempotency_key": "k1",
                "retry": {"max_retries": 3, "base_delay": 5},
            },
        )
        task_id = r.json()["task_id"]
        wait_until(
            lambda: client.get(f"/tasks/{task_id}").json()["state"]
            == "RETRY_WAITING"
        )
        view = client.get(f"/tasks/{task_id}").json()
        assert view["remaining_retries"] == 2
        assert view["next_run_at"] is not None

        r = client.post(f"/tasks/{task_id}/cancel")
        assert r.status_code == 200 and r.json()["state"] == "CANCELLED"
        assert r.json()["next_run_at"] is None
        logger.info(
            "request=%s decision verified: HTTP 等待重试中取消成功 task=%s",
            request_id, task_id,
        )


def test_attempts_of_unknown_task_404(tmp_db, request_id):
    app = create_app(Settings(db_path=tmp_db, handler_timeout=5))
    with TestClient(app) as client:
        r = client.get("/tasks/nope/attempts")
        assert r.status_code == 404
        assert r.json()["error"]["reason"] == "TASK_NOT_FOUND"
