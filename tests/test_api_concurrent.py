"""HTTP 层并发场景：并发提交、幂等重放、容量拒绝的状态码与原因可区分。"""

from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor

from fastapi.testclient import TestClient

from app.api import create_app
from app.handlers import register_handler


def _client(service_factory):
    svc = service_factory(max_concurrency=2, max_capacity=4)
    return TestClient(create_app(svc)), svc


def test_http_concurrent_idempotent_submit(service_factory) -> None:
    client, svc = _client(service_factory)
    barrier = threading.Barrier(6)

    def post(i: int):
        barrier.wait()
        return client.post(
            "/tasks",
            json={"handler": "echo", "payload": f"p{i}", "idempotency_key": "http-key"},
            headers={"X-Request-ID": f"http-idem-{i}"},
        )

    with ThreadPoolExecutor(max_workers=6) as pool:
        responses = list(pool.map(post, range(6)))

    statuses = sorted(r.status_code for r in responses)
    assert statuses.count(201) == 1, statuses
    assert statuses.count(200) == 5, statuses
    task_ids = {r.json()["task_id"] for r in responses}
    assert len(task_ids) == 1
    replays = [r for r in responses if r.headers.get("X-Idempotent-Replay") == "true"]
    assert len(replays) == 5


def test_http_capacity_full_429_with_reason(service_factory) -> None:
    release = threading.Event()

    def block(payload: str, cancel: threading.Event) -> str:
        release.wait(5)
        return "ok"

    register_handler("http_block", block)
    client, svc = _client(service_factory)

    for i in range(4):
        r = client.post("/tasks", json={"handler": "http_block"},
                        headers={"X-Request-ID": f"http-cap-{i}"})
        assert r.status_code == 201

    r = client.post("/tasks", json={"handler": "echo"},
                    headers={"X-Request-ID": "http-cap-overflow"})
    assert r.status_code == 429
    body = r.json()
    assert body["error"]["reason"] == "CAPACITY_FULL"
    print(f"请求标识=http-cap-overflow HTTP=429 判定依据={body['error']['detail']}")

    # 非法参数也有独立原因码，不与容量混淆
    r = client.post("/tasks", json={"handler": "ghost"})
    assert r.status_code == 422
    assert r.json()["error"]["reason"] == "INVALID_HANDLER"

    release.set()


def test_http_cancel_flow_and_terminal_conflict(service_factory) -> None:
    client, svc = _client(service_factory)
    r = client.post("/tasks", json={"handler": "echo", "payload": "x"})
    tid = r.json()["task_id"]

    import time
    for _ in range(100):
        if client.get(f"/tasks/{tid}").json()["state"] == "SUCCEEDED":
            break
        time.sleep(0.02)

    r = client.post(f"/tasks/{tid}/cancel")
    assert r.status_code == 409
    assert r.json()["error"]["reason"] == "ALREADY_TERMINAL"
