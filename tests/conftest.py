"""pytest 公共夹具。

* 每个测试使用独立临时 SQLite 文件；
* 开启日志（含 request_id、状态迁移、判定依据），单测运行时可直接看到；
* 自动注册内置 handler。
"""

from __future__ import annotations

import logging
from collections.abc import Iterator
from pathlib import Path

import pytest

from app.handlers import register_builtin_handlers
from app.logging_setup import configure_logging
from app.service import TaskService
from app.settings import Settings


@pytest.fixture(autouse=True)
def _show_logs() -> Iterator[None]:
    configure_logging(logging.INFO)
    yield


@pytest.fixture
def service_factory(tmp_path: Path):
    created: list[TaskService] = []

    def _factory(
        max_concurrency: int = 2,
        max_capacity: int = 4,
        default_timeout_seconds: float | None = None,
    ) -> TaskService:
        register_builtin_handlers()
        db = tmp_path / f"tasks-{len(created)}.db"
        svc = TaskService(
            Settings(
                max_concurrency=max_concurrency,
                max_capacity=max_capacity,
                db_path=db,
                default_timeout_seconds=default_timeout_seconds,
            )
        )
        svc.initialize()
        created.append(svc)
        return svc

    yield _factory

    for svc in created:
        svc.shutdown()


def wait_terminal(svc: TaskService, task_id: str, timeout: float = 3.0):
    import time

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        task = svc.get(task_id)
        if task.state.is_terminal:
            return task
        time.sleep(0.01)
    raise AssertionError(f"task {task_id} did not reach terminal state")
