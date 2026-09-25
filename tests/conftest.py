"""pytest 公共配置：保证单测日志打印请求标识、状态迁移与判定依据。"""
from __future__ import annotations

import logging
import os
import tempfile
import time
import uuid

import pytest

from taskpipe.api import configure_logging
from taskpipe.config import Settings
from taskpipe.executor import Executor
from taskpipe.service import TaskService
from taskpipe.store import TaskStore

configure_logging()
logger = logging.getLogger("taskpipe.test")


@pytest.fixture
def request_id() -> str:
    rid = f"req-test-{uuid.uuid4().hex[:8]}"
    logger.info("request=%s 测试用例开始", rid)
    return rid


@pytest.fixture
def tmp_db() -> str:
    return os.path.join(tempfile.mkdtemp(), f"tasks-{uuid.uuid4().hex[:8]}.db")


@pytest.fixture
def make_stack(tmp_db):
    """构造 (store, service, executor, settings) 栈，测试结束后清理。"""
    created = []

    def _make(settings: Settings | None = None, handler=None):
        if settings is None:
            settings = Settings(db_path=tmp_db)
        else:
            settings.db_path = tmp_db  # 始终使用临时数据库
        store = TaskStore(settings.db_path)
        service = TaskService(store, settings)
        executor = Executor(store, settings, handler or (lambda p: {"echo": p}))
        created.append((store, executor))
        return store, service, executor, settings

    yield _make

    for store, executor in created:
        executor.shutdown(wait=True)
        store.close()


def wait_until(condition, timeout: float = 5.0, interval: float = 0.01):
    """轮询等待条件，用于等待异步执行结果。"""
    deadline = time.time() + timeout
    while time.time() < deadline:
        result = condition()
        if result:
            return result
        time.sleep(interval)
    raise AssertionError("condition not met before timeout")
