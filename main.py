"""本地运行入口：

    uv run python main.py

可用环境变量调整容量/并发/数据库路径，见 app.settings 文档。
SIGTERM/SIGINT 会触发优雅停机（取消在制任务并等待 worker 排空）。
"""

from __future__ import annotations

import logging
import signal

import uvicorn

from app.api import create_app
from app.handlers import register_builtin_handlers
from app.logging_setup import configure_logging
from app.service import TaskService
from app.settings import load_settings


def build_service() -> TaskService:
    configure_logging(logging.INFO)
    settings = load_settings()
    register_builtin_handlers()
    service = TaskService(settings)
    service.initialize()
    return service


def main() -> None:
    service = build_service()
    app = create_app(service)
    config = uvicorn.Config(app, host="127.0.0.1", port=8000, log_config=None)
    server = uvicorn.Server(config)

    def _graceful_exit(signum: int, frame: object) -> None:
        logging.getLogger("pipeline.main").info(
            "received signal %s: initiating graceful shutdown", signum
        )
        server.should_exit = True

    signal.signal(signal.SIGTERM, _graceful_exit)
    signal.signal(signal.SIGINT, _graceful_exit)

    try:
        server.run()
    finally:
        service.shutdown()


if __name__ == "__main__":
    main()
