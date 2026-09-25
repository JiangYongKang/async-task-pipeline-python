"""本地启动入口：``uv run python main.py``。"""
from __future__ import annotations

import os

import uvicorn

from taskpipe.api import create_app

app = create_app()

if __name__ == "__main__":
    uvicorn.run(
        app,
        host=os.environ.get("TASKPIPE_HOST", "127.0.0.1"),
        port=int(os.environ.get("TASKPIPE_PORT", "8000")),
    )
