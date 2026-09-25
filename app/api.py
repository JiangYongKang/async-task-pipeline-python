"""FastAPI HTTP 层。

端点
----
POST   /tasks                    提交任务
GET    /tasks/{task_id}          查询状态
POST   /tasks/{task_id}/cancel   取消任务
GET    /healthz                  存活探针

约定
----
* 所有请求可用 ``X-Request-ID`` 头关联日志；未提供则服务端生成。
* 业务拒绝返回 4xx，错误体形如 ``{"error": {"reason": ..., "detail": ...}}``，
  reason 取自 :class:`app.enums.RejectReason`，客户端可据此区分处理。
* 幂等命中返回 200 并带 ``X-Idempotent-Replay: true``；新建返回 201。
"""

from __future__ import annotations

import uuid

from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from .enums import RejectReason
from .errors import RejectedError
from .logging_setup import get_logger, request_context
from .service import TaskService

_REASON_STATUS = {
    RejectReason.CAPACITY_FULL: 429,
    RejectReason.INVALID_HANDLER: 422,
    RejectReason.INVALID_PAYLOAD: 422,
    RejectReason.ALREADY_TERMINAL: 409,
    RejectReason.ILLEGAL_TRANSITION: 409,
    RejectReason.TASK_NOT_FOUND: 404,
}


class SubmitRequest(BaseModel):
    handler: str = Field(min_length=1)
    payload: str = ""
    idempotency_key: str | None = Field(default=None, max_length=128)


def create_app(service: TaskService) -> FastAPI:
    app = FastAPI(title="Async Task Pipeline", version="0.1.0")
    log = get_logger("pipeline.api")

    @app.middleware("http")
    async def request_id_middleware(request: Request, call_next):
        request_id = request.headers.get("X-Request-ID") or uuid.uuid4().hex[:12]
        with request_context(request_id):
            log.info(
                "http-in req=%s method=%s path=%s",
                request_id, request.method, request.url.path,
            )
            response = await call_next(request)
            response.headers["X-Request-ID"] = request_id
            return response

    @app.exception_handler(RejectedError)
    async def rejected_handler(request: Request, exc: RejectedError) -> JSONResponse:
        status = _REASON_STATUS.get(exc.reason, 400)
        log.warning(
            "http-out rejected status=%d reason=%s detail=%s",
            status, exc.reason.value, exc.detail,
        )
        return JSONResponse(
            status_code=status,
            content={
                "error": {"reason": exc.reason.value, "detail": exc.detail}
            },
        )

    @app.post("/tasks", status_code=201)
    async def submit_task(body: SubmitRequest, response: Response) -> dict[str, object]:
        task, replayed = service.submit(
            handler=body.handler,
            payload=body.payload,
            idempotency_key=body.idempotency_key,
        )
        if replayed:
            response.status_code = 200
            response.headers["X-Idempotent-Replay"] = "true"
        view = task.to_view()
        view["replayed"] = replayed
        return view

    @app.get("/tasks/{task_id}")
    async def get_task(task_id: str) -> dict[str, object]:
        return service.get(task_id).to_view()

    @app.post("/tasks/{task_id}/cancel")
    async def cancel_task(task_id: str) -> dict[str, object]:
        return service.cancel(task_id).to_view()

    @app.get("/healthz")
    async def healthz() -> dict[str, str]:
        return {"status": "ok"}

    return app
