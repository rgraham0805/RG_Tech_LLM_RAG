"""Common FastAPI middleware for logging, request IDs, and error handling."""

import logging
import time
import uuid

from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse

logger = logging.getLogger(__name__)


def add_common_middleware(app: FastAPI) -> None:
    """Attach standard middleware to a FastAPI application."""

    @app.middleware("http")
    async def request_logging(request: Request, call_next) -> Response:
        request_id = request.headers.get("X-Request-ID", str(uuid.uuid4()))
        start = time.perf_counter()

        # Attach request_id to state so handlers can use it
        request.state.request_id = request_id

        response: Response = await call_next(request)

        elapsed_ms = (time.perf_counter() - start) * 1000
        logger.info(
            "method=%s path=%s status=%s duration_ms=%.1f request_id=%s",
            request.method,
            request.url.path,
            response.status_code,
            elapsed_ms,
            request_id,
        )
        response.headers["X-Request-ID"] = request_id
        return response

    @app.exception_handler(Exception)
    async def unhandled_exception_handler(
        request: Request, exc: Exception
    ) -> JSONResponse:
        request_id = getattr(request.state, "request_id", "unknown")
        logger.exception(
            "Unhandled exception request_id=%s: %s", request_id, exc
        )
        return JSONResponse(
            status_code=500,
            content={"detail": "Internal server error", "request_id": request_id},
        )
