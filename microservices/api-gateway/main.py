"""API Gateway — entry point for all client requests."""

import logging
import sys
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware

sys.path.insert(0, "..")
from shared.config import ServiceURLs, AppConfig
from shared.models import (
    ChatRequest,
    ChatResponse,
    SearchRequest,
    SearchResponse,
    CompletionRequest,
    CompletionResponse,
    ModelsResponse,
    CategoriesResponse,
    PresignedURLRequest,
    PresignedURLResponse,
    HealthResponse,
)
from shared.middleware import add_common_middleware

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("API Gateway starting")
    app.state.urls = ServiceURLs()
    app.state.config = AppConfig()
    app.state.http_client = httpx.AsyncClient(timeout=120.0)
    yield
    await app.state.http_client.aclose()
    logger.info("API Gateway shut down")


app = FastAPI(title="LLM RAG API Gateway", lifespan=lifespan)
add_common_middleware(app)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


async def _proxy(client: httpx.AsyncClient, method: str, url: str, **kwargs):
    """Forward a request to a downstream service."""
    try:
        resp = await client.request(method, url, **kwargs)
        resp.raise_for_status()
        return resp.json()
    except httpx.HTTPStatusError as exc:
        logger.error("Downstream error: %s %s → %s", method, url, exc.response.status_code)
        raise HTTPException(exc.response.status_code, exc.response.text)
    except httpx.RequestError as exc:
        logger.error("Downstream unreachable: %s %s → %s", method, url, exc)
        raise HTTPException(502, f"Service unavailable: {url}")


# ── Health ──

@app.get("/health", response_model=HealthResponse)
async def health():
    return HealthResponse(service="api-gateway")


# ── Chat ──

@app.post("/api/v1/chat", response_model=ChatResponse)
async def chat(req: ChatRequest):
    data = await _proxy(
        app.state.http_client,
        "POST",
        f"{app.state.urls.orchestrator}/chat",
        json=req.model_dump(),
    )
    return ChatResponse.model_validate(data)


# ── Search ──

@app.post("/api/v1/search", response_model=SearchResponse)
async def search(req: SearchRequest):
    data = await _proxy(
        app.state.http_client,
        "POST",
        f"{app.state.urls.retrieval}/search",
        json=req.model_dump(),
    )
    return SearchResponse.model_validate(data)


# ── Models ──

@app.get("/api/v1/models", response_model=ModelsResponse)
async def list_models():
    data = await _proxy(
        app.state.http_client,
        "GET",
        f"{app.state.urls.completion}/models",
    )
    return ModelsResponse.model_validate(data)


# ── Documents ──

@app.get("/api/v1/documents/categories", response_model=CategoriesResponse)
async def get_categories():
    data = await _proxy(
        app.state.http_client,
        "GET",
        f"{app.state.urls.document}/documents/categories",
    )
    return CategoriesResponse.model_validate(data)


@app.post("/api/v1/documents/presigned-urls", response_model=PresignedURLResponse)
async def get_presigned_urls(req: PresignedURLRequest):
    data = await _proxy(
        app.state.http_client,
        "POST",
        f"{app.state.urls.document}/documents/presigned-urls",
        json=req.model_dump(),
    )
    return PresignedURLResponse.model_validate(data)
