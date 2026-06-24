"""Retrieval Service — wraps Snowflake Cortex Search."""

import logging
import sys
from contextlib import asynccontextmanager

from fastapi import FastAPI

sys.path.insert(0, "..")
from shared.config import SnowflakeConfig, AppConfig
from shared.models import SearchRequest, SearchResponse, ChunkResult, HealthResponse
from shared.snowflake_client import get_root, close_session
from shared.middleware import add_common_middleware

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

COLUMNS = ["chunk", "relative_path", "category"]


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Retrieval Service starting — initializing Snowflake connection")
    sf_cfg = SnowflakeConfig()
    app_cfg = AppConfig()

    root = get_root(sf_cfg)
    svc = (
        root.databases[sf_cfg.database]
        .schemas[sf_cfg.schema]
        .cortex_search_services[app_cfg.cortex_search_service]
    )
    app.state.search_service = svc
    app.state.app_config = app_cfg

    yield

    close_session()
    logger.info("Retrieval Service shut down")


app = FastAPI(title="Retrieval Service", lifespan=lifespan)
add_common_middleware(app)


@app.get("/health", response_model=HealthResponse)
async def health():
    return HealthResponse(service="retrieval-service")


@app.post("/search", response_model=SearchResponse)
async def search(req: SearchRequest):
    svc = app.state.search_service
    num_chunks = req.num_chunks or app.state.app_config.default_num_chunks

    if req.category == "ALL":
        response = svc.search(req.query, COLUMNS, limit=num_chunks)
    else:
        filter_obj = {"@eq": {"category": req.category}}
        response = svc.search(req.query, COLUMNS, filter=filter_obj, limit=num_chunks)

    data = response.json()
    results = [
        ChunkResult(
            chunk=item["chunk"],
            relative_path=item["relative_path"],
            category=item["category"],
        )
        for item in data.get("results", [])
    ]
    return SearchResponse(results=results)
