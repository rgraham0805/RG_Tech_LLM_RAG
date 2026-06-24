"""Completion Service — wraps Snowflake Cortex Complete."""

import logging
import sys
from contextlib import asynccontextmanager

from fastapi import FastAPI

sys.path.insert(0, "..")
from shared.config import SnowflakeConfig
from shared.models import (
    CompletionRequest,
    CompletionResponse,
    CompletionUsage,
    ModelsResponse,
    HealthResponse,
)
from shared.snowflake_client import get_session, close_session
from shared.middleware import add_common_middleware

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

AVAILABLE_MODELS = [
    "mistral-large2",
    "llama3.1-70b",
    "llama3.1-8b",
    "snowflake-arctic",
]


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Completion Service starting — initializing Snowflake connection")
    sf_cfg = SnowflakeConfig()
    app.state.session = get_session(sf_cfg)
    yield
    close_session()
    logger.info("Completion Service shut down")


app = FastAPI(title="Completion Service", lifespan=lifespan)
add_common_middleware(app)


@app.get("/health", response_model=HealthResponse)
async def health():
    return HealthResponse(service="completion-service")


@app.get("/models", response_model=ModelsResponse)
async def list_models():
    return ModelsResponse(models=AVAILABLE_MODELS)


@app.post("/complete", response_model=CompletionResponse)
async def complete(req: CompletionRequest):
    session = app.state.session
    sql = "SELECT snowflake.cortex.complete(?, ?) AS response"
    result = session.sql(sql, params=[req.model, req.prompt]).collect()
    response_text = result[0].RESPONSE

    return CompletionResponse(
        response=response_text,
        model=req.model,
        usage=CompletionUsage(),
    )
