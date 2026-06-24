"""Document Service — manages document metadata and presigned URLs."""

import logging
import sys
from contextlib import asynccontextmanager

from fastapi import FastAPI

sys.path.insert(0, "..")
from shared.config import SnowflakeConfig
from shared.models import (
    CategoriesResponse,
    PresignedURLRequest,
    PresignedURLResponse,
    HealthResponse,
)
from shared.snowflake_client import get_session, close_session
from shared.middleware import add_common_middleware

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Document Service starting — initializing Snowflake connection")
    sf_cfg = SnowflakeConfig()
    app.state.session = get_session(sf_cfg)
    yield
    close_session()
    logger.info("Document Service shut down")


app = FastAPI(title="Document Service", lifespan=lifespan)
add_common_middleware(app)


@app.get("/health", response_model=HealthResponse)
async def health():
    return HealthResponse(service="document-service")


@app.get("/documents/categories", response_model=CategoriesResponse)
async def get_categories():
    session = app.state.session
    rows = session.sql(
        "SELECT category FROM SG_SEARCH_DOCS.DATA.docs_chunks_table "
        "GROUP BY category"
    ).collect()
    categories = [row.CATEGORY for row in rows]
    return CategoriesResponse(categories=categories)


@app.post("/documents/presigned-urls", response_model=PresignedURLResponse)
async def get_presigned_urls(req: PresignedURLRequest):
    session = app.state.session
    urls: dict[str, str] = {}

    for path in req.paths:
        sql = (
            f"SELECT GET_PRESIGNED_URL(@docs, '{path}', 360) AS URL_LINK "
            f"FROM directory(@docs)"
        )
        df = session.sql(sql).to_pandas()
        urls[path] = df.iloc[0]["URL_LINK"]

    return PresignedURLResponse(urls=urls)
