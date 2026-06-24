"""Chat Orchestrator — coordinates retrieval and completion for RAG pipeline."""

import json
import logging
import os
import sys
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, HTTPException

sys.path.insert(0, "..")
from shared.config import ServiceURLs
from shared.models import (
    ChatRequest,
    ChatResponse,
    SearchRequest,
    SearchResponse,
    CompletionRequest,
    CompletionResponse,
    HealthResponse,
)
from shared.middleware import add_common_middleware

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

PROMPT_TEMPLATE = """You are an expert chat assistance that extracts information from the CONTEXT provided
between <context> and </context> tags.
When answering the question contained between <question> and </question> tags
be thorough and do not hallucinate.
If you don't have the information just say so.
Only answer the question if you can extract it from the CONTEXT provided.

Do not mention the CONTEXT used in your answer.

<context>
{context}
</context>
<question>
{question}
</question>
Answer:"""

NO_RAG_TEMPLATE = """Question:
{question}
Answer:"""


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Chat Orchestrator starting")
    urls = ServiceURLs()
    app.state.urls = urls
    app.state.http_client = httpx.AsyncClient(timeout=120.0)
    yield
    await app.state.http_client.aclose()
    logger.info("Chat Orchestrator shut down")


app = FastAPI(title="Chat Orchestrator", lifespan=lifespan)
add_common_middleware(app)


@app.get("/health", response_model=HealthResponse)
async def health():
    return HealthResponse(service="chat-orchestrator")


@app.post("/chat", response_model=ChatResponse)
async def chat(req: ChatRequest):
    client: httpx.AsyncClient = app.state.http_client
    urls: ServiceURLs = app.state.urls
    sources: list[str] = []

    if req.use_rag:
        # Step 1: Retrieve relevant chunks
        search_payload = SearchRequest(
            query=req.question,
            category=req.category,
            num_chunks=req.num_chunks,
        )
        search_resp = await client.post(
            f"{urls.retrieval}/search",
            json=search_payload.model_dump(),
        )
        if search_resp.status_code != 200:
            logger.error("Retrieval failed: %s", search_resp.text)
            raise HTTPException(502, "Retrieval service error")

        search_data = SearchResponse.model_validate(search_resp.json())
        context = json.dumps(
            [r.model_dump() for r in search_data.results], indent=2
        )
        sources = list({r.relative_path for r in search_data.results})

        prompt = PROMPT_TEMPLATE.format(context=context, question=req.question)
    else:
        prompt = NO_RAG_TEMPLATE.format(question=req.question)

    # Step 2: Call completion
    completion_payload = CompletionRequest(model=req.model, prompt=prompt)
    comp_resp = await client.post(
        f"{urls.completion}/complete",
        json=completion_payload.model_dump(),
    )
    if comp_resp.status_code != 200:
        logger.error("Completion failed: %s", comp_resp.text)
        raise HTTPException(502, "Completion service error")

    comp_data = CompletionResponse.model_validate(comp_resp.json())

    return ChatResponse(
        answer=comp_data.response,
        sources=sources,
        model=comp_data.model,
    )
