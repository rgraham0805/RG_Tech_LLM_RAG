"""Pydantic models shared across microservices."""

from pydantic import BaseModel, Field


# ── Retrieval Service ──


class SearchRequest(BaseModel):
    query: str
    category: str = "ALL"
    num_chunks: int = Field(default=3, ge=1, le=20)


class ChunkResult(BaseModel):
    chunk: str
    relative_path: str
    category: str


class SearchResponse(BaseModel):
    results: list[ChunkResult]


# ── Completion Service ──


class CompletionRequest(BaseModel):
    model: str = "mistral-large2"
    prompt: str


class CompletionUsage(BaseModel):
    prompt_tokens: int | None = None
    completion_tokens: int | None = None


class CompletionResponse(BaseModel):
    response: str
    model: str
    usage: CompletionUsage = CompletionUsage()


class ModelsResponse(BaseModel):
    models: list[str]


# ── Chat Orchestrator ──


class ChatRequest(BaseModel):
    question: str
    use_rag: bool = True
    model: str = "mistral-large2"
    category: str = "ALL"
    num_chunks: int = Field(default=3, ge=1, le=20)


class ChatResponse(BaseModel):
    answer: str
    sources: list[str] = []
    model: str


# ── Document Service ──


class CategoriesResponse(BaseModel):
    categories: list[str]


class PresignedURLRequest(BaseModel):
    paths: list[str]


class PresignedURLResponse(BaseModel):
    urls: dict[str, str]


# ── Health ──


class HealthResponse(BaseModel):
    status: str = "ok"
    service: str
