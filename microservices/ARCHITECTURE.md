# LLM RAG Chat Assistant — Microservices Architecture

## Overview

This document describes the microservices decomposition of the monolithic
Streamlit-based LLM RAG Chat Assistant. The goal is to break the single
`streamlit_app.py` into independently deployable services while preserving
existing latency characteristics and the Snowflake Cortex integration.

---

## Current Monolith Analysis

The existing `streamlit_app.py` combines five responsibilities in one process:

| Responsibility        | Code Region                          |
|-----------------------|--------------------------------------|
| UI rendering          | `main()`, Streamlit widgets          |
| Snowflake connection  | `st.connection("snowflake")`, Root() |
| Document retrieval    | `get_similar_chunks_search_service()`|
| Prompt construction   | `create_prompt()`                    |
| LLM completion        | `complete()` → Cortex Complete SQL   |
| Document URL serving  | Presigned URL generation in `main()` |

---

## Service Decomposition

```
┌─────────────┐
│   Frontend   │  (Streamlit or React)
│  :8501       │
└──────┬───────┘
       │ HTTP
       ▼
┌─────────────────┐
│   API Gateway    │  FastAPI — auth, rate-limit, routing
│   :8000          │
└──┬───┬───┬───┬──┘
   │   │   │   │
   ▼   │   │   ▼
┌──────┐ │ │ ┌───────────┐
│ Chat │ │ │ │ Document  │
│Orch. │ │ │ │ Service   │
│:8003 │ │ │ │ :8004     │
└─┬──┬─┘ │ │ └───────────┘
  │  │   │ │
  │  │   ▼ ▼
  │  │ ┌──────────┐
  │  │ │Retrieval │
  │  │ │ Service  │
  │  │ │ :8001    │
  │  │ └──────────┘
  │  │
  │  ▼
  │ ┌──────────┐
  │ │Completion│
  │ │ Service  │
  │ │ :8002    │
  │ └──────────┘
  │
  ▼
Snowflake Cortex (external)
```

### 1. API Gateway (`api-gateway/`, port 8000)

**Responsibility:** Single entry-point for all client requests. Handles
authentication, rate limiting, request validation, and routing to downstream
services.

| Endpoint              | Method | Downstream          |
|-----------------------|--------|---------------------|
| `/api/v1/chat`        | POST   | Chat Orchestrator   |
| `/api/v1/search`      | POST   | Retrieval Service   |
| `/api/v1/models`      | GET    | Completion Service  |
| `/api/v1/documents/*` | GET    | Document Service    |
| `/health`             | GET    | local               |

**Tech:** FastAPI, `httpx` (async HTTP client), Python `slowapi` for rate
limiting.

---

### 2. Retrieval Service (`retrieval-service/`, port 8001)

**Responsibility:** Encapsulates all Snowflake Cortex Search interactions.
Accepts a natural-language query + optional category filter and returns ranked
document chunks.

```
POST /search
{
  "query": "What are multi-modal LLMs?",
  "category": "ALL",        // optional filter
  "num_chunks": 3           // configurable
}

Response 200:
{
  "results": [
    {
      "chunk": "...",
      "relative_path": "doc.pdf",
      "category": "LLM"
    }
  ]
}
```

**Tech:** FastAPI, `snowflake-snowpark-python`, `snowflake.core`.

**Latency note:** Reuses a connection pool to Snowflake to avoid per-request
connection overhead.

---

### 3. Completion Service (`completion-service/`, port 8002)

**Responsibility:** Wraps Snowflake Cortex Complete. Accepts a fully formed
prompt + model name and returns the LLM response. This service owns model
availability and can be extended to support streaming or multiple LLM backends.

```
POST /complete
{
  "model": "mistral-large2",
  "prompt": "You are an expert..."
}

Response 200:
{
  "response": "Multi-modal LLMs are...",
  "model": "mistral-large2",
  "usage": { "prompt_tokens": 512, "completion_tokens": 128 }
}
```

```
GET /models

Response 200:
{
  "models": [
    "mistral-large2",
    "llama3.1-70b",
    "llama3.1-8b",
    "snowflake-arctic"
  ]
}
```

**Tech:** FastAPI, `snowflake-snowpark-python`.

---

### 4. Chat Orchestrator (`chat-orchestrator/`, port 8003)

**Responsibility:** The "brain" of the RAG pipeline. Receives a user question,
decides whether to use RAG, calls Retrieval Service for context, builds the
prompt, calls Completion Service, and assembles the final response.

```
POST /chat
{
  "question": "What are multi-modal LLMs?",
  "use_rag": true,
  "model": "mistral-large2",
  "category": "ALL",
  "num_chunks": 3
}

Response 200:
{
  "answer": "Multi-modal LLMs are...",
  "sources": ["doc1.pdf", "doc2.pdf"],
  "model": "mistral-large2"
}
```

**Latency strategy:** The orchestrator calls Retrieval and then Completion
sequentially (Completion depends on retrieval context). Total latency ≈
retrieval_latency + completion_latency, same as the monolith. No added
network hop between orchestrator and Snowflake — those go through the
dedicated services that maintain persistent connections.

**Tech:** FastAPI, `httpx`.

---

### 5. Document Service (`document-service/`, port 8004)

**Responsibility:** Manages document metadata and generates presigned URLs
for source documents referenced in RAG responses.

```
GET /documents/categories

Response 200:
{
  "categories": ["LLM", "RAG", "Transformers"]
}
```

```
POST /documents/presigned-urls
{
  "paths": ["doc1.pdf", "doc2.pdf"]
}

Response 200:
{
  "urls": {
    "doc1.pdf": "https://...",
    "doc2.pdf": "https://..."
  }
}
```

**Tech:** FastAPI, `snowflake-snowpark-python`.

---

### 6. Frontend (`frontend/`, port 8501)

**Responsibility:** Pure presentation layer. Renders the chat UI and sidebar
configuration. All business logic is delegated to the API Gateway.

**Tech:** Streamlit (preserving familiarity with the existing stack). Can be
migrated to React/Next.js later for richer interactivity.

---

## Shared Library (`shared/`)

Common code used across services:

- **`config.py`** — Environment-based configuration (Snowflake credentials,
  service URLs, feature flags)
- **`snowflake_client.py`** — Snowflake session/connection pool factory
- **`models.py`** — Pydantic request/response models shared across services
- **`middleware.py`** — Common middleware (logging, tracing, error handling)

---

## Inter-Service Communication

| Path                    | Protocol | Pattern     |
|-------------------------|----------|-------------|
| Frontend → Gateway      | HTTP/REST| Sync        |
| Gateway → Orchestrator  | HTTP/REST| Sync        |
| Orchestrator → Retrieval| HTTP/REST| Sync        |
| Orchestrator → Completion| HTTP/REST| Sync       |
| Gateway → Document Svc  | HTTP/REST| Sync        |
| All services → Logging  | Stdout   | Async (ELK) |

All inter-service calls are synchronous REST to match the existing latency
profile. The monolith makes sequential Snowflake calls; the microservices
preserve this exact call chain with minimal added network overhead (services
run in the same Docker network).

---

## Latency Analysis

```
Monolith:
  User → Streamlit → Snowflake Search → Snowflake Complete → Streamlit → User
  Total: ~search_time + ~complete_time + ~render_time

Microservices:
  User → Frontend → Gateway → Orchestrator → Retrieval Svc → Snowflake Search
                                            → Completion Svc → Snowflake Complete
       ← Frontend ← Gateway ← Orchestrator ←
  Added overhead: ~2-4ms per HTTP hop (same Docker network)
  Total: ~search_time + ~complete_time + ~render_time + ~8-16ms
```

The 8-16ms overhead from internal HTTP hops is negligible relative to the
Snowflake Cortex round-trip times (typically 200-2000ms). Latency is preserved.

---

## Deployment

All services are containerized with Docker and orchestrated via Docker Compose
for local development. For production, deploy to Kubernetes (EKS/GKE/AKS) or
a managed container service (ECS, Cloud Run).

```
docker-compose up --build
```

### Scaling Strategy

| Service            | Scaling Approach                                |
|--------------------|-------------------------------------------------|
| API Gateway        | Horizontal — stateless, scale by request volume |
| Retrieval Service  | Horizontal — scale with search traffic          |
| Completion Service | Horizontal — scale with LLM request volume      |
| Chat Orchestrator  | Horizontal — stateless coordinator              |
| Document Service   | Horizontal — low traffic, minimal scaling needed|
| Frontend           | Horizontal — serve static assets via CDN later  |

---

## Configuration

All services read configuration from environment variables, centralized in
`.env` files per service and a shared `docker-compose.yml`.

Key environment variables:
- `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD`
- `SNOWFLAKE_DATABASE`, `SNOWFLAKE_SCHEMA`, `SNOWFLAKE_WAREHOUSE`
- `CORTEX_SEARCH_SERVICE` — name of the Cortex Search service
- `SERVICE_*_URL` — URLs for inter-service communication
- `LOG_LEVEL`, `RATE_LIMIT_PER_MINUTE`

---

## Future Extensions

1. **Streaming responses** — Add SSE/WebSocket support in Completion Service
   for token-by-token streaming to the frontend.
2. **Conversation memory** — Add a Conversation Service backed by Redis to
   store chat history and enable multi-turn context.
3. **Async ingestion pipeline** — Add a Document Ingestion Service that
   watches a storage bucket, chunks documents, and updates the Cortex Search
   index via a message queue (RabbitMQ/SQS).
4. **Observability** — Add OpenTelemetry tracing across all services for
   end-to-end latency visibility.
5. **Auth service** — Extract authentication into a dedicated service with
   JWT/OAuth2 support.
