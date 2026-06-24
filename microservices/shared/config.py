"""Centralized configuration loaded from environment variables."""

import os
from dataclasses import dataclass, field


@dataclass(frozen=True)
class SnowflakeConfig:
    account: str = field(default_factory=lambda: os.environ["SNOWFLAKE_ACCOUNT"])
    user: str = field(default_factory=lambda: os.environ["SNOWFLAKE_USER"])
    password: str = field(default_factory=lambda: os.environ["SNOWFLAKE_PASSWORD"])
    database: str = field(
        default_factory=lambda: os.environ.get("SNOWFLAKE_DATABASE", "SG_SEARCH_DOCS")
    )
    schema: str = field(
        default_factory=lambda: os.environ.get("SNOWFLAKE_SCHEMA", "DATA")
    )
    warehouse: str = field(
        default_factory=lambda: os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")
    )
    role: str = field(
        default_factory=lambda: os.environ.get("SNOWFLAKE_ROLE", "")
    )


@dataclass(frozen=True)
class ServiceURLs:
    retrieval: str = field(
        default_factory=lambda: os.environ.get(
            "SERVICE_RETRIEVAL_URL", "http://retrieval-service:8001"
        )
    )
    completion: str = field(
        default_factory=lambda: os.environ.get(
            "SERVICE_COMPLETION_URL", "http://completion-service:8002"
        )
    )
    orchestrator: str = field(
        default_factory=lambda: os.environ.get(
            "SERVICE_ORCHESTRATOR_URL", "http://chat-orchestrator:8003"
        )
    )
    document: str = field(
        default_factory=lambda: os.environ.get(
            "SERVICE_DOCUMENT_URL", "http://document-service:8004"
        )
    )
    gateway: str = field(
        default_factory=lambda: os.environ.get(
            "SERVICE_GATEWAY_URL", "http://api-gateway:8000"
        )
    )


@dataclass(frozen=True)
class AppConfig:
    cortex_search_service: str = field(
        default_factory=lambda: os.environ.get(
            "CORTEX_SEARCH_SERVICE", "SG_SEARCH_SERVICE_CS"
        )
    )
    default_num_chunks: int = field(
        default_factory=lambda: int(os.environ.get("DEFAULT_NUM_CHUNKS", "3"))
    )
    log_level: str = field(
        default_factory=lambda: os.environ.get("LOG_LEVEL", "INFO")
    )
    rate_limit_per_minute: int = field(
        default_factory=lambda: int(os.environ.get("RATE_LIMIT_PER_MINUTE", "60"))
    )
