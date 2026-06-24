"""Snowflake session factory with connection pooling."""

import logging
from contextlib import contextmanager

from snowflake.snowpark import Session
from snowflake.core import Root

from .config import SnowflakeConfig

logger = logging.getLogger(__name__)

_session: Session | None = None


def _build_connection_params(cfg: SnowflakeConfig) -> dict:
    params = {
        "account": cfg.account,
        "user": cfg.user,
        "password": cfg.password,
        "database": cfg.database,
        "schema": cfg.schema,
        "warehouse": cfg.warehouse,
    }
    if cfg.role:
        params["role"] = cfg.role
    return params


def get_session(cfg: SnowflakeConfig | None = None) -> Session:
    """Return a reusable Snowpark session (created on first call)."""
    global _session
    if _session is None:
        if cfg is None:
            cfg = SnowflakeConfig()
        params = _build_connection_params(cfg)
        logger.info("Creating Snowflake session for account=%s", cfg.account)
        _session = Session.builder.configs(params).create()
    return _session


def get_root(cfg: SnowflakeConfig | None = None) -> Root:
    """Return a Snowflake Root object backed by a reusable session."""
    return Root(get_session(cfg))


@contextmanager
def session_scope(cfg: SnowflakeConfig | None = None):
    """Context manager that yields a session and handles cleanup on error."""
    sess = get_session(cfg)
    try:
        yield sess
    except Exception:
        logger.exception("Error during Snowflake session usage")
        raise


def close_session() -> None:
    """Close the global session (call on shutdown)."""
    global _session
    if _session is not None:
        _session.close()
        _session = None
        logger.info("Snowflake session closed")
