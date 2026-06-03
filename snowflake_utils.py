# Shared Snowflake helpers.
#
# Consolidates the duplicated connection / session / service-lookup code
# that previously appeared twice at module level in streamlit_app.py,
# and wraps the repeated ``session.sql(...)`` calls into small,
# reusable functions.

import streamlit as st
from snowflake.core import Root

from config import (
    CORTEX_SEARCH_DATABASE,
    CORTEX_SEARCH_SCHEMA,
    CORTEX_SEARCH_SERVICE,
    DOCS_CHUNKS_TABLE,
    NUM_CHUNKS,
    SEARCH_COLUMNS,
)


@st.cache_resource
def get_session():
    """Return a cached Snowflake session (created once per app lifecycle)."""
    cnx = st.connection("snowflake")
    return cnx.session()


@st.cache_resource
def get_search_service():
    """Return the Cortex Search service handle (created once per app lifecycle)."""
    session = get_session()
    root = Root(session)
    return (
        root.databases[CORTEX_SEARCH_DATABASE]
        .schemas[CORTEX_SEARCH_SCHEMA]
        .cortex_search_services[CORTEX_SEARCH_SERVICE]
    )


def get_categories():
    """Fetch distinct product categories from the docs chunks table."""
    session = get_session()
    return session.sql(
        f"SELECT category FROM {DOCS_CHUNKS_TABLE} GROUP BY category"
    ).collect()


def search_documents(query, category="ALL"):
    """Run a Cortex Search query, optionally filtered by *category*.

    Returns the JSON response from the search service.
    """
    svc = get_search_service()
    if category == "ALL":
        response = svc.search(query, SEARCH_COLUMNS, limit=NUM_CHUNKS)
    else:
        filter_obj = {"@eq": {"category": category}}
        response = svc.search(
            query, SEARCH_COLUMNS, filter=filter_obj, limit=NUM_CHUNKS
        )
    return response.json()


def run_completion(model_name, prompt):
    """Call ``snowflake.cortex.complete`` and return the result rows."""
    session = get_session()
    return session.sql(
        "SELECT snowflake.cortex.complete(?, ?) AS response",
        params=[model_name, prompt],
    ).collect()


def get_presigned_url(path):
    """Return a pre-signed URL for a document stored in the @docs stage."""
    session = get_session()
    sql = f"SELECT GET_PRESIGNED_URL(@docs, '{path}', 360) AS URL_LINK FROM directory(@docs)"
    df = session.sql(sql).to_pandas()
    return df._get_value(0, "URL_LINK")
