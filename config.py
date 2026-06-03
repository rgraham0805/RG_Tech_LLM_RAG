# Centralized configuration constants for the RAG chat assistant.
# Previously these values were scattered (and sometimes duplicated)
# across streamlit_app.py.

# Number of chunks provided as context for the search service
NUM_CHUNKS = 3

# Snowflake Cortex Search service coordinates
CORTEX_SEARCH_DATABASE = "SG_SEARCH_DOCS"
CORTEX_SEARCH_SCHEMA = "DATA"
CORTEX_SEARCH_SERVICE = "SG_SEARCH_SERVICE_CS"

# Fully-qualified table used for category lookups
DOCS_CHUNKS_TABLE = f"{CORTEX_SEARCH_DATABASE}.{CORTEX_SEARCH_SCHEMA}.docs_chunks_table"

# Columns returned by the Cortex Search service
SEARCH_COLUMNS = [
    "chunk",
    "relative_path",
    "category",
]

# LLM models available in the sidebar selector
AVAILABLE_MODELS = (
    "mistral-large2",
    "llama3.1-70b",
    "llama3.1-8b",
    "snowflake-arctic",
)
