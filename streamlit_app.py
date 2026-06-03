# Import python packages
import streamlit as st
from snowflake.core import Root
import pandas as pd
import json
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

## Default Values
NUM_CHUNKS = 3  # Num-chunks provided as context. Play with this to check how it affects your accuracy

# service parameters
CORTEX_SEARCH_DATABASE = "SG_SEARCH_DOCS"
CORTEX_SEARCH_SCHEMA = "DATA"
CORTEX_SEARCH_SERVICE = "SG_SEARCH_SERVICE_CS"

# columns to query in the service
COLUMNS = [
    "chunk",
    "relative_path",
    "category"
]


def init_snowflake_connection():
    """Initialize Snowflake connection and Cortex search service.

    Returns a tuple of (session, svc) or raises an error displayed in the Streamlit UI.
    """
    try:
        cnx = st.connection("snowflake")
        session = cnx.session()
    except Exception as e:
        logger.error("Failed to establish Snowflake connection: %s", e)
        st.error(f"Failed to connect to Snowflake: {e}")
        raise

    try:
        root = Root(session)
        database = root.databases[CORTEX_SEARCH_DATABASE]
        schema = database.schemas[CORTEX_SEARCH_SCHEMA]
        svc = schema.cortex_search_services[CORTEX_SEARCH_SERVICE]
    except KeyError as e:
        logger.error("Cortex search service not found: %s", e)
        st.error(
            f"Cortex search service configuration error — could not find {e}. "
            "Verify that the database, schema, and service names are correct."
        )
        raise
    except Exception as e:
        logger.error("Failed to initialize Cortex search service: %s", e)
        st.error(f"Failed to initialize search service: {e}")
        raise

    return session, svc


session, svc = init_snowflake_connection()
pd.set_option("max_colwidth", None)


### Functions

def config_options():
    st.sidebar.selectbox(
        'Select your model:',
        ('mistral-large2', 'llama3.1-70b', 'llama3.1-8b', 'snowflake-arctic'),
        key="model_name",
    )

    try:
        categories = session.sql(
            "select category from SG_SEARCH_DOCS.DATA.docs_chunks_table group by category"
        ).collect()
    except Exception as e:
        logger.error("Failed to fetch categories: %s", e)
        st.sidebar.error(f"Could not load categories: {e}")
        categories = []

    cat_list = ['ALL']
    for cat in categories:
        cat_list.append(cat.CATEGORY)

    st.sidebar.selectbox(
        'Select what products you are looking for', cat_list, key="category_value"
    )

    st.sidebar.expander("Session State").write(st.session_state)


def get_similar_chunks_search_service(query):
    """Search for similar chunks using the Cortex search service.

    Raises:
        RuntimeError: If the search service call fails.
    """
    try:
        if st.session_state.category_value == "ALL":
            response = svc.search(query, COLUMNS, limit=NUM_CHUNKS)
        else:
            filter_obj = {"@eq": {"category": st.session_state.category_value}}
            response = svc.search(query, COLUMNS, filter=filter_obj, limit=NUM_CHUNKS)
    except Exception as e:
        logger.error("Cortex search service request failed: %s", e)
        raise RuntimeError(
            f"Search service request failed: {e}"
        ) from e

    st.sidebar.json(response.json())
    return response.json()


def create_prompt(myquestion):
    """Build the LLM prompt, optionally enriched with RAG context.

    Raises:
        RuntimeError: If RAG context retrieval or parsing fails.
    """
    if st.session_state.rag == 1:
        prompt_context = get_similar_chunks_search_service(myquestion)

        prompt = f"""
           You are an expert chat assistance that extracs information from the CONTEXT provided
           between <context> and </context> tags.
           When ansering the question contained between <question> and </question> tags
           be thorough and do not hallucinate. 
           If you don´t have the information just say so.
           Only anwer the question if you can extract it from the CONTEXT provideed.
           
           Do not mention the CONTEXT used in your answer.
    
           <context>          
           {prompt_context}
           </context>
           <question>  
           {myquestion}
           </question>
           Answer: 
           """

        try:
            json_data = json.loads(prompt_context)
        except (json.JSONDecodeError, TypeError) as e:
            logger.error("Failed to parse search response as JSON: %s", e)
            raise RuntimeError(
                f"Failed to parse search service response: {e}"
            ) from e

        if "results" not in json_data:
            logger.error("Search response missing 'results' key: %s", json_data.keys())
            raise RuntimeError(
                "Unexpected search response format: missing 'results' key"
            )

        try:
            relative_paths = set(item['relative_path'] for item in json_data['results'])
        except KeyError as e:
            logger.error("Search result item missing expected key: %s", e)
            raise RuntimeError(
                f"Malformed search result — missing key {e}"
            ) from e

    else:
        prompt = f"""[0]
         'Question:  
           {myquestion} 
           Answer: '
           """
        relative_paths = "None"

    return prompt, relative_paths


def complete(myquestion):
    """Send the question to the selected Snowflake Cortex model.

    Raises:
        RuntimeError: If the LLM completion query fails.
    """
    prompt, relative_paths = create_prompt(myquestion)
    cmd = """
            select snowflake.cortex.complete(?, ?) as response
          """

    try:
        df_response = session.sql(cmd, params=[st.session_state.model_name, prompt]).collect()
    except Exception as e:
        logger.error("LLM completion query failed: %s", e)
        raise RuntimeError(
            f"Failed to get model response: {e}"
        ) from e

    if not df_response:
        logger.warning("LLM completion returned empty result set")
        raise RuntimeError("Model returned an empty response")

    return df_response, relative_paths


def main():
    st.title(":speech_balloon: Steve's Chat Assistant")

    config_options()

    st.session_state.rag = st.sidebar.checkbox('Use your own documents as context?')

    question = st.text_input(
        "Enter question",
        placeholder="What are multi-modal LLMs?",
        label_visibility="collapsed",
    )

    if question:
        try:
            response, relative_paths = complete(question)
        except RuntimeError as e:
            st.error(str(e))
            logger.error("Error processing question: %s", e)
            return

        try:
            res_text = response[0].RESPONSE
        except (IndexError, AttributeError) as e:
            logger.error("Unexpected response structure: %s", e)
            st.error("Received an unexpected response format from the model.")
            return

        st.markdown(res_text)

        if relative_paths != "None":
            with st.sidebar.expander("Related Documents"):
                for path in relative_paths:
                    try:
                        cmd2 = f"select GET_PRESIGNED_URL(@docs, '{path}', 360) as URL_LINK from directory(@docs)"
                        df_url_link = session.sql(cmd2).to_pandas()
                        if df_url_link.empty:
                            logger.warning("No presigned URL returned for path: %s", path)
                            st.sidebar.warning(f"Could not generate link for: {path}")
                            continue
                        url_link = df_url_link.iloc[0]["URL_LINK"]
                        display_url = f"Doc: [{path}]({url_link})"
                        st.sidebar.markdown(display_url)
                    except Exception as e:
                        logger.error("Failed to generate presigned URL for %s: %s", path, e)
                        st.sidebar.warning(f"Could not generate link for: {path}")


if __name__ == "__main__":
    main()
