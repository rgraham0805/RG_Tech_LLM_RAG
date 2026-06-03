# Streamlit RAG Chat Assistant
#
# All Snowflake connection logic, prompt templates, and configuration
# constants now live in shared utility modules so nothing is duplicated.

import streamlit as st
import pandas as pd

from config import AVAILABLE_MODELS
from snowflake_utils import (
    get_categories,
    get_presigned_url,
    run_completion,
    search_documents,
)
from prompts import build_direct_prompt, build_rag_prompt

pd.set_option("max_colwidth", None)


# ---------------------------------------------------------------------------
# Sidebar configuration
# ---------------------------------------------------------------------------


def config_options():
    st.sidebar.selectbox("Select your model:", AVAILABLE_MODELS, key="model_name")

    categories = get_categories()

    cat_list = ["ALL"]
    for cat in categories:
        cat_list.append(cat.CATEGORY)

    st.sidebar.selectbox(
        "Select what products you are looking for", cat_list, key="category_value"
    )

    st.sidebar.expander("Session State").write(st.session_state)


# ---------------------------------------------------------------------------
# Core helpers
# ---------------------------------------------------------------------------


def get_similar_chunks(query):
    """Search for relevant document chunks and display the raw JSON in the sidebar."""
    response_json = search_documents(query, st.session_state.category_value)
    st.sidebar.json(response_json)
    return response_json


def create_prompt(question):
    """Build a prompt (with or without RAG context) for *question*."""
    if st.session_state.rag:
        context_json = get_similar_chunks(question)
        return build_rag_prompt(context_json, question)
    return build_direct_prompt(question)


def complete(question):
    """Send *question* through the selected LLM and return (response_rows, relative_paths)."""
    prompt, relative_paths = create_prompt(question)
    df_response = run_completion(st.session_state.model_name, prompt)
    return df_response, relative_paths


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main():
    st.title(":speech_balloon: Steve's Chat Assistant")

    config_options()

    st.session_state.rag = st.sidebar.checkbox("Use your own documents as context?")

    question = st.text_input(
        "Enter question",
        placeholder="What are multi-modal LLMs?",
        label_visibility="collapsed",
    )

    if question:
        response, relative_paths = complete(question)
        res_text = response[0].RESPONSE
        st.markdown(res_text)

        if relative_paths is not None:
            with st.sidebar.expander("Related Documents"):
                for path in relative_paths:
                    url_link = get_presigned_url(path)
                    st.sidebar.markdown(f"Doc: [{path}]({url_link})")


if __name__ == "__main__":
    main()
