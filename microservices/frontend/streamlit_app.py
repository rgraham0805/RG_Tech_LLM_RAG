"""Frontend — Streamlit UI that talks to the API Gateway instead of Snowflake directly."""

import os

import requests
import streamlit as st

GATEWAY_URL = os.environ.get("SERVICE_GATEWAY_URL", "http://api-gateway:8000")


def get_models() -> list[str]:
    """Fetch available models from the API Gateway."""
    resp = requests.get(f"{GATEWAY_URL}/api/v1/models", timeout=10)
    resp.raise_for_status()
    return resp.json()["models"]


def get_categories() -> list[str]:
    """Fetch document categories from the API Gateway."""
    resp = requests.get(f"{GATEWAY_URL}/api/v1/documents/categories", timeout=10)
    resp.raise_for_status()
    return resp.json()["categories"]


def get_presigned_urls(paths: list[str]) -> dict[str, str]:
    """Fetch presigned URLs for source documents."""
    resp = requests.post(
        f"{GATEWAY_URL}/api/v1/documents/presigned-urls",
        json={"paths": paths},
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()["urls"]


def chat(question: str, model: str, use_rag: bool, category: str) -> dict:
    """Send a chat request through the API Gateway."""
    resp = requests.post(
        f"{GATEWAY_URL}/api/v1/chat",
        json={
            "question": question,
            "use_rag": use_rag,
            "model": model,
            "category": category,
        },
        timeout=120,
    )
    resp.raise_for_status()
    return resp.json()


def config_options():
    models = get_models()
    st.sidebar.selectbox("Select your model:", models, key="model_name")

    categories = ["ALL"] + get_categories()
    st.sidebar.selectbox(
        "Select what products you are looking for", categories, key="category_value"
    )

    st.sidebar.expander("Session State").write(st.session_state)


def main():
    st.title(":speech_balloon: Steve's Chat Assistant")

    config_options()

    use_rag = st.sidebar.checkbox("Use your own documents as context?")

    question = st.text_input(
        "Enter question",
        placeholder="What are multi-modal LLMs?",
        label_visibility="collapsed",
    )

    if question:
        result = chat(
            question=question,
            model=st.session_state.model_name,
            use_rag=use_rag,
            category=st.session_state.category_value,
        )

        st.markdown(result["answer"])

        sources = result.get("sources", [])
        if sources:
            urls = get_presigned_urls(sources)
            with st.sidebar.expander("Related Documents"):
                for path, url in urls.items():
                    st.sidebar.markdown(f"Doc: [{path}]({url})")


if __name__ == "__main__":
    main()
