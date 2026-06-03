# Prompt-building utilities.
#
# Extracts the two prompt templates that were previously inlined inside
# ``create_prompt`` in streamlit_app.py so they can be tested and reused
# independently.

import json


def build_rag_prompt(context_json, question):
    """Build a RAG prompt from search-service *context_json* and a *question*.

    Returns ``(prompt_text, relative_paths)`` where *relative_paths* is the
    set of source-document paths extracted from the context.
    """
    prompt = (
        "You are an expert chat assistance that extracts information from the CONTEXT provided "
        "between <context> and </context> tags.\n"
        "When answering the question contained between <question> and </question> tags "
        "be thorough and do not hallucinate.\n"
        "If you don't have the information just say so.\n"
        "Only answer the question if you can extract it from the CONTEXT provided.\n"
        "Do not mention the CONTEXT used in your answer.\n\n"
        f"<context>\n{context_json}\n</context>\n"
        f"<question>\n{question}\n</question>\n"
        "Answer: "
    )

    results = json.loads(context_json)
    relative_paths = {item["relative_path"] for item in results["results"]}

    return prompt, relative_paths


def build_direct_prompt(question):
    """Build a simple (non-RAG) prompt for *question*.

    Returns ``(prompt_text, None)`` — *None* signals that no source
    documents are associated with the answer.
    """
    prompt = f"Question:\n{question}\nAnswer: "
    return prompt, None
