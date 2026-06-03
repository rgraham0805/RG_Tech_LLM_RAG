"""Unit tests for streamlit_app.py.

All Streamlit / Snowflake dependencies are replaced by fakes via the
``mock_env`` fixture defined in conftest.py.
"""

import json
from unittest.mock import MagicMock

import pytest


# =====================================================================
# Module-level constants
# =====================================================================


class TestModuleConstants:
    def test_num_chunks(self, mock_env):
        assert mock_env["app"].NUM_CHUNKS == 3

    def test_columns(self, mock_env):
        assert mock_env["app"].COLUMNS == ["chunk", "relative_path", "category"]

    def test_cortex_search_database(self, mock_env):
        assert mock_env["app"].CORTEX_SEARCH_DATABASE == "SG_SEARCH_DOCS"

    def test_cortex_search_schema(self, mock_env):
        assert mock_env["app"].CORTEX_SEARCH_SCHEMA == "DATA"

    def test_cortex_search_service(self, mock_env):
        assert mock_env["app"].CORTEX_SEARCH_SERVICE == "SG_SEARCH_SERVICE_CS"


# =====================================================================
# get_similar_chunks_search_service
# =====================================================================


class TestGetSimilarChunksSearchService:
    @pytest.fixture(autouse=True)
    def _setup(self, mock_env):
        self.app = mock_env["app"]
        self.st = mock_env["st"]
        self.svc = mock_env["svc"]

        self.search_results = {
            "results": [
                {"chunk": "text1", "relative_path": "doc1.pdf", "category": "tech"},
                {"chunk": "text2", "relative_path": "doc2.pdf", "category": "science"},
            ]
        }
        mock_response = MagicMock()
        mock_response.json.return_value = json.dumps(self.search_results)
        self.svc.search.return_value = mock_response

    def test_all_categories_no_filter(self):
        self.st.session_state["category_value"] = "ALL"
        self.app.get_similar_chunks_search_service("test query")

        self.svc.search.assert_called_once_with(
            "test query",
            self.app.COLUMNS,
            limit=self.app.NUM_CHUNKS,
        )

    def test_specific_category_applies_filter(self):
        self.st.session_state["category_value"] = "tech"
        self.app.get_similar_chunks_search_service("test query")

        expected_filter = {"@eq": {"category": "tech"}}
        self.svc.search.assert_called_once_with(
            "test query",
            self.app.COLUMNS,
            filter=expected_filter,
            limit=self.app.NUM_CHUNKS,
        )

    def test_returns_json_string(self):
        self.st.session_state["category_value"] = "ALL"
        result = self.app.get_similar_chunks_search_service("query")

        parsed = json.loads(result)
        assert "results" in parsed
        assert len(parsed["results"]) == 2

    def test_displays_json_in_sidebar(self):
        self.st.session_state["category_value"] = "ALL"
        result = self.app.get_similar_chunks_search_service("query")
        self.st.sidebar.json.assert_called_once_with(result)


# =====================================================================
# create_prompt
# =====================================================================


class TestCreatePrompt:
    @pytest.fixture(autouse=True)
    def _setup(self, mock_env):
        self.app = mock_env["app"]
        self.st = mock_env["st"]
        self.svc = mock_env["svc"]

    def _mock_search(self, paths=None):
        if paths is None:
            paths = ["doc1.pdf"]
        results = [
            {"chunk": f"chunk for {p}", "relative_path": p, "category": "cat"}
            for p in paths
        ]
        mock_resp = MagicMock()
        mock_resp.json.return_value = json.dumps({"results": results})
        self.svc.search.return_value = mock_resp

    # -- RAG enabled --

    def test_rag_enabled_prompt_contains_context_tags(self):
        self.st.session_state["rag"] = 1
        self.st.session_state["category_value"] = "ALL"
        self._mock_search()

        prompt, _ = self.app.create_prompt("What is AI?")
        assert "<context>" in prompt
        assert "</context>" in prompt

    def test_rag_enabled_prompt_contains_question_tags(self):
        self.st.session_state["rag"] = 1
        self.st.session_state["category_value"] = "ALL"
        self._mock_search()

        prompt, _ = self.app.create_prompt("What is AI?")
        assert "<question>" in prompt
        assert "</question>" in prompt

    def test_rag_enabled_prompt_embeds_question_text(self):
        self.st.session_state["rag"] = 1
        self.st.session_state["category_value"] = "ALL"
        self._mock_search()

        prompt, _ = self.app.create_prompt("What is AI?")
        assert "What is AI?" in prompt

    def test_rag_enabled_prompt_embeds_context_data(self):
        self.st.session_state["rag"] = 1
        self.st.session_state["category_value"] = "ALL"
        self._mock_search(["report.pdf"])

        prompt, _ = self.app.create_prompt("summarize")
        assert "report.pdf" in prompt

    def test_rag_enabled_returns_relative_paths_as_set(self):
        self.st.session_state["rag"] = 1
        self.st.session_state["category_value"] = "ALL"
        self._mock_search(["doc1.pdf", "doc2.pdf"])

        _, paths = self.app.create_prompt("query")
        assert paths == {"doc1.pdf", "doc2.pdf"}

    def test_rag_enabled_deduplicates_paths(self):
        self.st.session_state["rag"] = 1
        self.st.session_state["category_value"] = "ALL"
        results = {
            "results": [
                {"chunk": "a", "relative_path": "dup.pdf", "category": "c"},
                {"chunk": "b", "relative_path": "dup.pdf", "category": "c"},
            ]
        }
        mock_resp = MagicMock()
        mock_resp.json.return_value = json.dumps(results)
        self.svc.search.return_value = mock_resp

        _, paths = self.app.create_prompt("q")
        assert paths == {"dup.pdf"}

    # -- RAG disabled --

    def test_rag_disabled_prompt_contains_question(self):
        self.st.session_state["rag"] = 0

        prompt, _ = self.app.create_prompt("What is AI?")
        assert "What is AI?" in prompt

    def test_rag_disabled_no_context_tags(self):
        self.st.session_state["rag"] = 0

        prompt, _ = self.app.create_prompt("What is AI?")
        assert "<context>" not in prompt

    def test_rag_disabled_returns_none_string(self):
        self.st.session_state["rag"] = 0

        _, paths = self.app.create_prompt("query")
        assert paths == "None"

    def test_rag_disabled_does_not_call_search(self):
        self.st.session_state["rag"] = 0

        self.app.create_prompt("query")
        self.svc.search.assert_not_called()


# =====================================================================
# complete
# =====================================================================


class TestComplete:
    @pytest.fixture(autouse=True)
    def _setup(self, mock_env):
        self.app = mock_env["app"]
        self.st = mock_env["st"]
        self.session = mock_env["session"]
        self.svc = mock_env["svc"]

    def _setup_sql_response(self, text="answer"):
        mock_row = MagicMock()
        mock_row.RESPONSE = text
        self.session.sql.return_value.collect.return_value = [mock_row]
        return mock_row

    def test_executes_cortex_complete_sql(self):
        self.st.session_state["rag"] = 0
        self.st.session_state["model_name"] = "mistral-large2"
        self._setup_sql_response()

        self.app.complete("What is AI?")

        # At least one call should contain the cortex.complete SQL
        sql_calls = [
            call[0][0] for call in self.session.sql.call_args_list
        ]
        assert any("snowflake.cortex.complete" in sql for sql in sql_calls)

    def test_passes_model_name_as_param(self):
        self.st.session_state["rag"] = 0
        self.st.session_state["model_name"] = "llama3.1-70b"
        self._setup_sql_response()

        self.app.complete("query")

        call_args = self.session.sql.call_args
        # session.sql(cmd, params=[model_name, prompt])
        params = call_args[1].get("params", call_args[0][1] if len(call_args[0]) > 1 else None)
        assert params is not None
        assert params[0] == "llama3.1-70b"

    def test_returns_collected_response(self):
        self.st.session_state["rag"] = 0
        self.st.session_state["model_name"] = "mistral-large2"
        self._setup_sql_response("42")

        response, _ = self.app.complete("meaning of life")
        assert response[0].RESPONSE == "42"

    def test_without_rag_returns_none_paths(self):
        self.st.session_state["rag"] = 0
        self.st.session_state["model_name"] = "mistral-large2"
        self._setup_sql_response()

        _, paths = self.app.complete("query")
        assert paths == "None"

    def test_with_rag_returns_document_paths(self):
        self.st.session_state["rag"] = 1
        self.st.session_state["category_value"] = "ALL"
        self.st.session_state["model_name"] = "mistral-large2"

        search_data = {
            "results": [
                {"chunk": "info", "relative_path": "guide.pdf", "category": "docs"}
            ]
        }
        mock_resp = MagicMock()
        mock_resp.json.return_value = json.dumps(search_data)
        self.svc.search.return_value = mock_resp
        self._setup_sql_response()

        _, paths = self.app.complete("question")
        assert paths == {"guide.pdf"}


# =====================================================================
# config_options
# =====================================================================


class TestConfigOptions:
    @pytest.fixture(autouse=True)
    def _setup(self, mock_env):
        self.app = mock_env["app"]
        self.st = mock_env["st"]
        self.session = mock_env["session"]

    def _mock_categories(self, names):
        cats = []
        for name in names:
            cat = MagicMock()
            cat.CATEGORY = name
            cats.append(cat)
        self.session.sql.return_value.collect.return_value = cats

    def test_creates_model_selectbox(self):
        self._mock_categories(["tech"])

        self.app.config_options()

        calls = self.st.sidebar.selectbox.call_args_list
        model_call = calls[0]
        assert "model" in model_call[0][0].lower()
        models = model_call[0][1]
        assert "mistral-large2" in models
        assert "llama3.1-70b" in models
        assert "llama3.1-8b" in models
        assert "snowflake-arctic" in models

    def test_creates_category_selectbox_starting_with_all(self):
        self._mock_categories(["tech", "science"])

        self.app.config_options()

        calls = self.st.sidebar.selectbox.call_args_list
        cat_call = calls[1]
        categories = cat_call[0][1]
        assert categories[0] == "ALL"
        assert "tech" in categories
        assert "science" in categories

    def test_queries_categories_from_database(self):
        self._mock_categories([])

        self.app.config_options()

        sql_arg = self.session.sql.call_args[0][0]
        assert "category" in sql_arg.lower()
        assert "docs_chunks_table" in sql_arg.lower()

    def test_shows_session_state_in_expander(self):
        self._mock_categories([])

        self.app.config_options()

        self.st.sidebar.expander.assert_called_once_with("Session State")


# =====================================================================
# main
# =====================================================================


class TestMain:
    @pytest.fixture(autouse=True)
    def _setup(self, mock_env):
        self.app = mock_env["app"]
        self.st = mock_env["st"]
        self.session = mock_env["session"]
        self.svc = mock_env["svc"]
        # Default: no question, no categories
        self.session.sql.return_value.collect.return_value = []
        self.st.sidebar.checkbox.return_value = 0
        self.st.text_input.return_value = ""

    def test_sets_title(self):
        self.app.main()

        self.st.title.assert_called_once()
        title_arg = self.st.title.call_args[0][0]
        assert "Chat Assistant" in title_arg

    def test_creates_text_input(self):
        self.app.main()
        self.st.text_input.assert_called_once()

    def test_creates_rag_checkbox(self):
        self.app.main()
        self.st.sidebar.checkbox.assert_called_once()

    def test_no_question_does_not_call_complete(self):
        self.app.main()

        for call_args in self.session.sql.call_args_list:
            sql = call_args[0][0]
            assert "cortex.complete" not in sql.lower()

    def test_question_triggers_markdown_response(self):
        self.st.text_input.return_value = "What is RAG?"
        self.st.sidebar.checkbox.return_value = 0
        self.st.session_state["model_name"] = "mistral-large2"

        mock_row = MagicMock()
        mock_row.RESPONSE = "RAG is Retrieval-Augmented Generation"
        self.session.sql.return_value.collect.return_value = [mock_row]

        self.app.main()

        self.st.markdown.assert_called_once_with(
            "RAG is Retrieval-Augmented Generation"
        )

    def test_question_with_rag_shows_related_documents_expander(self):
        self.st.text_input.return_value = "question"
        self.st.sidebar.checkbox.return_value = 1
        self.st.session_state["model_name"] = "mistral-large2"
        self.st.session_state["category_value"] = "ALL"

        search_data = {
            "results": [
                {"chunk": "info", "relative_path": "guide.pdf", "category": "docs"}
            ]
        }
        mock_resp = MagicMock()
        mock_resp.json.return_value = json.dumps(search_data)
        self.svc.search.return_value = mock_resp

        mock_row = MagicMock()
        mock_row.RESPONSE = "answer"
        self.session.sql.return_value.collect.return_value = [mock_row]
        self.session.sql.return_value.to_pandas.return_value._get_value.return_value = (
            "https://example.com/guide.pdf"
        )

        self.app.main()

        self.st.sidebar.expander.assert_called_with("Related Documents")

    def test_question_with_rag_renders_doc_links(self):
        self.st.text_input.return_value = "question"
        self.st.sidebar.checkbox.return_value = 1
        self.st.session_state["model_name"] = "mistral-large2"
        self.st.session_state["category_value"] = "ALL"

        search_data = {
            "results": [
                {"chunk": "info", "relative_path": "guide.pdf", "category": "docs"}
            ]
        }
        mock_resp = MagicMock()
        mock_resp.json.return_value = json.dumps(search_data)
        self.svc.search.return_value = mock_resp

        mock_row = MagicMock()
        mock_row.RESPONSE = "answer"
        self.session.sql.return_value.collect.return_value = [mock_row]
        self.session.sql.return_value.to_pandas.return_value._get_value.return_value = (
            "https://example.com/guide.pdf"
        )

        self.app.main()

        # Verify sidebar.markdown was called with a link
        md_calls = [
            str(c) for c in self.st.sidebar.markdown.call_args_list
        ]
        assert any("guide.pdf" in c for c in md_calls)

    def test_no_rag_does_not_show_related_documents(self):
        self.st.text_input.return_value = "question"
        self.st.sidebar.checkbox.return_value = 0
        self.st.session_state["model_name"] = "mistral-large2"

        mock_row = MagicMock()
        mock_row.RESPONSE = "answer"
        self.session.sql.return_value.collect.return_value = [mock_row]

        self.app.main()

        # The "Related Documents" expander should NOT be created
        expander_calls = [
            str(c) for c in self.st.sidebar.expander.call_args_list
        ]
        assert not any("Related Documents" in c for c in expander_calls)
