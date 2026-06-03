"""Shared test fixtures for streamlit_app tests.

streamlit_app.py executes Snowflake/Streamlit connection code at *import
time*, so every external dependency must be patched in ``sys.modules``
before the module is imported.  The ``mock_env`` fixture handles this:
it injects lightweight fakes, forces a fresh import of ``streamlit_app``,
and tears everything down after each test.
"""

import sys
from unittest.mock import MagicMock

import pytest


class _AttrDict(dict):
    """Minimal dict subclass with attribute access (mirrors st.session_state)."""

    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError(key) from None

    def __setattr__(self, key, value):
        self[key] = value

    def __delattr__(self, key):
        try:
            del self[key]
        except KeyError:
            raise AttributeError(key) from None


@pytest.fixture()
def mock_env():
    """Mock Streamlit + Snowflake, (re-)import streamlit_app, yield handles."""
    saved: dict = {}

    # -- Streamlit mock --
    mock_st = MagicMock(name="streamlit")
    mock_st.session_state = _AttrDict()

    mock_cnx = MagicMock(name="snowflake_cnx")
    mock_session = MagicMock(name="snowflake_session")
    mock_cnx.session.return_value = mock_session
    mock_st.connection.return_value = mock_cnx

    # -- Snowflake / other mocks --
    modules_to_mock = {
        "streamlit": mock_st,
        "snowflake": MagicMock(name="snowflake"),
        "snowflake.core": MagicMock(name="snowflake.core"),
        "snowflake.snowpark": MagicMock(name="snowflake.snowpark"),
        "snowflake.snowpark.context": MagicMock(name="snowflake.snowpark.context"),
        "snowflake.snowpark.functions": MagicMock(name="snowflake.snowpark.functions"),
        "pandas": MagicMock(name="pandas"),
        "requests": MagicMock(name="requests"),
    }

    for name, mock in modules_to_mock.items():
        saved[name] = sys.modules.get(name)
        sys.modules[name] = mock

    # Remove any cached import of the app module
    saved["streamlit_app"] = sys.modules.pop("streamlit_app", None)

    import streamlit_app  # fresh import against fakes

    yield {
        "app": streamlit_app,
        "st": mock_st,
        "session": streamlit_app.session,
        "svc": streamlit_app.svc,
    }

    # -- teardown: restore original sys.modules --
    sys.modules.pop("streamlit_app", None)
    for name, orig in saved.items():
        if orig is None:
            sys.modules.pop(name, None)
        else:
            sys.modules[name] = orig
