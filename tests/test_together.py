"""The Together client: which endpoint, key and model it is built with."""

import pytest

from distsys_eval.shellmate import ShellmateAgent
from distsys_eval.together import BASE_URL, DEFAULT_MODEL, together_model


def test_it_needs_the_key(monkeypatch):
    monkeypatch.delenv("TOGETHER_API_KEY", raising=False)
    with pytest.raises(RuntimeError, match="TOGETHER_API_KEY is not set"):
        together_model()


def test_it_calls_together_with_inkling_by_default(monkeypatch):
    monkeypatch.setenv("TOGETHER_API_KEY", "test-key")
    monkeypatch.delenv("TOGETHER_BASE_URL", raising=False)
    model = together_model()
    assert model._model == DEFAULT_MODEL == "thinkingmachines/Inkling"
    assert str(model._client.base_url).rstrip("/") == BASE_URL
    assert model._client.api_key == "test-key"


def test_the_endpoint_and_model_can_be_changed(monkeypatch):
    monkeypatch.setenv("TOGETHER_API_KEY", "test-key")
    monkeypatch.setenv("TOGETHER_BASE_URL", "https://example.test/v1")
    model = together_model("some-org/other-model")
    assert model._model == "some-org/other-model"
    assert str(model._client.base_url).rstrip("/") == "https://example.test/v1"


def test_the_agent_is_named_after_the_together_model(monkeypatch):
    monkeypatch.setenv("TOGETHER_API_KEY", "test-key")
    assert ShellmateAgent.from_provider("together").name == "shellmate/together/thinkingmachines/Inkling"
