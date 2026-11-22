"""A Shellmate model client for Together AI, which serves open-weights models too large to run
on one machine, such as Thinking Machines' Inkling (975B parameters, 41B active).

    model = together_model()                       thinkingmachines/Inkling
    model = together_model("some-org/other-model")

Together speaks the OpenAI Chat Completions format, the same as DeepSeek's API, so this is
Shellmate's DeepSeek client pointed at Together's endpoint and keyed by TOGETHER_API_KEY.
TOGETHER_BASE_URL overrides the endpoint.
"""

from __future__ import annotations

import os

import openai
from runtime.deepseek_model import DeepSeekModel
from runtime.providers import max_tokens_from_env

BASE_URL = "https://api.together.xyz/v1"
DEFAULT_MODEL = "thinkingmachines/Inkling"
KEY_ENV = "TOGETHER_API_KEY"


def together_model(model: str | None = None, max_retries: int = 3) -> DeepSeekModel:
    """A chat-completions client for `model` on Together. Raises RuntimeError if
    TOGETHER_API_KEY isn't set."""
    key = os.environ.get(KEY_ENV, "").strip()
    if not key:
        raise RuntimeError(f"{KEY_ENV} is not set; Together's models need it")
    client = openai.AsyncOpenAI(
        base_url=os.environ.get("TOGETHER_BASE_URL") or BASE_URL,
        api_key=key,
        max_retries=max_retries,
    )
    return DeepSeekModel(model=model or DEFAULT_MODEL, max_tokens=max_tokens_from_env(), client=client)
