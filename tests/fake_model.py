"""A scripted model for Shellmate: it returns prepared replies in order and records each request."""

from __future__ import annotations

from dataclasses import replace

from runtime.messages import Message, ToolCall
from runtime.model import ModelReply, ToolSpec, Usage

USAGE = Usage(input_tokens=100, output_tokens=10)


class ScriptedModel:
    """Each reply is a ModelReply, or an exception to raise in its place. When the script runs
    out, the last reply is repeated, so a model that never stops can be scripted with one. Tool
    calls get fresh ids on every request, as a real model's do."""

    def __init__(self, *replies: ModelReply | Exception):
        self.replies = list(replies)
        self.requests: list[tuple[str, list[Message], list[ToolSpec]]] = []

    async def complete(self, system, messages, tools, on_delta=None) -> ModelReply:
        self.requests.append((system, list(messages), list(tools)))
        reply = self.replies.pop(0) if len(self.replies) > 1 else self.replies[0]
        if isinstance(reply, Exception):
            raise reply
        n = len(self.requests)
        calls = tuple(replace(c, id=f"call-{n}-{i}") for i, c in enumerate(reply.tool_calls))
        return replace(reply, tool_calls=calls)


def call(name: str, **arguments) -> ModelReply:
    """A reply that calls one tool."""
    return ModelReply(tool_calls=(ToolCall("", name, arguments),), usage=USAGE)


def say(text: str) -> ModelReply:
    """A final reply: no tool calls."""
    return ModelReply(text=text, usage=USAGE)
