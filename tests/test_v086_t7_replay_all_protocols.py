"""T7 regression: Chat, Responses and Messages models replay turn-1 tool calls on turn 3, per user|flow|session.

Runs against the real Protected* classes (with whatever ChatOpenAI/ChatAnthropic base is installed); only the
base ``_generate`` network call is stubbed.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from lfx_snowflake import standard_langflow as sl
from lfx_snowflake.providers import messages_replay as mr

TOOLS = [{"type": "function", "function": {"name": "calc", "parameters": {"type": "object", "properties": {}}}}]
H1 = {"type": "human", "content": "what is 17*23?"}
TC = {"type": "ai", "content": "", "tool_calls": [{"name": "calc", "args": {"expr": "17*23"}, "id": "call_1"}]}
TR = {"type": "tool", "content": "391", "tool_call_id": "call_1"}
FINAL = {"type": "ai", "content": "391"}
H2 = {"type": "human", "content": "did you use a tool in turn 1?"}

CASES = [
    ("chat", sl.ProtectedChat, sl.ChatOpenAI),
    ("responses", sl.ProtectedResponses, sl.ChatOpenAI),
    ("messages", sl.ProtectedAnthropic, sl.ChatAnthropic),
]


def _result(msg: dict) -> SimpleNamespace:
    return SimpleNamespace(generations=[SimpleNamespace(message=msg)])


def _model(cls, session: str):
    model = cls(model="m", api_key="k")
    model._sflf_session_id = session
    return model


@pytest.fixture(autouse=True)
def _clean_store():
    mr.STORE.clear()
    yield
    mr.STORE.clear()


def _stub(monkeypatch, base, replies, sent):
    it = iter(replies)

    def fake_generate(self, messages, stop=None, run_manager=None, **kwargs):
        sent.append(list(messages))
        return _result(next(it))

    monkeypatch.setattr(base, "_generate", fake_generate, raising=False)


@pytest.mark.parametrize(("protocol", "cls", "base"), CASES, ids=[c[0] for c in CASES])
def test_three_turn_tool_replay_per_protocol(protocol, cls, base, monkeypatch: pytest.MonkeyPatch) -> None:
    sent: list = []
    _stub(monkeypatch, base, [TC, FINAL, {"type": "ai", "content": "Yes, calc"}], sent)
    model = _model(cls, "sess-A")
    model._generate([H1], tools=TOOLS)  # turn 1a -> tool call
    model._generate([H1, TC, TR], tools=TOOLS)  # turn 1b -> final "391" (records the loop)
    model._generate([H1, dict(FINAL), H2], tools=TOOLS)  # turn 3: Langflow history keeps only plain text
    last = sent[-1]
    assert [m["type"] for m in last] == ["human", "ai", "tool", "ai", "human"], protocol
    assert last[1]["tool_calls"][0]["id"] == "call_1"
    assert last[2]["content"] == "391"

    # Different session in the same process never sees sess-A's tool loop.
    _stub(monkeypatch, base, [{"type": "ai", "content": "No"}], sent)
    _model(cls, "sess-B")._generate([H1, dict(FINAL), H2], tools=TOOLS)
    assert [m["type"] for m in sent[-1]] == ["human", "ai", "human"]


@pytest.mark.parametrize(("protocol", "cls", "base"), CASES, ids=[c[0] for c in CASES])
def test_no_session_means_no_replay(protocol, cls, base, monkeypatch: pytest.MonkeyPatch) -> None:
    sent: list = []
    _stub(monkeypatch, base, [FINAL, {"type": "ai", "content": "No"}], sent)
    monkeypatch.setattr(mr, "current_session_id", lambda: None)
    model = cls(model="m", api_key="k")
    model._generate([H1, TC, TR], tools=TOOLS)
    model._generate([H1, dict(FINAL), H2], tools=TOOLS)
    assert [m["type"] for m in sent[-1]] == ["human", "ai", "human"]


def test_build_maps_chat_and_responses_to_replay_classes() -> None:
    assert sl._REPLAY_CLASSES == {"responses": sl.ProtectedResponses, "chat": sl.ProtectedChat}
    for cls in (sl.ProtectedChat, sl.ProtectedResponses, sl.ProtectedAnthropic):
        assert issubclass(cls, sl._SflfReplayMixin)
    assert issubclass(sl.ProtectedChat, sl.ChatOpenAI)
    assert sl.ProtectedChat._sflf_thinking_on(SimpleNamespace(thinking=None)) is False
