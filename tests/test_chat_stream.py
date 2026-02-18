from fastapi.testclient import TestClient

import src.api as api


class _StubChatService:
    def __init__(self):
        self.last_query = None

    def chat_with_copilot(self, query: str):
        self.last_query = query
        return {"response": f"Echo: {query}"}


def test_chat_stream_accepts_ai_sdk_message_shape(monkeypatch):
    stub = _StubChatService()
    monkeypatch.setattr(api, "service", stub)

    client = TestClient(api.app)
    payload = {
        "messages": [
            {"role": "assistant", "content": "prior"},
            {
                "role": "user",
                "parts": [
                    {"type": "text", "text": "show"},
                    {"type": "text", "text": "pending contracts"},
                ],
            },
        ]
    }

    response = client.post("/chat/stream", json=payload)

    assert response.status_code == 200
    assert response.text == "Echo: show pending contracts"
    assert stub.last_query == "show pending contracts"


def test_chat_stream_rejects_invalid_messages_shape(monkeypatch):
    stub = _StubChatService()
    monkeypatch.setattr(api, "service", stub)

    client = TestClient(api.app)
    response = client.post("/chat/stream", json={"messages": "not-a-list"})

    assert response.status_code == 400
    assert response.json()["detail"] == "messages must be a list"


def test_chat_stream_rejects_missing_user_message(monkeypatch):
    stub = _StubChatService()
    monkeypatch.setattr(api, "service", stub)

    client = TestClient(api.app)
    response = client.post("/chat/stream", json={"messages": [{"role": "assistant", "content": "hi"}]})

    assert response.status_code == 400
    assert response.json()["detail"] == "No user message found"
