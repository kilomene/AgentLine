"""Wire protocol for AgentLine.

Every frame is a JSON object sent over a websocket. Two directions:

Client -> Server
    {"type": "hello", "code": "ABC123"|null, "name": "Phoenix",
     "secret": "..."|null, "ephemeral": false}
        Claim a code (code=null asks the server to assign one).
        ephemeral=true gets a throwaway code that is never persisted
        (used by the dial CLI).

    {"type": "send", "to": "XYZ789", "body": "...", "id": "uuid",
     "in_reply_to": "uuid"|null}

    {"type": "ping"}

Server -> Client
    {"type": "welcome", "code": "ABC123", "secret": "...",
     "ephemeral": false, "queued": 2}

    {"type": "error", "reason": "code_taken" | "unknown_recipient"
                              | "bad_code" | "not_registered",
     "detail": "..."}

    {"type": "message", "id": "uuid", "from": "ABC123",
     "from_name": "Phoenix", "body": "...", "ts": 1234567890,
     "in_reply_to": "uuid"|null, "queued": false}

    {"type": "delivered", "id": "uuid", "to": "XYZ789", "queued": false}

    {"type": "pong"}
"""

from __future__ import annotations

import re
import secrets
import time
import uuid

# 4-12 chars, unambiguous alphabet (no 0/O, 1/I/L).
CODE_RE = re.compile(r"^[A-Z2-9]{4,12}$")
_CODE_ALPHABET = "ABCDEFGHJKMNPQRSTUVWXYZ23456789"


def new_code(n: int = 6) -> str:
    return "".join(secrets.choice(_CODE_ALPHABET) for _ in range(n))


def valid_code(code: object) -> bool:
    return isinstance(code, str) and CODE_RE.match(code) is not None


def new_secret() -> str:
    return secrets.token_urlsafe(24)


def new_id() -> str:
    return uuid.uuid4().hex


def now_ts() -> int:
    return int(time.time())
