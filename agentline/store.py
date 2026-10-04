"""Persistent state for the AgentLine relay: claimed codes and offline queue."""

from __future__ import annotations

import sqlite3
import threading
from pathlib import Path

from .protocol import new_secret, now_ts


class CodeTaken(Exception):
    pass


class Store:
    """SQLite-backed store. Safe to use from the relay's single event loop."""

    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._db = sqlite3.connect(str(self.path), check_same_thread=False)
        self._db.row_factory = sqlite3.Row
        with self._lock:
            self._db.executescript(
                """
                CREATE TABLE IF NOT EXISTS codes (
                    code TEXT PRIMARY KEY,
                    secret TEXT NOT NULL,
                    name TEXT NOT NULL DEFAULT '',
                    created_at INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS queue (
                    id TEXT PRIMARY KEY,
                    to_code TEXT NOT NULL,
                    from_code TEXT NOT NULL,
                    from_name TEXT NOT NULL DEFAULT '',
                    body TEXT NOT NULL,
                    in_reply_to TEXT,
                    ts INTEGER NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_queue_to ON queue (to_code, ts);
                """
            )
            self._db.commit()

    def close(self) -> None:
        with self._lock:
            self._db.close()

    # -- codes -----------------------------------------------------------
    def claim(self, code: str, name: str) -> str:
        """Claim a fresh code. Returns the owner secret. Raises CodeTaken."""
        secret = new_secret()
        with self._lock:
            try:
                self._db.execute(
                    "INSERT INTO codes (code, secret, name, created_at)"
                    " VALUES (?, ?, ?, ?)",
                    (code, secret, name, now_ts()),
                )
                self._db.commit()
            except sqlite3.IntegrityError:
                raise CodeTaken(code)
        return secret

    def verify(self, code: str, secret: str | None) -> bool:
        if not secret:
            return False
        with self._lock:
            row = self._db.execute(
                "SELECT secret FROM codes WHERE code = ?", (code,)
            ).fetchone()
        return bool(row) and row["secret"] == secret

    def exists(self, code: str) -> bool:
        with self._lock:
            row = self._db.execute(
                "SELECT 1 FROM codes WHERE code = ?", (code,)
            ).fetchone()
        return row is not None

    def set_name(self, code: str, name: str) -> None:
        with self._lock:
            self._db.execute("UPDATE codes SET name = ? WHERE code = ?", (name, code))
            self._db.commit()

    # -- offline queue ----------------------------------------------------
    def enqueue(
        self,
        msg_id: str,
        to_code: str,
        from_code: str,
        from_name: str,
        body: str,
        in_reply_to: str | None,
    ) -> None:
        with self._lock:
            self._db.execute(
                "INSERT INTO queue (id, to_code, from_code, from_name, body,"
                " in_reply_to, ts) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (msg_id, to_code, from_code, from_name, body, in_reply_to, now_ts()),
            )
            self._db.commit()

    def dequeue_all(self, to_code: str) -> list[dict]:
        with self._lock:
            rows = self._db.execute(
                "SELECT id, from_code, from_name, body, in_reply_to, ts"
                " FROM queue WHERE to_code = ? ORDER BY ts",
                (to_code,),
            ).fetchall()
            self._db.execute("DELETE FROM queue WHERE to_code = ?", (to_code,))
            self._db.commit()
        return [dict(r) for r in rows]

    def queued_count(self, to_code: str) -> int:
        with self._lock:
            row = self._db.execute(
                "SELECT COUNT(*) AS n FROM queue WHERE to_code = ?", (to_code,)
            ).fetchone()
        return int(row["n"])
