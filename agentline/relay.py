"""The AgentLine relay: routes messages between agents by permanent code."""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
from dataclasses import dataclass, field

import websockets
import websockets.exceptions
from websockets.server import serve

from .protocol import new_code, new_id, now_ts, valid_code
from .store import CodeTaken, Store

log = logging.getLogger("agentline.relay")


@dataclass
class Peer:
    ws: object
    code: str
    name: str
    ephemeral: bool = False


@dataclass
class Relay:
    host: str = "127.0.0.1"
    port: int = 8765
    db_path: str = "~/.agentline/relay.db"
    peers: dict = field(default_factory=dict)  # code -> Peer
    _server: object = field(default=None, repr=False)
    _serve_task: object = field(default=None, repr=False)

    def __post_init__(self) -> None:
        self.store = Store(os.path.expanduser(self.db_path))

    # -- lifecycle ------------------------------------------------------
    async def start(self) -> "Relay":
        self._server = await serve(self._handle, self.host, self.port)
        # Resolve the real port when port=0 was requested.
        sockets = self._server.sockets or ()
        if sockets:
            self.port = sockets[0].getsockname()[1]
        log.info("relay listening on %s:%s", self.host, self.port)
        return self

    async def stop(self) -> None:
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
            self._server = None
        self.store.close()

    # -- websocket handler ----------------------------------------------
    async def _handle(self, ws) -> None:
        peer: Peer | None = None
        try:
            async for raw in ws:
                try:
                    frame = json.loads(raw)
                except (json.JSONDecodeError, TypeError):
                    await self._send(ws, {"type": "error", "reason": "bad_frame"})
                    continue
                if not isinstance(frame, dict):
                    await self._send(ws, {"type": "error", "reason": "bad_frame"})
                    continue
                ftype = frame.get("type")
                if ftype == "hello":
                    peer = await self._on_hello(ws, frame, peer)
                elif ftype == "send":
                    await self._on_send(ws, frame, peer)
                elif ftype == "ping":
                    await self._send(ws, {"type": "pong"})
                else:
                    await self._send(ws, {"type": "error", "reason": "unknown_type"})
        except websockets.exceptions.ConnectionClosed:
            pass
        finally:
            if peer is not None and self.peers.get(peer.code) is peer:
                del self.peers[peer.code]
                log.info("peer %s disconnected", peer.code)

    @staticmethod
    async def _send(ws, frame: dict) -> None:
        await ws.send(json.dumps(frame))

    async def _on_hello(self, ws, frame: dict, old: Peer | None) -> Peer | None:
        if old is not None and self.peers.get(old.code) is old:
            del self.peers[old.code]

        code = frame.get("code")
        name = str(frame.get("name") or "")[:64]
        secret = frame.get("secret")
        ephemeral = bool(frame.get("ephemeral"))

        if ephemeral:
            code = new_code()
            peer = Peer(ws=ws, code=code, name=name or "dial", ephemeral=True)
            self.peers[code] = peer
            await self._send(
                ws,
                {"type": "welcome", "code": code, "secret": None,
                 "ephemeral": True, "queued": 0},
            )
            log.info("ephemeral peer %s connected", code)
            return peer

        if code is None:
            # Ask the server to assign a permanent code.
            for _ in range(20):
                candidate = new_code()
                if candidate not in self.peers and not self.store.exists(candidate):
                    code = candidate
                    break
            else:
                await self._send(ws, {"type": "error", "reason": "assign_failed"})
                return None

        if not valid_code(code):
            await self._send(
                ws, {"type": "error", "reason": "bad_code",
                     "detail": "codes are 4-12 chars of A-Z 2-9"})
            return None

        if self.store.exists(code):
            # Reclaim path: the owner presents the secret.
            if self.store.verify(code, secret):
                self.store.set_name(code, name)
            else:
                await self._send(ws, {"type": "error", "reason": "code_taken"})
                return None
            owner_secret = secret
        else:
            try:
                owner_secret = self.store.claim(code, name)
            except CodeTaken:
                await self._send(ws, {"type": "error", "reason": "code_taken"})
                return None

        # Supersede any live socket still holding this code.
        stale = self.peers.get(code)
        if stale is not None and stale.ws is not ws:
            try:
                await stale.ws.close(code=4000, reason="superseded")
            except Exception:
                pass

        peer = Peer(ws=ws, code=code, name=name)
        self.peers[code] = peer

        queued = self.store.dequeue_all(code)
        await self._send(
            ws,
            {"type": "welcome", "code": code, "secret": owner_secret,
             "ephemeral": False, "queued": len(queued)},
        )
        for q in queued:
            await self._send(
                ws,
                {"type": "message", "id": q["id"], "from": q["from_code"],
                 "from_name": q["from_name"], "body": q["body"],
                 "ts": q["ts"], "in_reply_to": q["in_reply_to"],
                 "queued": True},
            )
        log.info("peer %s (%s) registered, flushed %d queued",
                 code, name, len(queued))
        return peer

    async def _on_send(self, ws, frame: dict, peer: Peer | None) -> None:
        if peer is None:
            await self._send(ws, {"type": "error", "reason": "not_registered"})
            return
        to_code = frame.get("to")
        body = frame.get("body")
        msg_id = frame.get("id") or new_id()
        in_reply_to = frame.get("in_reply_to")
        if not isinstance(to_code, str) or not isinstance(body, str):
            await self._send(ws, {"type": "error", "reason": "bad_send"})
            return
        if not valid_code(to_code):
            await self._send(ws, {"type": "error", "reason": "unknown_recipient"})
            return
        if to_code not in self.peers and not self.store.exists(to_code):
            await self._send(ws, {"type": "error", "reason": "unknown_recipient"})
            return

        target = self.peers.get(to_code)
        if target is not None:
            # Online: deliver live (ephemeral dial clients can receive too).
            try:
                await self._send(
                    target.ws,
                    {"type": "message", "id": msg_id, "from": peer.code,
                     "from_name": peer.name, "body": body, "ts": now_ts(),
                     "in_reply_to": in_reply_to, "queued": False},
                )
                await self._send(
                    ws, {"type": "delivered", "id": msg_id,
                         "to": to_code, "queued": False})
                return
            except websockets.exceptions.ConnectionClosed:
                pass  # fall through to queueing
        # Offline: queue for later delivery.
        self.store.enqueue(msg_id, to_code, peer.code, peer.name, body,
                           in_reply_to)
        await self._send(
            ws, {"type": "delivered", "id": msg_id, "to": to_code,
                 "queued": True})


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(description="Run the AgentLine relay server.")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=8765)
    ap.add_argument("--db", default="~/.agentline/relay.db")
    args = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")

    async def _run() -> None:
        relay = Relay(host=args.host, port=args.port, db_path=args.db)
        await relay.start()
        try:
            await asyncio.Future()  # serve forever
        except (KeyboardInterrupt, asyncio.CancelledError):
            pass
        finally:
            await relay.stop()

    asyncio.run(_run())


if __name__ == "__main__":
    main()
