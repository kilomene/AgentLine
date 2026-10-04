"""The AgentLine agent client: claim a permanent code and answer messages."""

from __future__ import annotations

import argparse
import asyncio
import inspect
import json
import logging
import os
from pathlib import Path
from typing import Awaitable, Callable

import websockets.exceptions
from websockets.client import connect

from .protocol import new_id, valid_code

log = logging.getLogger("agentline.agent")

# on_message(from_code, from_name, body) -> reply text | None (may be async)
Handler = Callable[[str, str, str], "str | None | Awaitable[str | None]"]

DEFAULT_STATE_DIR = os.path.expanduser("~/.agentline")


class RegistrationFailed(Exception):
    pass


class Agent:
    """A persistent agent with a permanent code on an AgentLine relay.

    The owner secret is saved in ``state_dir/secret-<CODE>.json`` so the
    agent reclaims its code across restarts. ``on_message`` answers every
    inbound message; returning None sends no reply.
    """

    def __init__(
        self,
        code: str | None,
        name: str,
        on_message: Handler,
        relay_url: str = "ws://127.0.0.1:8765",
        state_dir: str = DEFAULT_STATE_DIR,
    ):
        self.requested_code = code.upper() if code else None
        if self.requested_code and not valid_code(self.requested_code):
            raise ValueError(f"bad code {code!r}: 4-12 chars of A-Z 2-9")
        self.name = name
        self.on_message = on_message
        self.relay_url = relay_url
        self.state_dir = Path(state_dir)
        self.state_dir.mkdir(parents=True, exist_ok=True)
        self.code: str | None = None
        self._ws = None
        self._secret_file: Path | None = None

    # -- secret persistence -------------------------------------------
    def _secret_path(self, code: str) -> Path:
        return self.state_dir / f"secret-{code}.json"

    def _load_secret(self, code: str) -> str | None:
        p = self._secret_path(code)
        if p.exists():
            try:
                return json.loads(p.read_text()).get("secret")
            except (json.JSONDecodeError, OSError, AttributeError):
                return None
        return None

    def _save_secret(self, code: str, secret: str) -> None:
        p = self._secret_path(code)
        p.write_text(json.dumps({"code": code, "secret": secret}))
        try:
            os.chmod(p, 0o600)
        except OSError:
            pass

    # -- connection ----------------------------------------------------
    async def start(self) -> str:
        """Connect and register. Returns the claimed code."""
        code = self.requested_code
        secret = self._load_secret(code) if code else None
        self._ws = await connect(self.relay_url, max_size=2**20)
        await self._send(
            {"type": "hello", "code": code, "name": self.name,
             "secret": secret, "ephemeral": False}
        )
        async for raw in self._ws:
            frame = json.loads(raw)
            if frame.get("type") == "welcome":
                self.code = frame["code"]
                if frame.get("secret"):
                    self._save_secret(self.code, frame["secret"])
                queued = frame.get("queued", 0)
                log.info("registered as %s (%d queued)", self.code, queued)
                return self.code
            if frame.get("type") == "error":
                await self._ws.close()
                raise RegistrationFailed(
                    f"{frame.get('reason')}: {frame.get('detail', '')}")
            # Any queued "message" frames arriving before welcome are
            # impossible by protocol; ignore anything else here.
        raise RegistrationFailed("connection closed before welcome")

    async def serve(self) -> None:
        """Dispatch inbound messages until the connection drops."""
        assert self._ws is not None, "call start() first"
        try:
            async for raw in self._ws:
                frame = json.loads(raw)
                if frame.get("type") == "message":
                    await self._dispatch(frame)
                elif frame.get("type") == "pong":
                    continue
        except websockets.exceptions.ConnectionClosed:
            pass

    async def run(self, reconnect_delay: float = 5.0) -> None:
        """Connect and serve forever, reclaiming the code after drops."""
        # Ping loop keeps NATs/proxies from idling the socket out.
        async def _pinger():
            while True:
                await asyncio.sleep(20)
                try:
                    if self._ws is not None:
                        await self._send({"type": "ping"})
                except Exception:
                    return

        while True:
            try:
                await self.start()
                ping_task = asyncio.create_task(_pinger())
                try:
                    await self.serve()
                finally:
                    ping_task.cancel()
                log.warning("disconnected; reconnecting in %.0fs",
                            reconnect_delay)
            except RegistrationFailed:
                raise
            except Exception as exc:  # connect() failures etc.
                log.warning("connection failed (%s); retrying in %.0fs",
                            exc, reconnect_delay)
            await asyncio.sleep(reconnect_delay)

    async def _dispatch(self, frame: dict) -> None:
        from_code = frame.get("from", "")
        from_name = frame.get("from_name", "")
        body = frame.get("body", "")
        msg_id = frame.get("id")
        log.info("message from %s: %r", from_code, body[:80])
        try:
            result = self.on_message(from_code, from_name, body)
            if inspect.isawaitable(result):
                result = await result
        except Exception:
            log.exception("on_message handler failed")
            return
        if result:
            # in_reply_to carries the id of the message being answered,
            # so the other side can correlate the reply.
            await self.send(from_code, str(result), in_reply_to=msg_id)

    async def send(self, to_code: str, body: str,
                   in_reply_to: str | None = None) -> str:
        """Send a message. Returns the message id."""
        msg_id = new_id()
        await self._send({"type": "send", "to": to_code, "body": body,
                          "id": msg_id, "in_reply_to": in_reply_to})
        return msg_id

    async def _send(self, frame: dict) -> None:
        await self._ws.send(json.dumps(frame))

    async def close(self) -> None:
        if self._ws is not None:
            try:
                await self._ws.close()
            except Exception:
                pass
            self._ws = None


def _demo_handler(from_code: str, from_name: str, body: str) -> str:
    return f"Hello {from_name or from_code}! You said: {body}"


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(
        description="Run an AgentLine agent with a permanent code.")
    ap.add_argument("--code", default=None,
                    help="permanent code to claim (server assigns one if omitted)")
    ap.add_argument("--name", default="agent", help="display name")
    ap.add_argument("--relay", default="ws://127.0.0.1:8765")
    ap.add_argument("--state-dir", default=DEFAULT_STATE_DIR)
    ap.add_argument("--echo", action="store_true",
                    help="use the built-in echo responder")
    args = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")

    async def _run() -> None:
        agent = Agent(code=args.code, name=args.name,
                      on_message=_demo_handler, relay_url=args.relay,
                      state_dir=args.state_dir)
        try:
            await agent.run()
        except RegistrationFailed as exc:
            print(f"registration failed: {exc}")

    asyncio.run(_run())


if __name__ == "__main__":
    main()
