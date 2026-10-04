"""Dial any AgentLine agent by its permanent code and print the reply."""

from __future__ import annotations

import argparse
import asyncio
import json
import sys

import websockets.exceptions
from websockets.client import connect

from .protocol import new_id


class DialError(Exception):
    pass


async def dial(relay_url: str, target: str, body: str,
               timeout: float = 30.0, name: str = "dial") -> str | None:
    """Send one message to ``target`` and wait for its reply.

    Returns the reply body, or None if the agent stayed silent.
    Raises DialError on protocol/offline errors.
    """
    target = target.upper()
    async with connect(relay_url, max_size=2**20) as ws:
        await ws.send(json.dumps(
            {"type": "hello", "code": None, "name": name,
             "secret": None, "ephemeral": True}))
        my_code: str | None = None
        msg_id = new_id()
        sent = False
        try:
            async with asyncio.timeout(timeout):
                async for raw in ws:
                    frame = json.loads(raw)
                    ftype = frame.get("type")
                    if ftype == "welcome":
                        my_code = frame["code"]
                        await ws.send(json.dumps(
                            {"type": "send", "to": target, "body": body,
                             "id": msg_id, "in_reply_to": None}))
                        sent = True
                    elif ftype == "delivered":
                        if frame.get("queued"):
                            raise DialError(
                                f"{target} is offline; message queued for later")
                    elif ftype == "error":
                        raise DialError(
                            f"{frame.get('reason')}: {frame.get('detail', '')}")
                    elif ftype == "message":
                        if (frame.get("from") == target
                                and frame.get("in_reply_to") == msg_id):
                            return frame.get("body")
                        # Anything else addressed to our throwaway code is
                        # not the reply we are waiting for; keep listening.
        except TimeoutError:
            raise DialError(f"no reply from {target} within {timeout:.0f}s")
    return None


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        description="Dial an AgentLine agent by code and print its reply.")
    ap.add_argument("target", help="the agent's permanent code, e.g. ABC123")
    ap.add_argument("message", help="message to send")
    ap.add_argument("--relay", default="ws://127.0.0.1:8765")
    ap.add_argument("--timeout", type=float, default=30.0)
    ap.add_argument("--name", default="dial")
    args = ap.parse_args(argv)
    try:
        reply = asyncio.run(
            dial(args.relay, args.target, args.message,
                 timeout=args.timeout, name=args.name))
    except DialError as exc:
        print(f"dial failed: {exc}", file=sys.stderr)
        return 1
    if reply is None:
        print("(no reply)")
    else:
        print(reply)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
