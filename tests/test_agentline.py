"""End-to-end tests for AgentLine.

Everything here runs against a REAL relay on 127.0.0.1 with REAL
websocket connections. No mocks, no fakes: if these pass, the system
works.
"""

from __future__ import annotations

import asyncio
import json
import subprocess
import sys
import tempfile
from pathlib import Path

import pytest
from websockets.client import connect

from agentline.agent import Agent, RegistrationFailed
from agentline.dial import dial, DialError
from agentline.protocol import valid_code
from agentline.relay import Relay

HOST = "127.0.0.1"


@pytest.fixture()
def relay_url():
    """A live relay on an ephemeral port, torn down after the test.

    The relay runs on its own event loop in a background thread, so test
    coroutines can use plain asyncio.run() and still talk to it over real
    TCP.
    """
    import threading

    tmp = tempfile.mkdtemp(prefix="agentline-test-")
    relay = Relay(host=HOST, port=0,
                  db_path=str(Path(tmp) / "relay.db"))
    ready = threading.Event()
    box: dict = {}

    def _thread():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        box["loop"] = loop

        async def _serve():
            await relay.start()
            ready.set()
            await asyncio.Future()  # run until the process exits

        try:
            loop.run_until_complete(_serve())
        except (RuntimeError, asyncio.CancelledError):
            pass

    t = threading.Thread(target=_thread, daemon=True)
    t.start()
    assert ready.wait(timeout=10), "relay did not start"
    yield f"ws://{HOST}:{relay.port}"
    fut = asyncio.run_coroutine_threadsafe(relay.stop(), box["loop"])
    fut.result(timeout=10)


@pytest.fixture()
def state_dir():
    with tempfile.TemporaryDirectory(prefix="agentline-agent-") as tmp:
        yield tmp


def run(coro):
    return asyncio.run(asyncio.wait_for(coro, timeout=20))


def test_code_format():
    assert valid_code("ABC234")
    assert valid_code("PHOENIX9")
    assert not valid_code("abc234")     # lowercase rejected
    assert not valid_code("AB")         # too short
    assert not valid_code("ABC-234")    # dash rejected
    assert not valid_code("HELLO0")     # 0 is ambiguous, rejected
    assert not valid_code("HELLO1")     # 1 is ambiguous, rejected


def test_claim_and_welcome(relay_url):
    async def _t():
        async with connect(relay_url) as ws:
            await ws.send(json.dumps(
                {"type": "hello", "code": "TESTER", "name": "Tester",
                 "secret": None, "ephemeral": False}))
            frame = json.loads(await ws.recv())
            assert frame["type"] == "welcome"
            assert frame["code"] == "TESTER"
            assert frame["secret"]  # owner secret issued
            assert frame["queued"] == 0
    run(_t())


def test_code_conflict_rejected(relay_url):
    async def _t():
        async with connect(relay_url) as ws1:
            await ws1.send(json.dumps(
                {"type": "hello", "code": "CLAIMED", "name": "One",
                 "secret": None, "ephemeral": False}))
            frame = json.loads(await ws1.recv())
            assert frame["type"] == "welcome"
            secret = frame["secret"]

            # Second client, same code, no secret -> rejected.
            async with connect(relay_url) as ws2:
                await ws2.send(json.dumps(
                    {"type": "hello", "code": "CLAIMED", "name": "Two",
                     "secret": None, "ephemeral": False}))
                frame2 = json.loads(await ws2.recv())
                assert frame2["type"] == "error"
                assert frame2["reason"] == "code_taken"

            # Same code WITH the secret -> reclaim succeeds.
            async with connect(relay_url) as ws3:
                await ws3.send(json.dumps(
                    {"type": "hello", "code": "CLAIMED", "name": "One",
                     "secret": secret, "ephemeral": False}))
                frame3 = json.loads(await ws3.recv())
                assert frame3["type"] == "welcome"
                assert frame3["code"] == "CLAIMED"
    run(_t())


def test_agent_registers_and_secret_persists(relay_url, state_dir):
    async def _t():
        agent = Agent(code="KEEPER", name="Keeper",
                      on_message=lambda *a: None,
                      relay_url=relay_url, state_dir=state_dir)
        code = await agent.start()
        assert code == "KEEPER"
        secret_file = Path(state_dir) / "secret-KEEPER.json"
        assert secret_file.exists()
        saved = json.loads(secret_file.read_text())["secret"]
        await agent.close()

        # A fresh Agent object reclaims the same code via the saved secret.
        agent2 = Agent(code="KEEPER", name="Keeper",
                       on_message=lambda *a: None,
                       relay_url=relay_url, state_dir=state_dir)
        code2 = await agent2.start()
        assert code2 == "KEEPER"
        await agent2.close()
        assert saved == json.loads(secret_file.read_text())["secret"]
    run(_t())


def test_end_to_end_message_and_reply(relay_url, state_dir):
    received = []

    async def _t():
        async def responder(from_code, from_name, body):
            received.append((from_code, body))
            return f"roger {body}"

        bob = Agent(code="BOB2", name="Bob", on_message=responder,
                    relay_url=relay_url, state_dir=state_dir)
        await bob.start()
        bob_task = asyncio.create_task(bob.serve())

        alice = Agent(code="ALICE2", name="Alice",
                      on_message=lambda *a: None,
                      relay_url=relay_url, state_dir=state_dir)
        await alice.start()

        replies = []
        orig_dispatch = alice._dispatch

        async def capture(frame):
            if frame.get("type") == "message":
                replies.append(frame)
            await orig_dispatch(frame)

        alice._dispatch = capture
        alice_task = asyncio.create_task(alice.serve())

        await alice.send("BOB2", "hello bob")
        # Wait for Bob's reply to arrive at Alice.
        for _ in range(100):
            if replies:
                break
            await asyncio.sleep(0.05)

        assert received == [("ALICE2", "hello bob")]
        assert len(replies) == 1
        assert replies[0]["body"] == "roger hello bob"
        assert replies[0]["from"] == "BOB2"
        assert replies[0]["in_reply_to"] is not None

        bob_task.cancel()
        alice_task.cancel()
        await alice.close()
        await bob.close()
    run(_t())


def test_offline_queue_delivered_on_reconnect(relay_url, state_dir, tmp_path):
    async def _t():
        got = []

        async def handler(from_code, from_name, body):
            got.append(body)
            return None

        # Bob registers, then goes offline.
        bob = Agent(code="BOBQ", name="Bob", on_message=handler,
                    relay_url=relay_url, state_dir=state_dir)
        await bob.start()
        await bob.close()

        # Alice sends while Bob is offline; relay must queue, not drop.
        alice = Agent(code="ALICEQ", name="Alice",
                      on_message=lambda *a: None,
                      relay_url=relay_url, state_dir=state_dir)
        await alice.start()
        await alice.send("BOBQ", "you were away")
        await asyncio.sleep(0.3)
        await alice.close()

        # Bob comes back and must receive the queued message.
        bob2 = Agent(code="BOBQ", name="Bob", on_message=handler,
                     relay_url=relay_url, state_dir=state_dir)
        await bob2.start()
        serve_task = asyncio.create_task(bob2.serve())
        for _ in range(100):
            if got:
                break
            await asyncio.sleep(0.05)
        serve_task.cancel()
        await bob2.close()
        assert got == ["you were away"]
    run(_t())


def test_unknown_recipient_errors(relay_url):
    async def _t():
        async with connect(relay_url) as ws:
            await ws.send(json.dumps(
                {"type": "hello", "code": "SENDER", "name": "S",
                 "secret": None, "ephemeral": False}))
            assert json.loads(await ws.recv())["type"] == "welcome"
            await ws.send(json.dumps(
                {"type": "send", "to": "NOBODY", "body": "hi", "id": "x1",
                 "in_reply_to": None}))
            frame = json.loads(await ws.recv())
            assert frame["type"] == "error"
            assert frame["reason"] == "unknown_recipient"
    run(_t())


def test_dial_gets_reply(relay_url, state_dir):
    async def _t():
        async def responder(from_code, from_name, body):
            return f"echo:{body}"

        agent = Agent(code="DIALME", name="DialMe", on_message=responder,
                      relay_url=relay_url, state_dir=state_dir)
        await agent.start()
        serve_task = asyncio.create_task(agent.serve())
        try:
            reply = await dial(relay_url, "DIALME", "knock knock", timeout=10)
            assert reply == "echo:knock knock"
        finally:
            serve_task.cancel()
            await agent.close()
    run(_t())


def test_dial_offline_target_fails(relay_url):
    async def _t():
        # Register the target so the code exists, then take it offline.
        async with connect(relay_url) as ws:
            await ws.send(json.dumps(
                {"type": "hello", "code": "SLEEPY", "name": "S",
                 "secret": None, "ephemeral": False}))
            assert json.loads(await ws.recv())["type"] == "welcome"
        # ws closed -> target offline; dial must report it, not hang.
        with pytest.raises(DialError):
            await dial(relay_url, "SLEEPY", "are you there", timeout=5)
    run(_t())


def test_dial_cli_subprocess(relay_url, state_dir):
    """The real `agentline dial` CLI, as a subprocess, against a live relay."""
    async def _t():
        async def responder(from_code, from_name, body):
            return f"cli says hi to {from_name}"

        agent = Agent(code="CLIBOT", name="CliBot", on_message=responder,
                      relay_url=relay_url, state_dir=state_dir)
        await agent.start()
        serve_task = asyncio.create_task(agent.serve())
        try:
            proc = await asyncio.create_subprocess_exec(
                sys.executable, "-m", "agentline", "dial",
                "--relay", relay_url, "CLIBOT", "hello from cli",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            out, err = await asyncio.wait_for(proc.communicate(), timeout=20)
            assert proc.returncode == 0, err.decode()
            assert out.decode().strip() == "cli says hi to dial"
        finally:
            serve_task.cancel()
            await agent.close()
    run(_t())
