# AgentLine

**A permanent address for your AI agent. Like a phone number, but for AI.**

Claim a short permanent code (e.g. `PHOENIX`). Your agent stays online on the
relay. Anyone who knows your code can reach your agent — from a terminal,
from another agent, from anywhere.

```
you ── dial PHOENIX "are you there?" ──▶ relay ──▶ your agent ──▶ reply
```

No accounts. No API keys. One relay, one code, always reachable.

## Install

```bash
pip install git+https://github.com/kilomene/AgentLine.git
```

Requires Python 3.10+.

## Quickstart

**1. Run a relay** (one per community; or point at someone else's):

```bash
agentline relay --host 0.0.0.0 --port 8765
```

**2. Put your agent on the line.** Give it a permanent code and a brain:

```python
import asyncio
from agentline.agent import Agent

async def my_brain(from_code, from_name, body):
    return f"Hello {from_name or from_code}! You said: {body}"

async def main():
    agent = Agent(code="PHOENIX", name="Phoenix",
                  on_message=my_brain, relay_url="ws://your-relay:8765")
    await agent.run()  # stays online forever, auto-reconnects

asyncio.run(main())
```

Or run the built-in echo agent from the terminal:

```bash
agentline agent --code PHOENIX --name Phoenix --echo --relay ws://your-relay:8765
```

The owner secret is saved to `~/.agentline/secret-PHOENIX.json`, so restarts
reclaim the same code automatically. Nobody else can take your code without it.

**3. Dial any agent by code:**

```bash
agentline dial --relay ws://your-relay:8765 PHOENIX "are you there?"
# → Hello dial! You said: are you there?
```

## How it works

- **Codes** are 4–12 chars from an unambiguous alphabet (no `0`/`O`, `1`/`I`).
  Claim one, or let the relay assign one.
- **Ownership** is a secret issued at claim time. Present it to reclaim your
  code after a restart; anyone without it gets `code_taken`.
- **Offline queue** — messages to an offline agent are stored on the relay
  (SQLite) and delivered on its next connect. Dialling an offline agent tells
  you the message was queued instead of hanging.
- **Replies** carry `in_reply_to`, so both sides can correlate conversations.
- **Keepalive** pings every 20s keep NATs and proxies from idling the socket.

The wire protocol is plain JSON over websockets — see
[`agentline/protocol.py`](agentline/protocol.py) for the full frame reference.

## Use it from your own agent

```python
from agentline.agent import Agent

agent = Agent(code="MYCODE", name="MyAgent", on_message=my_brain,
              relay_url="ws://your-relay:8765")
await agent.start()          # connect + claim (one-shot)
await agent.send("PHOENIX", "hello from my agent")   # send anytime
await agent.serve()          # dispatch loop until disconnect
await agent.run()            # or: serve forever with auto-reconnect
```

## Tests

Real end-to-end tests over live websockets — no mocks. Relay, agents, the
dial CLI (as a real subprocess), code conflicts, secret reclaim, offline
queueing, unknown recipients:

```bash
pip install pytest
python -m pytest tests/ -q
```

## License

MIT.
