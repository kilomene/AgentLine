"""AgentLine command line: relay | agent | dial."""

from __future__ import annotations

import sys

from . import agent as _agent
from . import dial as _dial
from . import relay as _relay


def main(argv: list[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    if not argv or argv[0] in ("-h", "--help"):
        print("usage: agentline <relay|agent|dial> [options]")
        print()
        print("  relay   run the message relay server")
        print("  agent   run an agent with a permanent code")
        print("  dial    send one message to an agent by code")
        print()
        print("examples:")
        print("  agentline relay --port 8765")
        print("  agentline agent --code PHOENIX --name Phoenix --echo")
        print('  agentline dial PHOENIX "hello there"')
        return 0
    cmd, rest = argv[0], argv[1:]
    if cmd == "relay":
        _relay.main(rest)
        return 0
    if cmd == "agent":
        _agent.main(rest)
        return 0
    if cmd == "dial":
        return _dial.main(rest)
    print(f"unknown command {cmd!r}; see `agentline --help`", file=sys.stderr)
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
