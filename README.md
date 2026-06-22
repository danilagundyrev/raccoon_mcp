# Agent Sync MCP Server

Minimal local MCP server for exchanging handoff notes between two Codex sessions running in different repositories.

## What it does

- keeps a shared persisted message log on disk
- lets each agent register itself as `client-agent`, `backend-agent`, or any other name
- lets one agent send the full handoff file content to the other
- provides a blocking `pull_updates(wait=true)` call for the "wait for peer reply" loop
- exposes `mark_task_complete` so the loop can stop cleanly

This implementation is intentionally small:

- no database
- no Django integration
- no SSE stream
- compact Rust HTTP binary with JSON state on disk

Instead of SSE, the waiting behavior is implemented as a long-running MCP tool call. That is simpler and works well for Codex, but it means your MCP `tool_timeout_sec` must be set high enough.

## Discovery model

New Codex sessions should not need this README to understand the server.

The MCP server now exposes:

- `initialize` instructions
- built-in role profiles for `backend-agent` and `client-agent`
- tool schemas including `bootstrap_agent_session`
- `sync://guide`
- `sync://profiles`
- `sync://profile/{agent_name}`

So the intended session bootstrap is:

1. user tells the agent its role, for example `backend-agent`
2. agent calls `bootstrap_agent_session`
3. agent follows the returned `next_actions`

## Start the server

From this repository root:

```powershell
cargo run -- --host 127.0.0.1 --port 8765
```

Optional flags:

```powershell
cargo run -- --help
```

Build a reusable executable:

```powershell
cargo build --release
.\target\release\agent-sync-mcp.exe --host 127.0.0.1 --port 8765
```

The previous Python implementation remains in `server.py` and `store.py`, but the Rust port is the current root implementation.

Default persisted state files:

```text
tools/agent_sync_mcp/.local/state_<session-id>.json
```

If `--session-id` is omitted, the server generates a session id. Use `--state-file` for an explicit state path.

Health check:

```text
GET http://127.0.0.1:8765/healthz
```

MCP endpoint:

```text
POST http://127.0.0.1:8765/mcp
```

## Codex config

Add the same MCP server entry in both repositories:

```toml
[mcp_servers.agent_sync]
url = "http://127.0.0.1:8765/mcp"
startup_timeout_sec = 10
tool_timeout_sec = 86400
```

`tool_timeout_sec` matters because `pull_updates(wait=true)` can block for a long time.

## Suggested agent loop

### Built-in roles

The server already includes ready-to-use defaults for:

- `backend-agent`
  - `peer_name = client-agent`
  - `handoff_path = SYNC_BACKEND_AGENT_HANDOFF.md`
- `client-agent`
  - `peer_name = backend-agent`
  - `handoff_path = SYNC_CLIENT_AGENT_HANDOFF.md`

These handoff paths are relative to the current repo root of the running agent session.

### Optional profile override

If you later want to change a built-in default without editing code, use `configure_agent_profile`.

That stores a persistent override in the server state file and takes precedence over the built-in defaults.

### Minimal session prompt

With the built-in roles, a new session can be started immediately with a short instruction such as:

```text
Use MCP server agent_sync. Your role is backend-agent.
```

The session should then call `bootstrap_agent_session` and continue from the returned config.

At session start:

1. Call `bootstrap_agent_session`.
2. Call `pull_updates` with `wait=false`.
3. If a `handoff` event is returned, write it into the local handoff file and continue work.
4. If `none` is returned, continue work from the repo state and shared implementation plan.

After each work iteration:

1. Write the local handoff file.
2. Call `send_handoff` with the handoff file content.
3. Call `pull_updates` with `wait=true`.

When both sides are done:

1. Call `mark_task_complete`.
2. Stop waiting for new work.

## Available tools

- `bootstrap_agent_session`
- `register_agent`
- `send_handoff`
- `pull_updates`
- `ack_handoff`
- `mark_task_complete`
- `configure_agent_profile` for optional overrides

## Available resources

- `sync://guide`
- `sync://profiles`
- `sync://profile/{agent_name}`
- `sync://status`
- `sync://history`
- `sync://agent/{agent_name}/mailbox`
- `sync://agent/{agent_name}/history`

## Important limitation

`pull_updates` does not acknowledge a handoff. A returned handoff remains pending in the recipient mailbox until `ack_handoff` succeeds.

That means the receiving agent must write `message.content` into its local handoff file first, then call `ack_handoff` with `message.message_id`.
