# Agent Sync MCP

Raccoon MCP is a local MCP server for coordinating multiple AI agents working on one feature across different repositories.

It persists shared coordination state in SQLite and provides MCP tools for repository registration, work planning, worker-run tracking, durable cross-repository questions, messages and artifacts, event waiting, and bounded audit/remediation cycles.

`create_worker_run` does **not** start a Codex process. It creates a tracked, leased worker run. The orchestrator is responsible for spawning the child agent and passing it the returned `worker_run_id`.

## Contents

- [Mental model](#mental-model)
- [Quick start](#quick-start)
- [Connect an MCP client](#connect-an-mcp-client)
- [Connect an orchestrator](#connect-an-orchestrator)
- [The normal delivery workflow](#the-normal-delivery-workflow)
- [Cross-repository questions](#cross-repository-questions)
- [Messages and artifacts](#messages-and-artifacts)
- [Audits and remediation](#audits-and-remediation)
- [Events and cursors](#events-and-cursors)
- [Leases and heartbeats](#leases-and-heartbeats)
- [MCP resources](#mcp-resources)
- [Tool reference](#tool-reference)
- [Server configuration](#server-configuration)
- [Limits and security](#limits-and-security)
- [Troubleshooting](#troubleshooting)

## Mental model

```text
Project
├── Repository: backend
│   ├── orchestrator instance(s)
│   ├── executor / butler / auditor runs
│   └── inbox deliveries
├── Repository: frontend
│   ├── orchestrator instance(s)
│   ├── executor / butler / auditor runs
│   └── inbox deliveries
├── Features
│   └── Work items with owners, dependencies, claims and revisions
├── Durable repository requests
└── Messages, artifacts and repository events
```

| Entity | Purpose |
| --- | --- |
| `project` | The collaboration boundary. Repositories with the same `project_key` join the same project. |
| `repository` | A stable identity for a backend, frontend, mobile, infrastructure, or other codebase. |
| `orchestrator instance` | A live top-level agent in a repository. A project or repository may have more than one. |
| `feature` | A high-level shared feature card. |
| `work item` | A concrete part of a feature with an owner repository, dependencies, revision, claim, and audit policy. |
| `worker run` | A leased child-agent run in the `executor`, `butler`, or `auditor` role. A worker is not a repository. |
| `request` | A durable question directed at one repository. It can be claimed, answered, released, or failed. |
| `message` | An immutable, compact envelope delivered to one or more repository inboxes. |
| `artifact` | The full body attached to a message: an implementation report, API contract, answer, audit report, or note. |
| `event` | A lightweight notification that points to a changed object. Read the object through its dedicated tool. |

An orchestrator acts through `instance_id`; a worker acts through `worker_run_id`. Actor-scoped tools require exactly one of those IDs.

## Quick start

Requirements:

- a stable Rust toolchain;
- an MCP client that supports Streamable HTTP;
- local access to the server port.

Run the server from the project root:

```powershell
cargo run --release -- --database .raccoon-mcp/hub.sqlite3
```

The default endpoint is:

```text
http://127.0.0.1:8765/mcp
```

The database directory is created automatically. Use a separate database and port for an isolated test run:

```powershell
cargo run --release -- `
  --port 8877 `
  --database C:\temp\raccoon-mcp\hub.sqlite3 `
  --log-level DEBUG
```

Check the HTTP process and SQLite write path:

```powershell
Invoke-RestMethod http://127.0.0.1:8765/healthz
Invoke-RestMethod http://127.0.0.1:8765/readyz
```

`/healthz` only checks the HTTP process. `/readyz` opens a SQLite transaction, performs a harmless write, and rolls it back. `/metrics` is unavailable unless the server was started with `--enable-metrics`.

`Ctrl+C` performs graceful shutdown: active waits are cancelled, maintenance stops, and local leases are expired.

## Connect an MCP client

### Codex

Add the server to `%USERPROFILE%\.codex\config.toml` on Windows, or `~/.codex/config.toml` on Unix-like systems:

```toml
[mcp_servers.raccoon_mcp]
url = "http://127.0.0.1:8765/mcp"
enabled = true
tool_timeout_sec = 65535
```

Restart the MCP client after changing its configuration.

The high `tool_timeout_sec` is important for `wait_for_events`. Omitting `timeout_seconds` removes the **server-side** deadline, but the MCP client can still cancel a long tool call using its own timeout.

The MCP client handles `initialize`, `Mcp-Session-Id`, protocol-version headers, and session deletion. Normal use does not require hand-written JSON-RPC or HTTP requests.

## Connect an orchestrator

Every orchestrator starts with `connect_orchestrator`.

Backend example:

```json
{
  "project_key": "shop-platform",
  "project_display_name": "Shop Platform",
  "repository_key": "backend",
  "repository_display_name": "Backend",
  "repository_path": "C:\\work\\shop-backend",
  "capabilities": {
    "stack": "rust",
    "role": "orchestrator"
  }
}
```

Frontend example, from a different terminal:

```json
{
  "project_key": "shop-platform",
  "repository_key": "frontend",
  "repository_path": "C:\\work\\shop-frontend",
  "capabilities": {
    "stack": "typescript",
    "role": "orchestrator"
  }
}
```

The same `project_key` joins the two repositories to one project. `project_key` and `repository_key` are limited to 64 bytes and may contain ASCII letters, numbers, `.`, `_`, and `-`.

Keep the returned IDs in the agent's working context:

- `project.project_id`;
- `repository.repository_id`;
- `instance.instance_id`;
- IDs of the peer repositories;
- `active_feature_count`, `pending_inbox_count`, and `open_request_count`.

For a normal fresh process, omit `instance_id`. To reconnect a known instance, pass its previously returned ID. Never invent IDs or reuse an ID of another entity.

After connecting, read `hub://guide`. It provides machine-readable bootstrap instructions and the current server limits.

## The normal delivery workflow

### 1. Create a feature

```json
{
  "instance_id": "<backend-instance-id>",
  "title": "Checkout flow",
  "summary": "Backend API and frontend checkout UI",
  "idempotency_key": "checkout-feature-v1"
}
```

Use an `idempotency_key` for retryable creation calls. Repeating the same key, actor, and payload returns the existing result; changing the payload with the same key is rejected.

### 2. Create work items

An item without dependencies starts as `ready`:

```json
{
  "instance_id": "<backend-instance-id>",
  "feature_id": "<feature-id>",
  "owner_repository_id": "<backend-repository-id>",
  "title": "Implement checkout API",
  "summary": "Add endpoint, validation, and an API contract artifact",
  "priority": 100,
  "max_audit_iterations": 1,
  "idempotency_key": "checkout-backend-item-v1"
}
```

A dependent item starts as `planned` until every dependency is completed:

```json
{
  "instance_id": "<backend-instance-id>",
  "feature_id": "<feature-id>",
  "owner_repository_id": "<frontend-repository-id>",
  "title": "Implement checkout UI",
  "summary": "Consume the checkout API and add UI states",
  "dependency_ids": ["<backend-work-item-id>"],
  "priority": 90,
  "max_audit_iterations": 1,
  "idempotency_key": "checkout-frontend-item-v1"
}
```

When all dependencies become `completed`, the server changes the dependent item to `ready` and emits `work_item_updated`.

Use pages for a large feature:

```json
{
  "feature_id": "<feature-id>",
  "limit": 20
}
```

If `has_more` is true, pass `work_items.next_cursor` as `after_work_item_id` in the next call.

### 3. Claim the item

Only an active orchestrator in the owner repository can claim a ready item:

```json
{
  "instance_id": "<backend-instance-id>",
  "work_item_id": "<backend-work-item-id>",
  "expected_revision": 1
}
```

Use the current `revision` from `get_work_item`, `list_work_items`, or the previous mutation response. Each successful mutation increments it.

### 4. Create and spawn an executor

First, the orchestrator records the worker run:

```json
{
  "instance_id": "<backend-instance-id>",
  "role": "executor",
  "task_summary": "Implement the claimed checkout API item",
  "work_item_id": "<backend-work-item-id>",
  "context_refs": [
    {
      "type": "file",
      "path": "src/server.rs",
      "note": "Relevant HTTP routing"
    }
  ],
  "idempotency_key": "checkout-backend-executor-v1"
}
```

The linked item must have a live claim owned by the parent orchestrator. Creating the linked executor changes it from `claimed` to `in_progress`.

Then spawn the child agent through the client/orchestrator mechanism and provide at least:

- `worker_run_id`;
- the task and completion criteria;
- `work_item_id` and `feature_id`;
- an instruction to call `activate_worker_run` first;
- an instruction to publish an `implementation_report` before completing.

The server never calls a spawn API or controls terminals.

### 5. Let the executor work

The child agent's first server call is `activate_worker_run`:

```json
{
  "worker_run_id": "<worker-run-id>"
}
```

An active executor can:

- call `heartbeat_worker_run` at useful checkpoints;
- publish reports and API contracts;
- use `ask_repository` for durable questions;
- use `wait_for_events` while it needs an answer;
- read accessible messages, artifacts, and requests.

Before completion, the executor publishes its implementation report:

```json
{
  "worker_run_id": "<worker-run-id>",
  "kind": "report",
  "target_repository_ids": ["<backend-repository-id>"],
  "summary": "Checkout API implemented and tested",
  "body": "## Changes\n...\n\n## Verification\n...",
  "artifact_kind": "implementation_report",
  "artifact_title": "Checkout API implementation report",
  "feature_id": "<feature-id>",
  "work_item_id": "<backend-work-item-id>",
  "content_type": "text/markdown",
  "idempotency_key": "checkout-backend-report-v1"
}
```

Passing `body` creates the artifact. An executor linked to a work item cannot successfully call `complete_worker_run` until it has published its own `implementation_report` artifact.

Finish the worker run:

```json
{
  "worker_run_id": "<worker-run-id>",
  "result_summary": "Checkout API and tests completed"
}
```

Use `fail_worker_run` with `error_summary` on failure. A terminal worker-run status does not automatically complete or fail the work item; that decision belongs to the orchestrator.

### 6. Complete the work item

The orchestrator reads the current item and links the implementation artifact while completing it:

```json
{
  "instance_id": "<backend-instance-id>",
  "work_item_id": "<backend-work-item-id>",
  "expected_revision": 7,
  "status": "completed",
  "implementation_artifact_id": "<implementation-artifact-id>"
}
```

The server verifies that the artifact is an `implementation_report` from the completed executor currently linked to this item and its owner repository.

```text
planned -- dependencies complete --> ready
ready -- claim_work_item -------> claimed
claimed -- linked executor -----> in_progress
in_progress --------------------> completed | failed | blocked | cancelled
blocked ------------------------> in_progress | failed | cancelled
failed -------------------------> ready | cancelled
```

Owner repository, parent item, and dependencies can only be changed while the item is `planned` or `ready`.

## Cross-repository questions

Use `ask_repository`, rather than an ordinary message, for a question that must survive agent restarts.

### Sender

An executor asks another repository:

```json
{
  "worker_run_id": "<requester-worker-run-id>",
  "target_repository_id": "<target-repository-id>",
  "question_summary": "What response shape does the checkout UI need?",
  "body": "Please confirm fields, optional values, and error states.",
  "context_refs": [
    {
      "type": "work_item",
      "work_item_id": "<work-item-id>"
    }
  ],
  "idempotency_key": "checkout-response-shape-question-v1"
}
```

The worker can remain active and wait for an answer:

```json
{
  "worker_run_id": "<requester-worker-run-id>",
  "cursor": 42,
  "kinds": ["request_answered", "request_failed"],
  "limit": 20
}
```

After an event arrives, call `read_request`. If the result contains `answer_message_id`, call `read_message`; if that message has `artifact_id`, call `read_artifact`.

The worker does not have to terminate merely because it asked a question. An active `wait_for_events` renews its lease and relevant claims. A valid alternative is to let the worker return a blocked result, have the parent wait, and spawn a continuation worker after the answer arrives.

### Receiver

The target repository receives `request_available`. The orchestrator may answer directly or create a `butler` worker:

1. Create a `butler` worker run.
2. Spawn the child and give it `worker_run_id` and `request_id`.
3. Activate the worker.
4. Read the request and its optional question artifact.
5. Claim the request.
6. Inspect the local repository.
7. Answer the request.
8. Complete the worker run.

Example answer:

```json
{
  "worker_run_id": "<butler-worker-run-id>",
  "request_id": "<request-id>",
  "answer_summary": "Checkout response contract confirmed",
  "body": "```json\n{ ... }\n```",
  "content_type": "text/markdown"
}
```

`answer_request` closes the owned request and delivers the answer message atomically.

Use `release_request_claim` to return a request to `pending`. `fail_request` with `retryable: true` also returns it to the queue; with `retryable: false`, it becomes terminally `failed`. Another actor in the target repository may take an expired request claim.

## Messages and artifacts

`publish_message` creates an immutable message envelope and, when `body` is present, an immutable artifact. Delivery to every recipient occurs in the same transaction.

Message kinds:

- `report` — a result or substantive report;
- `announcement` — an informational notification;
- `command` — an instruction or action request.

Artifact kinds:

- `implementation_report`;
- `api_contract`;
- `answer`;
- `audit_report`;
- `note`.

Choose exactly one addressing mode:

```json
{
  "target_repository_ids": ["<repository-id>"]
}
```

or:

```json
{
  "broadcast": true
}
```

Use typed links `feature_id`, `work_item_id`, and `request_id` for traceability. `thread_id` identifies a conversation, `in_reply_to` identifies the message being answered, and `correlation_id` is an application-defined operation key.

When `in_reply_to` is supplied, the thread and typed links are inherited from the original message. Explicit incompatible links are rejected.

Inbox processing is deliberately two-stage:

1. `list_inbox` returns pending headers, not full artifact bodies.
2. `read_message` reads the envelope.
3. `read_artifact` loads body content only when needed.
4. `ack_message` idempotently acknowledges the delivery.

Example filtered inbox:

```json
{
  "instance_id": "<instance-id>",
  "after_delivery_id": 0,
  "kinds": ["report", "command"],
  "limit": 20
}
```

Acknowledgement closes a delivery for one repository. It does not delete the message or artifact.

## Audits and remediation

Set `max_audit_iterations` on a work item to a value from 1 to 10. The default is 1.

An auditor can only be created for completed work items. One auditor run can cover up to 50 items:

```json
{
  "instance_id": "<orchestrator-instance-id>",
  "role": "auditor",
  "task_summary": "Audit completed checkout implementation",
  "audit_work_items": [
    { "work_item_id": "<backend-work-item-id>" },
    { "work_item_id": "<frontend-work-item-id>" }
  ],
  "context_refs": [],
  "idempotency_key": "checkout-audit-1"
}
```

The auditor activates, examines the code, and publishes exactly one `audit_report`:

```json
{
  "worker_run_id": "<auditor-worker-run-id>",
  "kind": "report",
  "target_repository_ids": ["<orchestrator-repository-id>"],
  "summary": "Checkout audit found two actionable issues",
  "body": "## Findings\n...",
  "artifact_kind": "audit_report",
  "artifact_title": "Checkout audit iteration 1",
  "feature_id": "<feature-id>"
}
```

Without its own `audit_report`, the auditor cannot complete. A second audit report from the same run is rejected.

If the report contains findings, the orchestrator may create exactly one remediation executor for that audit:

```json
{
  "instance_id": "<orchestrator-instance-id>",
  "role": "executor",
  "task_summary": "Address findings from checkout audit",
  "addresses_audit_run_id": "<completed-auditor-run-id>",
  "context_refs": [
    {
      "type": "artifact",
      "artifact_id": "<audit-artifact-id>"
    }
  ],
  "idempotency_key": "checkout-audit-1-remediation"
}
```

The next audit iteration is allowed only after the prior auditor completed with exactly one report and any remediation executor also completed. Every scoped item must still have remaining audit depth. Remediation cannot be created retroactively after a later audit iteration starts.

The server does not automatically start audit iterations. The orchestrator decides whether another iteration is useful.

## Events and cursors

`wait_for_events` is the primary waiting mechanism. It returns only events addressed to the actor's repository.

```json
{
  "instance_id": "<instance-id>",
  "cursor": 0,
  "kinds": ["message_available", "request_available", "work_item_updated"],
  "limit": 20,
  "timeout_seconds": 300
}
```

Rules:

- `cursor` is the last processed event ID; use `0` initially;
- persist `next_cursor` after every response;
- `timeout_seconds: 0` performs a non-blocking check;
- an explicit timeout ranges from 0 to 65535 seconds;
- omitting `timeout_seconds` means no server-side deadline;
- `outcome: "timeout"` is a normal result, not an error;
- when `has_more` is true, fetch the next page immediately using the new cursor.

An event is a pointer, not the full object. Use `entity_type` and `entity_id` to call `read_message`, `read_request`, `get_worker_run`, `get_feature`, or `get_work_item`.

Event and inbox cursors are independent:

- `wait_for_events.cursor` uses `event_id`;
- `list_inbox.after_delivery_id` uses `delivery_id`;
- acknowledging a message does not advance the event cursor.

## Leases and heartbeats

A lease is not the maximum execution time of a task and not an MCP tool-call timeout. It is the period during which the server treats an actor or claim as live and exclusively owned.

| Lease | Default |
| --- | ---: |
| Orchestrator instance | 1800 seconds |
| Worker run | 3600 seconds |
| Request claim | 600 seconds |
| Work-item claim | 3600 seconds |

Leases are extended to a new deadline; they are not a one-time process lifetime.

Use `heartbeat` for an orchestrator and `heartbeat_worker_run` at useful checkpoints for an active worker. Actor-scoped mutations and an active `wait_for_events` also renew relevant actor and claim leases. Simple reads such as `list_inbox`, `read_message`, and `read_artifact` are not heartbeats.

When a lease expires:

- an orchestrator becomes offline during maintenance, while its repository remains registered;
- a worker becomes `abandoned` and cannot be revived or completed later;
- a request claim returns to `pending`;
- a work-item claim may be taken by another owner-repository orchestrator.

Maintenance runs every 30 seconds by default. It is required lightweight lease cleanup, not full analytics collection. Retention and metrics are disabled by default.

MCP session idle timeout is separate from leases. An in-flight request or long wait is not evicted from the session registry. If `max_sessions` is reached, a new `initialize` fails with a capacity error instead of evicting a live session.

## MCP resources

Resources expose compact non-sensitive state:

| URI | Content |
| --- | --- |
| `hub://guide` | Machine-readable workflow and server limits. |
| `hub://project/{project_id}/overview` | Project metadata and repository presence. |
| `hub://project/{project_id}/repositories` | Repositories and online orchestrator counts. |
| `hub://feature/{feature_id}` | A feature card. |
| `hub://work-item/{work_item_id}` | A work item with revision, dependencies, claims, and artifact links. |

Inbox, messages, and artifacts are intentionally not resources. They require an explicit actor identity for repository authorization, so use their actor-scoped tools.

## Tool reference

### Presence and project

| Tool | Purpose |
| --- | --- |
| `connect_orchestrator` | Register or reconnect an orchestrator. Required first business call. |
| `heartbeat` | Extend an orchestrator lease. |
| `disconnect_orchestrator` | Explicitly disconnect an instance without deleting repository identity. |
| `list_repositories` | List project repositories with `all`, `online`, or `offline` filtering. |
| `get_project_overview` | Read compact project and presence state. |

### Features and work items

| Tool | Purpose |
| --- | --- |
| `create_feature` | Create a feature. |
| `get_feature` | Read one feature. |
| `list_features` | List project features. |
| `create_work_item` | Create an item with dependencies and audit policy. |
| `get_work_item` | Read a consistent item snapshot with dependencies. |
| `list_work_items` | Read a bounded page of feature items. |
| `update_work_item` | Update an item using `expected_revision`. |
| `claim_work_item` | Atomically claim a ready owner-repository item. |
| `renew_work_item_claim` | Renew an owned, unexpired claim. |
| `release_work_item_claim` | Return an owned claim to `ready`. |

### Worker runs

| Tool | Purpose |
| --- | --- |
| `create_worker_run` | Create an executor, butler, or auditor run before spawning a child agent. |
| `activate_worker_run` | Move a created run to active. The child agent's first call. |
| `heartbeat_worker_run` | Extend an active worker lease. |
| `get_worker_run` | Read worker status, scope, and result data. |
| `complete_worker_run` | Complete an active worker. |
| `fail_worker_run` | Fail a created or active worker. |

### Requests

| Tool | Purpose |
| --- | --- |
| `ask_repository` | Create a durable question for one repository. |
| `read_request` | Read a request as its requester or target repository. |
| `claim_request` | Claim a pending request or an expired claim. |
| `renew_request_claim` | Renew an owned request claim. |
| `release_request_claim` | Return an owned request to pending. |
| `answer_request` | Close an owned request and deliver an answer artifact. |
| `fail_request` | Requeue a retryable request or terminally fail it. |

### Messages and events

| Tool | Purpose |
| --- | --- |
| `publish_message` | Publish a message and optional artifact directly or as a broadcast. |
| `list_inbox` | List pending headers with pagination and kind filtering. |
| `read_message` | Read an accessible message envelope. |
| `read_artifact` | Read accessible full artifact content. |
| `ack_message` | Idempotently acknowledge a delivery. |
| `wait_for_events` | Wait for repository events with cursor and optional timeout. |

## Server configuration

Show the authoritative CLI help:

```powershell
cargo run -- --help
```

| Flag | Default | Purpose |
| --- | ---: | --- |
| `--host` | `127.0.0.1` | Bind interface. |
| `--port` | `8765` | TCP port. |
| `--database` | `.raccoon-mcp/hub.sqlite3` | SQLite path. |
| `--max-db-connections` | `4` | Pool size, from 1 to 32. |
| `--lease-seconds` | `1800` | Orchestrator lease, 60–65535 seconds. |
| `--worker-lease-seconds` | `3600` | Worker lease, 60–65535 seconds. |
| `--request-claim-lease-seconds` | `600` | Request claim lease, 60–65535 seconds. |
| `--work-item-claim-lease-seconds` | `3600` | Work-item claim lease, 60–65535 seconds. |
| `--max-summary-bytes` | `1024` | UTF-8 limit for compact title and summary fields, 128–65535. |
| `--max-artifact-bytes` | `524288` | UTF-8 limit for one artifact body, 1024–1048576. |
| `--maintenance-interval-seconds` | `30` | Lease and session cleanup interval, 1–3600 seconds. |
| `--retention-interval-seconds` | `0` | Retention interval; `0` disables retention. |
| `--retention-seconds` | `604800` | Event/idempotency retention age when retention is enabled. |
| `--enable-metrics` | off | Expose `GET /metrics`. |
| `--session-idle-timeout-seconds` | `3600` | MCP session idle timeout, 60–65535 seconds. |
| `--max-sessions` | `128` | In-memory MCP-session limit, 1–4096. |
| `--log-level` | `INFO` | `DEBUG`, `INFO`, `WARNING`, or `ERROR`. |
| `--allow-host` | — | Additional Origin hostname; may be repeated. |

A practical local profile is usually simple:

```powershell
cargo run --release -- `
  --database .raccoon-mcp/hub.sqlite3 `
  --max-summary-bytes 4096 `
  --log-level INFO
```

## Limits and security

| Value | Limit |
| --- | ---: |
| `context_refs` | 50 objects |
| Work items in one auditor run | 50 |
| Dependencies of one work item | 100 |
| Recipients of one message | 100 |
| `list_work_items` page | 100 |
| `list_inbox` page | 100 |
| `wait_for_events` page | 50 |
| Audit depth per item | 1–10 |
| Explicit `wait_for_events.timeout_seconds` | 0–65535 seconds |
| `idempotency_key` | 1–128 bytes |
| `correlation_id` | up to 128 bytes |

`context_refs` is an array of arbitrary JSON objects. The server validates object shape and count but does not impose a custom schema. Keep references compact instead of copying large context into every call.

JSON-RPC batching is not supported. Send one MCP request at a time.

This server is intended for a trusted local environment:

- it has no user authentication;
- Origin validation is not authentication;
- artifacts may contain local source-code context and are persisted in SQLite;
- do not expose the port to an untrusted network or use `--host 0.0.0.0` without external protection.

## Troubleshooting

### The agent cannot see the tools

1. Check `GET /healthz`.
2. Check the configured URL: `http://127.0.0.1:8765/mcp`.
3. Restart the MCP client after changing its configuration.
4. Check whether another process owns the port.
5. Start the server with `--log-level DEBUG`.

### `wait_for_events` ended unexpectedly

An `outcome: "timeout"` response is normal: repeat the call with `next_cursor`.

If the MCP client cancelled the call, increase `tool_timeout_sec`. Omitting `timeout_seconds` removes only the server deadline; `65535` is the maximum explicit server timeout.

### `revision_conflict`

Another mutation changed the work item first. Call `get_work_item`, reapply the intended change to current state, and retry with the new `expected_revision`.

### `claim_conflict`

Another live actor owns the resource. Respect returned `retry_at` or `retry_after_seconds`, then read current state before retrying.

### A worker cannot complete

Use `get_worker_run`. An expired worker becomes `abandoned` and cannot be revived; create a new worker run. A work-item executor additionally needs its own `implementation_report`; an auditor needs exactly one `audit_report`.

### A work item cannot complete

Check that:

1. the item is `in_progress`;
2. its claim is still owned by the current orchestrator;
3. the linked executor completed;
4. `expected_revision` is current;
5. `implementation_artifact_id` belongs to the linked executor and item.


## Suggested orchestrator instruction

An orchestrator prompt can include this compact operating contract:

```text
First call connect_orchestrator and retain project_id, repository_id, and instance_id.
Read hub://guide. Model work using features and work items. Before spawning an executor,
claim its owner item, create a worker run, and pass worker_run_id to the child. Require
activate_worker_run, an implementation_report, and complete_worker_run. Use
expected_revision for every work-item mutation. Use ask_repository for cross-repository
questions and wait_for_events with persistent next_cursor for waiting. Complete the item
after implementation, then run the bounded max_audit_iterations audit/remediation cycle
when it is useful.
```

The server provides identity, state, routing, and durable delivery. Planning, child spawning, code changes, and judgment about audit findings remain the orchestrator's responsibility.
