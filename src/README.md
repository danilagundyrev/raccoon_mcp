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
tool_timeout_sec = 90
```

Restart the MCP client after changing its configuration.

Keep `tool_timeout_sec` above the server's `--max-wait-seconds` value. Event waits are finite on both sides: the server defaults to a 60-second ceiling and the MCP client may still cancel sooner.

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
  "max_audit_iterations": 2,
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
  "max_audit_iterations": 2,
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
  "feature_id": "<feature-id>",
  "work_item_id": "<work-item-id>",
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

Set `max_audit_iterations` on an implementation work item to a value from 1 to 10. The default is 2: one initial audit and room for one post-remediation audit.

Audit work is part of the shared work graph. Create an `audit` work item owned by the repository that contains the code. Every scoped implementation must already be completed, belong to the same Feature, and have the same owner repository. Split cross-repository review into separate audit cards. The owner of an audit or remediation card is immutable after creation because changing it would invalidate the stored scope or lineage.

```json
{
  "instance_id": "<orchestrator-instance-id>",
  "feature_id": "<feature-id>",
  "owner_repository_id": "<repository-containing-the-code>",
  "kind": "audit",
  "title": "Audit checkout implementation",
  "summary": "Independent review against the acceptance criteria",
  "audit_scope_work_item_ids": ["<completed-work-item-id>"],
  "idempotency_key": "checkout-audit-card-1"
}
```

The owner repository claims this card normally. It then creates a fresh auditor whose `work_item_id` is the claimed audit card. The server derives the scope and iteration from that card:

```json
{
  "instance_id": "<owner-orchestrator-instance-id>",
  "role": "auditor",
  "work_item_id": "<claimed-audit-work-item-id>",
  "task_summary": "Audit checkout implementation",
  "context_refs": [],
  "idempotency_key": "checkout-auditor-1"
}
```

The auditor activates, examines its local checkout, and publishes exactly one `audit_report`:

```json
{
  "worker_run_id": "<auditor-worker-run-id>",
  "kind": "report",
  "target_repository_ids": ["<orchestrator-repository-id>"],
  "summary": "Checkout audit found two actionable issues",
  "body": "## Findings\n...",
  "artifact_kind": "audit_report",
  "artifact_title": "Checkout audit iteration 1",
  "feature_id": "<feature-id>",
  "audit_outcomes": [
    {
      "work_item_id": "<completed-work-item-id>",
      "verdict": "changes_required",
      "summary": "Validation accepts an invalid checkout state"
    }
  ]
}
```

`audit_outcomes` is required with `audit_report` and must cover every scoped implementation exactly once. Supported verdicts are `passed`, `changes_required`, and `blocked`. Without its own report and exact structured outcomes, the auditor cannot complete. A second report from the same run is rejected. After the worker completes, its parent orchestrator completes the audit work item through `update_work_item` without an `implementation_artifact_id`; only then are the verdicts applied. If an auditor attempt fails or its lease is abandoned, reclaim the same audit card and create a fresh worker run; another active attempt or any completed attempt blocks replacement.

Implementation execution and verification are separate. `verification_status` progresses through `pending_audit`, `auditing`, `remediation_required`, `remediating`, `pending_reaudit`, and finally `verified`. A non-passing final allowed audit produces `verification_blocked`.

If the report contains findings, create exactly one `remediation` work item in the same repository:

```json
{
  "instance_id": "<orchestrator-instance-id>",
  "feature_id": "<feature-id>",
  "owner_repository_id": "<repository-containing-the-code>",
  "kind": "remediation",
  "title": "Address checkout audit findings",
  "summary": "Implement the actionable audit findings",
  "addresses_audit_run_id": "<completed-auditor-run-id>",
  "idempotency_key": "checkout-remediation-card-1"
}
```

Claim that card, then create its executor with both `work_item_id` and the same `addresses_audit_run_id`. The executor publishes an `implementation_report`; its orchestrator completes the remediation card with that artifact. A failed or abandoned executor can likewise be retried on the same reclaimed remediation card, but attempts cannot overlap and a completed attempt is final.

The next audit card is allowed only after the prior audit card and any remediation card complete. Remediation completion produces `pending_reaudit`, never `verified`; another independent passing auditor is required. A `blocked` outcome with remaining depth can proceed directly to a later audit after its external blocker is resolved. Remediation cannot be created retroactively after a later audit iteration starts.

If the last configured audit is non-passing, the implementation becomes `verification_blocked`. Do not silently extend the loop. After explicit user authorization, the owner repository may call `increase_audit_depth` with the current WorkItem revision, a larger value, and a durable reason. The mutation emits `audit_depth_increased` and reopens the correct next state.

## Complete or cancel a feature

The repository that created a feature is its coordinator. After durable work state is verified, that repository closes the feature using its current revision:

```json
{
  "instance_id": "<coordinator-instance-id>",
  "feature_id": "<feature-id>",
  "expected_revision": 1
}
```

`complete_feature` rejects unfinished work items, open linked repository requests, active linked executors, auditors, and remediation runs. It also rejects every accepted implementation report whose WorkItem is not `verified` by an independent passing audit. `cancel_feature` rejects active claims, workers, and requests and atomically cancels remaining unclaimed `planned`, `ready`, or `failed` items; cancellation never changes verification state into `verified`. Both operations increment the feature revision and emit a terminal project-wide event.

Peer repositories stop their wait loop when they receive `feature_completed` or `feature_cancelled` for the current feature. A repository that joins later reads the durable terminal status through `get_feature` or `list_features`; events are wake-up hints, not the source of truth. Every orchestrator calls `disconnect_orchestrator` before it exits. MCP session DELETE is transport cleanup, not the shared completion signal.

## Events and cursors

`wait_for_events` is the primary waiting mechanism. It returns only events addressed to the actor's repository.

```json
{
  "instance_id": "<instance-id>",
  "cursor": 0,
  "kinds": ["message_available", "request_available", "work_item_updated"],
  "limit": 20,
  "timeout_seconds": 30
}
```

Rules:

- `cursor` is the last processed event ID; use `0` initially;
- persist `next_cursor` after every response;
- `timeout_seconds: 0` performs a non-blocking check;
- omitting `timeout_seconds` uses the configured `--max-wait-seconds` ceiling;
- a value above that ceiling is clamped and reported in `effective_timeout_seconds`;
- `outcome: "deadline"` is a normal empty result, not completion or an error;
- each response reports `requested_timeout_seconds`, `effective_timeout_seconds`, and `elapsed_ms`;
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
| `complete_feature` | Complete a coordinator-owned feature after linked state is terminal. |
| `cancel_feature` | Cancel a coordinator-owned feature and remaining unclaimed work. |
| `create_work_item` | Create an implementation, audit, or remediation card with repository ownership and lineage. |
| `get_work_item` | Read a consistent item snapshot with dependencies. |
| `list_work_items` | Read a bounded page of feature items. |
| `update_work_item` | Update an item using `expected_revision`. |
| `increase_audit_depth` | Explicitly reopen `verification_blocked` work with a larger depth, current revision, and reason. |
| `claim_work_item` | Atomically claim a ready owner-repository item. |
| `renew_work_item_claim` | Renew an owned, unexpired claim. |
| `release_work_item_claim` | Return an owned claim to `ready`. |

### Worker runs

| Tool | Purpose |
| --- | --- |
| `create_worker_run` | Create a role-matched worker for a claimed card, or a request-scoped butler. |
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
| `wait_for_events` | Wait for repository events within the configured finite ceiling. |

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
| `--max-wait-seconds` | `60` | Hard ceiling for one event wait, 1–65535 seconds. |
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
| Explicit `wait_for_events.timeout_seconds` | 0–configured `--max-wait-seconds` in the published schema |
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

An `outcome: "deadline"` response is normal: it means no matching event arrived during this finite call. Repeat with the unchanged `next_cursor` only while the feature is still active.

If the MCP client cancels earlier than intended, keep its `tool_timeout_sec` above `--max-wait-seconds`. Omission uses the server ceiling; it never creates an unbounded wait.

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


## Recommended Codex skill

Keep the repository-level `AGENTS.md` routing rule small and move the operational workflow into a reusable Codex skill. This keeps the workflow out of every session unless MCP coordination is explicitly requested.

Install the skill as `%USERPROFILE%\.codex\skills\raccoon-mcp-collaboration\SKILL.md` on Windows, or under `$CODEX_HOME/skills/raccoon-mcp-collaboration/SKILL.md` when `CODEX_HOME` is configured.

### Repository `AGENTS.md` block

Replace the placeholders for each participating repository. Use the same project key and a distinct stable repository key.

```xml
<mcp_collaboration_mode>
Use `raccoon-mcp` only when the user explicitly requests MCP coordination,
cross-repository collaboration, tracked worker delegation, or continuation of an
existing coordinated task. Otherwise work only inside this repository.

When coordination is requested, use `$raccoon-mcp-collaboration` with:
- project key: `<PROJECT_KEY>`;
- repository key: `<REPOSITORY_KEY>`;
- repository display name: `<REPOSITORY_DISPLAY_NAME>`;
- repository path: the canonical absolute path of the current worktree;
- repository scope: `<REPOSITORY_SCOPE>`;
- known peers: `<OPTIONAL_PEER_KEYS_AND_SCOPES>`.

The user's latest explicit instructions override this configuration.
</mcp_collaboration_mode>
```

### `SKILL.md`

Create `raccoon-mcp-collaboration/SKILL.md` with the following content:

```markdown
---
name: raccoon-mcp-collaboration
description: Coordinate durable multi-repository development through the raccoon-mcp server. Use only when the user explicitly requests MCP coordination, cross-repository collaboration, tracked executor/butler/auditor delegation, or continuation of an existing coordinated task. Covers orchestrator registration, feature and work-item state, worker runs, repository requests, messages, event waiting, leases, and bounded audit/remediation.
---

# Raccoon MCP Collaboration

Read repository-specific project, repository, path, scope, and peer configuration from
the active repository's `AGENTS.md`. Follow the user's latest explicit override.

## Workflow

1. Establish repository identity before doing coordinated work.
   - First call `connect_orchestrator` with the configured project and repository keys,
     the current absolute repository path, and useful capability metadata.
   - For a fresh process, omit `instance_id`. Reuse a previously returned instance ID
     only when intentionally reconnecting that same orchestrator instance.
   - Retain `project_id`, `repository_id`, `instance_id`, peer repository IDs, and the
     bootstrap counters returned by the server.
   - Read `hub://guide` after connecting, especially when the server contract may have
     changed since this file was written.

2. Recover durable state before creating new coordination objects.
   - Use `list_features` and bounded `list_work_items` pages to find existing work.
   - Do not duplicate an existing feature, work item, worker run, request, or report just
     because this is a new chat session.
   - Use stable actor-scoped `idempotency_key` values for retryable creation calls.
   - Check pending inbox headers with `list_inbox`; use `read_message` and `read_artifact`
     only when the content is needed, then acknowledge processed deliveries with
     `ack_message`.
   - Resume event consumption from the last retained `next_cursor` when it is available.
     If no cursor was retained, start from `0` and filter for relevant event kinds.

3. Treat server work cards as the shared execution backlog.
   - Represent the overall user-visible objective with `create_feature` and executable
     units with `create_work_item`, using explicit owner repositories and dependency IDs.
   - A work item is the normal unit of ownership and execution; do not invent a second
     hidden queue outside the server unless the user explicitly designates a local plan.
   - Respect dependency ordering. `planned` items are not executable until the server
     moves them to `ready`.
   - Before every `update_work_item` or claim mutation, obtain the current card and pass
     its current `expected_revision`. On `revision_conflict`, reread, reapply the intended
     change to current state, and retry only if the change is still valid.
   - Claims are ownership locks, not completion markers. Use `claim_work_item` only for
     ready work owned by this repository, and do not mutate another orchestrator's live
     claimed item.

4. Use tracked worker runs for delegated child-agent work.
   - Before spawning a child agent, claim the owner work item when the run implements a
     specific item, then call `create_worker_run` with role `executor` and that
     `work_item_id`.
   - `create_worker_run` records the run but does not spawn a process. Spawn the child
     through the available agent runtime only after the server returns `worker_run_id`.
   - Pass the child its `worker_run_id`, task, acceptance criteria, relevant feature and
     work-item IDs, and compact `context_refs`.
   - A child worker must never call `connect_orchestrator` or register itself as a new
     repository. Its first coordination call is `activate_worker_run`.
   - The child should call `heartbeat_worker_run` at natural checkpoints when it is not
     using another lease-renewing operation.
   - Before a linked executor completes, it must publish an `implementation_report`
     artifact tied to the correct feature and work item, then call `complete_worker_run`.
   - The parent rereads the work item and completes it through `update_work_item` with the
     returned implementation artifact ID and current revision. A worker terminal state
     alone does not finish the work item.

5. Use durable requests for cross-repository questions.
   - When progress depends on information from another repository, call `ask_repository`
     so the question has an explicit target, state, owner, and answer.
   - Include a compact question summary, full body when useful, and `context_refs` that
     let the receiver identify the feature and work involved.
   - The target repository may answer through its orchestrator or create a `butler`
     worker run. The answering actor uses `read_request`, `claim_request`, and then
     `answer_request`, `release_request_claim`, or `fail_request`.
   - A blocked executor may remain active in `wait_for_events` and continue after
     `request_answered`. Alternatively it may report the blocked state to its parent and
     let the parent start a continuation worker after the answer arrives.

6. Use messages and artifacts for durable reports, not as an implicit task queue.
   - Use `publish_message` for reports, announcements, commands, API contracts, notes,
     and replies that should be delivered to repository inboxes.
   - Prefer typed `feature_id`, `work_item_id`, and `request_id` links. Use `thread_id`,
     `in_reply_to`, and `correlation_id` when a multi-message conversation needs them.
   - Keep message summaries compact and put detailed content in the artifact body.
   - Every substantive implementation report should state the current feature, work
     item, worker run, status, completed changes, verification, blockers, and expected
     next owner repository.

7. Consume repository events.
   - Call `wait_for_events` with the actor ID, the last `next_cursor`, relevant event
     filters, and a bounded page limit.
   - Omit `timeout_seconds` to use the server's configured finite ceiling. Values above
     that ceiling are clamped; zero performs a non-blocking check.
   - An `outcome: "deadline"` response is a non-event. While the shared task is still
     active and waiting is the correct action, call `wait_for_events` again with the same
     returned cursor.
   - Events are pointers, not payloads. Read the referenced message, request, worker run,
     feature, or work item through its dedicated tool.
   - Persist or clearly report the newest processed cursor before ending a session that
     expects later continuation.

8. Apply bounded audit/remediation to every completed implementation.
   - Create a repository-owned `audit` work item whose `audit_scope_work_item_ids`
     contains only completed implementation cards from that same repository. Split a
     cross-repository review into one card per owner.
   - The owner repository claims the audit card, creates a fresh role `auditor` worker
     linked by `work_item_id`, and requires exactly one `audit_report` with one structured
     `audit_outcomes` verdict per scoped item before completing both the worker and audit card.
   - If the report contains actionable findings, create at most one `remediation` card
     linked by `addresses_audit_run_id`; claim it and create a matching executor with
     both the card and audit-run links.
   - Treat remediation completion as `pending_reaudit`, not verified. Start a later audit
     card only after the prior audit card and any remediation card are completed.
   - Never exceed `max_audit_iterations` automatically. If verification becomes blocked,
     ask the user before calling `increase_audit_depth` with a reason and current revision.
   - Do not create an endless review loop for minor preferences. Stop when the configured
     depth is exhausted or remaining findings do not justify another implementation run.

9. Continue eligible repository-owned work.
   - After completing the current item, inspect the feature backlog. If another ready,
     in-scope item is clearly owned by this repository and remains part of the user's
     requested objective, call `claim_work_item` and execute it unless the user constrained
     the scope.
   - When no eligible item is ready, wait for relevant dependency/events or report the
     concrete human/runtime input required for progress.
   - Multiple orchestrators may exist in one project. Claims and revisions, not an
     assumption of a singleton orchestrator, determine mutation ownership.

10. Finish coordination from durable state.
    - Treat the requested shared task as complete only when its relevant work items are
      in the intended terminal states, required implementation reports are linked,
      every accepted implementation report is `verified`, required audits/remediation are
      finished, and no request necessary for completion remains unresolved.
    - Use work-item states, worker-run states, requests, and linked artifacts as the
      completion source of truth.
    - If this repository coordinates the feature, reread it and call `complete_feature`
      with its current revision only after those checks pass. If the feature is being
      abandoned, use `cancel_feature` only after active work and requests are resolved.
    - If this repository is not the coordinator, stop waiting when `feature_completed`
      or `feature_cancelled` names the current feature. A deadline alone is not a
      completion signal.
    - When this orchestrator is intentionally done and will not continue waiting, call
      `disconnect_orchestrator`. Do not disconnect merely because a child worker is active
      or a short client-side turn is ending.
    - MCP session DELETE is transport cleanup and does not replace the durable feature
      terminal state or `disconnect_orchestrator`.

Operational rules:
- Do not read or write the SQLite database directly; use MCP tools.
- Do not invent IDs, reuse another actor's ID, or pass both `instance_id` and
  `worker_run_id` to an actor-scoped tool.
- Do not treat read-only calls as heartbeats. Use `heartbeat` for the orchestrator,
  `heartbeat_worker_run` for a worker, or an active lease-renewing operation during long
  work.
- Do not attempt to revive an expired/abandoned worker run; create a replacement run and
  preserve the causal link in its task context/report.
- Actor-sensitive inbox, message, artifact, and request data must be read through tools,
  not guessed from resources or another repository's output.
- If the coordination server is unavailable or returns a state conflict that cannot be
  safely resolved, report the blocker with the relevant IDs and error instead of
  fabricating successful coordination.

```

The skill needs no scripts or bundled assets. `hub://guide` provides the live server-specific reference, while `SKILL.md` contains the stable operating procedure.
