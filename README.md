# Raccoon MCP

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](/LICENSE)
[![Buy Me a Coffee](https://img.shields.io/badge/Buy_Me_a_Coffee-support-FFDD00?style=flat&logo=buy-me-a-coffee&logoColor=000000)](https://buymeacoffee.com/dangunn)

Raccoon MCP is a local MCP server for coordinating multiple AI agents working on one feature across different repositories.

It persists shared coordination state in SQLite and provides MCP tools for repository registration, work planning, worker-run tracking, durable cross-repository questions, messages and artifacts, event waiting, and bounded audit/remediation cycles.


## Contents

- [Mental model](#mental-model)
- [Quick start](#quick-start)
- [Connect an MCP client](#connect-an-mcp-client)
- [Use the server](#use-the-server)
- [Architecture and protocol documentation](#architecture-and-protocol-documentation)
- [Server configuration](#server-configuration)
- [Request tracing](#request-tracing)
- [Limits and security](#limits-and-security)
- [Troubleshooting](#troubleshooting)
- [Recommended Codex skill](#recommended-codex-skill)

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

## Use the server

Once the server, MCP client, repository routing block, and collaboration skill are configured, ask the agent in one participating repository to coordinate the task through Raccoon MCP. For example:

> Use `$raccoon-mcp-collaboration` to coordinate this feature with the backend, Android, and web repositories. Act as the primary orchestrator, create the shared plan, delegate repository-owned work, and keep the feature open until the required independent audits pass.

Start or open an agent session in every repository that should participate. The agents register their repositories, discover shared work, spawn tracked child agents, exchange durable reports, and wait for relevant events through the MCP tools. You remain responsible for the feature request, product decisions, and reviewing the resulting code.

At a high level, one coordinated task follows this lifecycle:

```text
Feature
  -> repository-owned WorkItems
  -> fresh Executor runs and implementation reports
  -> independent Auditor runs
  -> optional remediation and re-audit
  -> completed or cancelled Feature
```

Repositories are durable identities. Orchestrator instances and child worker runs are leased execution sessions. If an orchestrator disappears, another instance in the same repository can recover durable work after ownership leases expire; it does not need to reuse the previous `instance_id`.

## Architecture and protocol documentation

Human installation, configuration, and troubleshooting stay in this README. Detailed entity rules, JSON examples, tool-by-tool workflows, schemas, event semantics, leases, and recovery behavior live in [Architecture and Protocol Reference](/docs/ARCHITECTURE.md).

Agents should normally follow the collaboration skill below together with the live MCP tool schemas and `hub://guide`. The architecture document is primarily for maintainers, skill authors, debugging, and protocol review; users do not need to manually reproduce its calls.

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
| `--log-level` | `INFO` | Application level: `DEBUG`, `INFO`, `WARNING`, or `ERROR`. Dependencies stay at `WARN` (`ERROR` when selected), so `DEBUG` does not print SQL statements. |
| `--allow-host` | — | Additional Origin hostname; may be repeated. |

A practical local profile is usually simple:

```powershell
cargo run --release -- `
  --database .raccoon-mcp/hub.sqlite3 `
  --max-summary-bytes 4096 `
  --log-level INFO
```

## Request tracing

Every parsed JSON-RPC call produces one `MCP request completed` event inside an `mcp_request` tracing span. At the default `INFO` level, that single searchable story includes:

- `session_id`, JSON-RPC `rpc_request_id`, method, and tool name;
- raw actor and linked-entity IDs supplied by the client;
- `coordination_request_id`, kept separate from the JSON-RPC request ID;
- scalar ownership, lineage, thread/reply, cancellation, resource, and page-cursor links;
- counts for dependency, audit-scope, and recipient arrays instead of dumping those arrays;
- resolved `project_id` and `repository_id` after actor authorization;
- total `duration_ms`, outcome, and a stable error code when the call failed.

`wait_for_events` additionally records requested/effective timeout, resolved starting and ending cursor, event count, elapsed time, and `events`, `deadline`, `cancelled`, or `error` as its exit reason. Actor-resolution details remain available at `DEBUG` without producing a second misleading INFO request line.

Zero-change lease-maintenance passes are also logged only at `DEBUG`. `INFO` maintenance entries mean that a session or actor was actually reaped, a worker was abandoned, or a request was requeued.

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
| History/discovery list page | 50 |
| Audit depth per item | 1–10 |
| Explicit `wait_for_events.timeout_seconds` | 0–configured `--max-wait-seconds` in the published schema |
| `idempotency_key` | 1–128 bytes |
| `correlation_id` | up to 128 bytes |
| `consumer_key` | 1–64 bytes; ASCII letters, digits, `.`, `_`, and `-` |

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

1. the item is `in_progress`, or `claimed` after recovery of an already completed worker report;
2. its claim is currently owned by the calling orchestrator;
3. the linked executor completed;
4. `expected_revision` is current;
5. `implementation_artifact_id` belongs to the latest completed executor for that repository and item. The executor's original parent instance need not remain online: after claim expiry, another orchestrator in the same repository can take over and accept the durable report.


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
   - Retain `project_id`, `repository_id`, `instance_id`, peer repository IDs,
     `snapshot_event_cursor`, and the bootstrap counters returned by the server.
   - Read `hub://guide` after connecting, especially when the server contract may have
     changed since this file was written.

2. Recover durable state before creating new coordination objects.
   - Use `list_features` and bounded `list_work_items` pages to find existing work.
   - Use `list_worker_runs`, `list_requests`, `list_deliveries`, `list_messages`, and
     metadata-only `list_artifacts` pages to rebuild execution and communication state
     without remembered entity IDs.
   - Do not duplicate an existing feature, work item, worker run, request, or report just
     because this is a new chat session.
   - Use stable actor-scoped `idempotency_key` values for retryable creation calls.
   - Check pending inbox headers with `list_inbox`; use `list_deliveries` when acknowledged
     history matters. Call `read_message` and `read_artifact` only when content is needed,
     then acknowledge processed deliveries with `ack_message`.
   - Recover accepted implementation/audit reports and explicitly Feature/WorkItem-linked
     non-request artifacts across the project when needed. Keep unlinked repository content
     private, and treat request question/answer artifacts as participant-private.
   - After reading the recovery pages, begin event consumption strictly after the
     connection's `snapshot_event_cursor` so mutations committed during recovery are reconciled.
   - For durable progress, choose an orchestrator-specific `consumer_key`, read it with
     `get_event_checkpoint`, and advance it with `commit_event_cursor` only after processing.

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
   - Scope every run exactly once: executor/auditor use only `work_item_id`; butler uses
     only `request_id`. Never submit both links or neither link.
   - Before spawning a child agent, claim the owner work item when the run implements a
     specific item, then call `create_worker_run` with role `executor` and that
     `work_item_id`.
   - Spawn one fresh executor child context for each independent implementation WorkItem.
     Never reuse that child for an unrelated WorkItem.
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
   - Worker attempts for one WorkItem are sequential. A `created`, `active`, or `completed`
     attempt blocks another run; retry only after `failed` or `abandoned`, after reopening
     and reclaiming the card.
   - Never cancel an implementation WorkItem after a linked executor publishes an
     `implementation_report`; the server requires normal completion and independent audit.
   - The same executor child may receive a focused remediation turn for its own WorkItem
     lineage when its context remains healthy, but create a new remediation `worker_run_id`
     linked through `addresses_audit_run_id`. Spawn a fresh remediation child when the old
     context is unavailable, saturated, repeatedly confused, or anchored to a rejected
     approach.

5. Use durable requests for cross-repository questions.
   - When progress depends on information from another repository, call `ask_repository`
     so the question has an explicit target, state, owner, and answer.
   - Include a compact question summary, full body when useful, and `context_refs` that
     let the receiver identify the feature and work involved.
   - Set `handler_requirement: orchestrator_allowed` only when the target can answer from
     coordination state, ownership, scope, or an existing report already in hand. Set
     `butler_required` whenever the answer requires current code inspection, exact contract
     verification, multi-file tracing, commands, or tests.
   - For `butler_required`, create and spawn a fresh role `butler` worker linked with that
     exact `request_id`. It may claim and answer only that request and must not be reused
     for unrelated questions. Give it the narrow request context and let it inspect the
     authoritative checkout itself.
   - Use `answer_requirement: artifact_required` when a non-empty answer body is mandatory;
     otherwise `summary_allowed` is sufficient. The returned request provenance identifies
     whether an orchestrator or butler produced the answer.
   - While an outgoing request is unresolved, include `request_answered` and
     `request_failed` in `wait_for_events` filters. Read the durable request after either
     event instead of treating the event header as the answer payload.
   - The permitted answering actor uses `read_request`, `claim_request`, and then
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
   - Call `wait_for_events` with the actor ID, relevant filters, a bounded page limit, and
     either an explicit last `next_cursor` or a `consumer_key`. Explicit cursor wins.
   - Use `feature_id` and `owner_repository_id` filters when only one feature or owner
     matters. Inspect compact status, revision, and linked IDs before loading full entities.
   - Omit `timeout_seconds` to use the server's configured finite ceiling. Values above
     that ceiling are clamped; zero performs a non-blocking check.
   - An `outcome: "deadline"` response is a non-event. While the shared task is still
     active and waiting is the correct action, call `wait_for_events` again with the same
     returned cursor.
   - Events are pointers, not payloads. Read the referenced message, request, worker run,
     feature, or work item through its dedicated tool.
   - Returned pages never auto-commit. After processing, call `commit_event_cursor` when
     using a named consumer; otherwise persist or clearly report the newest cursor.

8. Apply bounded audit/remediation to every completed implementation.
   - Create a repository-owned `audit` work item whose `audit_scope_work_item_ids`
     contains only completed implementation cards from that same repository. Split a
     cross-repository review into one card per owner.
   - The owner repository claims the audit card and spawns a fresh role `auditor` child
     with no executor/remediation conversation history, linked by `work_item_id`. Give it
     only the specification, acceptance criteria, IDs, artifact references, and current
     repository state. Require exactly one `audit_report` with one structured
     `audit_outcomes` verdict per scoped item before completing both worker and audit card.
   - If the report contains actionable findings, create at most one `remediation` card
     linked by `addresses_audit_run_id`; claim it and create a matching executor with
     both the card and audit-run links.
   - Treat remediation completion as `pending_reaudit`, not verified. Start a later audit
     card only after the prior audit card and any remediation card are completed, and use
     another fresh auditor child. Orchestrator review and executor self-review are only
     supplemental and never replace the formal audit.
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
      every published implementation report is `verified`, required audits/remediation are
      finished, and no request necessary for completion remains unresolved.
    - Before Feature completion or cancellation, ensure every linked worker is terminal,
      including a request-scoped butler whose request has already been answered.
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
