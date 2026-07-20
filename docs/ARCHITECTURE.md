# Raccoon MCP Architecture and Protocol Reference

This document explains the coordination model and concrete MCP calls for maintainers, skill authors, and protocol debugging. It is not required for ordinary use of the server.

The live MCP tool schemas, runtime validation, and `hub://guide` resource are authoritative if this document differs from the implementation. For installation and operation, see the [server README](../src/README.md).

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
- `snapshot_event_cursor`, the event watermark captured with those bootstrap values.

For a normal fresh process, omit `instance_id`. To reconnect a known instance, pass its previously returned ID. Never invent IDs or reuse an ID of another entity.

Project and repository `updated_at` values describe stored metadata, not presence. Reconnecting or sending heartbeats with identical display names and paths leaves them unchanged. Supplying a different display name updates that entity and its timestamp. A different path for an existing repository key is not accepted silently: the original canonical path remains stored and `connect_orchestrator` returns a warning.

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

Spawn a new executor child context for every independent implementation WorkItem. Do not turn one long-lived child into a general repository worker that moves across unrelated cards. The context may be retained for remediation of that same WorkItem lineage after an independent audit, but remediation still receives a new `worker_run_id` and `addresses_audit_run_id`. Use a fresh remediation child when the original context is unavailable, saturated, repeatedly confused, or anchored to an approach the audit rejected fundamentally.

A WorkItem has sequential worker attempts. While one attempt is `created` or `active`, another cannot be created; a `completed` attempt is final. A replacement run is allowed only after the previous attempt becomes `failed` or `abandoned` and the card is reopened and claimed again.

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

After any linked executor publishes an `implementation_report`, the implementation WorkItem cannot be cancelled. The orchestrator must accept the report through normal completion and obtain independent audit coverage. This prevents cancellation from becoming a verification bypass.

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
in_progress --------------------> completed | failed | blocked | cancelled*
blocked ------------------------> in_progress | failed | cancelled*
failed -------------------------> ready | cancelled
```

`cancelled*` is unavailable for an implementation WorkItem after any linked executor has published an `implementation_report`.

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
  "handler_requirement": "butler_required",
  "answer_requirement": "artifact_required",
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

The target repository receives `request_available`. Choose the handling policy when creating the request:

- `handler_requirement: "orchestrator_allowed"` permits a direct answer when the target already has the evidence in coordination state: status, ownership, scope, or an existing report/artifact. This is the default.
- `handler_requirement: "butler_required"` requires a fresh request-scoped butler when the answer needs current source inspection, exact API/DTO verification, multi-file tracing, commands, tests, or other repository investigation. Answer length is irrelevant.
- `answer_requirement: "summary_allowed"` permits a summary-only answer. This is the default.
- `answer_requirement: "artifact_required"` makes `answer_request` reject a missing or empty `body` without changing request state.

For a `butler_required` request:

1. Create a `butler` worker run with this exact `request_id`.
2. Spawn the child and give it `worker_run_id` and `request_id`.
3. Activate the worker.
4. Read the request and its optional question artifact.
5. Claim the request.
6. Inspect the local repository.
7. Answer the request.
8. Complete the worker run.

```json
{
  "instance_id": "<target-orchestrator-instance-id>",
  "role": "butler",
  "request_id": "<request-id>",
  "task_summary": "Inspect the checkout response contract",
  "context_refs": []
}
```

The butler must be a fresh child for this directed request and cannot claim or answer a different request. A butler receives only the exact question, answer requirement, Feature/WorkItem links, and relevant artifact references; it independently discovers authoritative repository details. The returned request records `answer_provenance` plus the answering instance or worker-run ID.

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

### Artifact access

Artifact bodies always require an actor-scoped `read_artifact` call. Authorization depends on how the artifact participates in coordination:

- an artifact accepted as a WorkItem's `implementation_artifact_id`, or published with persisted audit outcomes, is readable by orchestrators in the same project; older audit iterations remain recoverable after `latest_audit_artifact_id` advances;
- adding `feature_id` or `work_item_id` to a non-request message explicitly makes its artifact project-shared for same-project orchestrators;
- an unlinked message artifact remains readable only by its producer repository and message recipients;
- request question and answer artifacts remain limited to the requester, target, and producer even when the request links a Feature or WorkItem;
- worker actors keep repository/run-scoped access and do not inherit orchestrator-wide project visibility.

`list_artifacts` applies the same rules and returns metadata only. For ordinary message, implementation, and remediation artifacts, `work_item_id` is the direct linked card. A batch audit artifact instead exposes its audit card through `audit_work_item_id` and every reviewed implementation through the deterministic `scoped_work_item_ids` array; it never collapses that scope into one arbitrary ID.

Linking a normal message to a Feature or WorkItem is therefore an authorization decision, not just a tracing label. Use no typed link for repository-private content, and use `request_id` for participant-private question/answer content.

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

The owner repository claims this card normally. It then creates a fresh auditor child with no executor or remediation conversation history whose `work_item_id` is the claimed audit card. Give it only the specification, acceptance criteria, IDs, artifact references, and current repository state. The server derives the scope and iteration from that card:

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

Claim that card, then create its executor with both `work_item_id` and the same `addresses_audit_run_id`. The executor publishes an `implementation_report`; an orchestrator holding the active repository claim completes the remediation card with that artifact. The original executor child may receive this focused same-lineage remediation turn when its context remains healthy, but the server-side remediation run is always new. Otherwise spawn a fresh remediation child. A failed or abandoned executor can likewise be retried on the same reclaimed remediation card, but attempts cannot overlap and a completed attempt is final.

A batch audit still has exactly one remediation card. Therefore every `changes_required` item in that audit must have remaining audit depth before the card can be created. If several items became `verification_blocked`, explicitly increase depth for all of them first; reopening only part of the batch is rejected and does not mutate neighbouring items.

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

`complete_feature` rejects unfinished work items, open linked repository requests, and every active linked worker, including a butler whose request has already been answered. It also rejects every published implementation report whose WorkItem is not `verified` by an independent passing audit. `cancel_feature` rejects active claims, workers, and requests and atomically cancels remaining unclaimed `planned`, `ready`, or `failed` items; cancellation never changes verification state into `verified`. Both operations increment the feature revision and emit a terminal project-wide event.

Peer repositories stop their wait loop when they receive `feature_completed` or `feature_cancelled` for the current feature. A repository that joins later reads the durable terminal status through `get_feature` or `list_features`; events are wake-up hints, not the source of truth. Every orchestrator calls `disconnect_orchestrator` before it exits. MCP session DELETE is transport cleanup, not the shared completion signal.

## Recovering durable state

A new or replacement orchestrator does not need remembered entity IDs. After connecting:

1. retain the returned `snapshot_event_cursor`;
2. page through `list_features`, optionally filtering by `active`;
3. page through `list_work_items` for relevant features;
4. use `list_worker_runs` and `list_requests` to recover execution and question state; request summaries include both handler and answer requirements, so routing does not require one `read_request` call per row;
5. use `list_deliveries` for mailbox history and `list_messages` for linked headers;
6. use `list_artifacts` to find metadata, then selectively call `read_artifact`;
7. call `wait_for_events` with `cursor: snapshot_event_cursor` and reconcile newer events.

The new history lists default to 20 rows and allow at most 50. Continue with the returned `next_cursor` while `has_more` is true and retain the same filters between pages. `get_work_item.worker_projection` identifies the active or latest linked run with role, status, heartbeat, and lease timestamps.

Durable queries are the source of truth. Events only tell an orchestrator that it should reconcile state.

## Events and cursors

`wait_for_events` is the primary waiting mechanism. It returns only events addressed to the actor's repository.

```json
{
  "instance_id": "<instance-id>",
  "cursor": 0,
  "kinds": ["message_available", "request_available", "work_item_updated"],
  "feature_id": "<feature-id>",
  "owner_repository_id": "<repository-id>",
  "limit": 20,
  "timeout_seconds": 30
}
```

Rules:

- an explicit `cursor` is the last processed event ID;
- when `cursor` is omitted and `consumer_key` is supplied, the named checkpoint is used;
- when both are omitted, the wait starts at `0`; when both are supplied, explicit `cursor` wins;
- a returned page never changes a named checkpoint automatically;
- call `commit_event_cursor` with `next_cursor` only after processing the page;
- `timeout_seconds: 0` performs a non-blocking check;
- omitting `timeout_seconds` uses the configured `--max-wait-seconds` ceiling;
- a value above that ceiling is clamped and reported in `effective_timeout_seconds`;
- `outcome: "deadline"` is a normal empty result, not completion or an error;
- each response reports `requested_timeout_seconds`, `effective_timeout_seconds`, and `elapsed_ms`;
- when `has_more` is true, fetch the next page immediately using the new cursor.

`feature_id` and `owner_repository_id` are optional server-side filters. Returned headers include those fields plus `entity_status`, `entity_revision`, and linked `work_item_id`/`request_id` where applicable. Use the compact header to decide whether the event matters, then load the full entity only when necessary. Status and revision are immutable snapshots captured after the mutation; they do not change when the entity changes later.

Events are persisted only for interested repositories:

- Feature create, update, completion, and cancellation events go to every repository in the project;
- WorkItem events and workers linked to those items go to the owner repository and the Feature coordinator, with duplicate recipients removed;
- request events and request-linked butler events go to the requester and target repositories;
- message events go only to their explicit delivery recipients, including the expanded recipient set of a broadcast message.

This means an unrelated repository does not receive every state transition from another repository. Durable history remains available through the authorized list/read tools; event routing only controls wake-up hints.

Named consumers are scoped by `(repository_id, consumer_key)`. Two orchestrators in one repository should use different keys unless they intentionally share progress. `get_event_checkpoint` returns cursor `0` for a new key without creating it. Commits are monotonic and cannot point beyond the current event high-water mark.

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

Use `heartbeat` for an orchestrator and `heartbeat_worker_run` at useful checkpoints for an active worker. Actor-scoped mutations and an active `wait_for_events` also renew relevant actor and claim leases. Discovery lists and simple reads such as `list_inbox`, `read_message`, and `read_artifact` are not heartbeats.

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
| `list_features` | List a bounded project-feature page with optional status filtering. |
| `complete_feature` | Complete a coordinator-owned feature after linked state is terminal. |
| `cancel_feature` | Cancel a coordinator-owned feature and remaining unclaimed work. |
| `create_work_item` | Create an implementation, audit, or remediation card with repository ownership and lineage. |
| `get_work_item` | Read a consistent item snapshot with dependencies and active/latest worker projection. |
| `list_work_items` | Read a bounded page of feature items. |
| `update_work_item` | Update an item using `expected_revision`. |
| `increase_audit_depth` | Explicitly reopen `verification_blocked` work with a larger depth, current revision, and reason. |
| `claim_work_item` | Atomically claim a ready owner-repository item. |
| `renew_work_item_claim` | Renew an owned, unexpired claim. |
| `release_work_item_claim` | Return an owned claim to `ready`. |

### Worker runs

| Tool | Purpose |
| --- | --- |
| `create_worker_run` | Create a role-matched worker for a claimed card, or a butler linked to one `request_id`. |
| `activate_worker_run` | Move a created run to active. The child agent's first call. |
| `heartbeat_worker_run` | Extend an active worker lease. |
| `get_worker_run` | Read worker status, scope, and result data. |
| `list_worker_runs` | Discover compact run history by repository, Feature, item, role, status, or audit link. |
| `complete_worker_run` | Complete an active worker. |
| `fail_worker_run` | Fail a created or active worker. |

Worker scope is exclusive. An `executor` or `auditor` must carry `work_item_id` and
must not carry `request_id`; a `butler` must carry `request_id` and must not carry
`work_item_id`. The runtime returns a stable conflict for an invalid combination,
and a fresh database enforces the same matrix with a SQLite constraint.

### Requests

| Tool | Purpose |
| --- | --- |
| `ask_repository` | Create a durable question with explicit handler and answer requirements. |
| `read_request` | Read a request as its requester or target repository. |
| `claim_request` | Claim a pending request or an expired claim. |
| `renew_request_claim` | Renew an owned request claim. |
| `release_request_claim` | Return an owned request to pending. |
| `answer_request` | Close an owned request, enforce its answer requirement, and record provenance. |
| `fail_request` | Requeue a retryable request or terminally fail it. |
| `list_requests` | Discover visible request history, including handler and answer requirements, by participant, linked entity, or status. |

### Messages and events

| Tool | Purpose |
| --- | --- |
| `publish_message` | Publish a message and optional artifact directly or as a broadcast. |
| `list_inbox` | List pending headers with pagination and kind filtering. |
| `list_deliveries` | List pending, acknowledged, or all deliveries for the actor repository. |
| `list_messages` | Discover accessible message headers by linked entity, thread, kind, producer, or recipient. |
| `list_artifacts` | Discover authorized artifact metadata, including complete batch-audit scope, without loading content. |
| `read_message` | Read an accessible message envelope. |
| `read_artifact` | Read authorized full content; orchestrators can recover explicitly project-shared linked artifacts. |
| `ack_message` | Idempotently acknowledge a delivery. |
| `wait_for_events` | Wait for compact repository event headers with optional Feature/owner filters and a finite ceiling. |
| `get_event_checkpoint` | Read a repository-scoped named consumer cursor. |
| `commit_event_cursor` | Monotonically commit a cursor after processing its page. |

