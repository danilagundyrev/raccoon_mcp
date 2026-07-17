use super::*;

pub(super) fn tool_definitions(
    max_summary_bytes: u16,
    max_artifact_bytes: u32,
    max_wait_seconds: u16,
) -> Value {
    json!([
        {
            "name": "connect_orchestrator",
            "description": "Join or reconnect an orchestrator and return peers plus compact active-feature, pending-inbox and open-request counters. This is the required first call.",
            "inputSchema": {
                "type": "object",
                "additionalProperties": false,
                "properties": {
                    "project_key": { "type": "string", "maxLength": 64 },
                    "project_display_name": { "type": "string" },
                    "repository_key": { "type": "string", "maxLength": 64 },
                    "repository_display_name": { "type": "string" },
                    "repository_path": { "type": "string" },
                    "instance_id": { "type": "string", "format": "uuid" },
                    "capabilities": { "type": "object" }
                },
                "required": ["project_key", "repository_key", "repository_path"]
            },
            "annotations": {
                "readOnlyHint": false,
                "destructiveHint": false,
                "idempotentHint": false,
                "openWorldHint": false
            }
        },
        {
            "name": "heartbeat",
            "description": "Extend an active orchestrator instance lease.",
            "inputSchema": id_schema("instance_id"),
            "annotations": {
                "readOnlyHint": false,
                "destructiveHint": false,
                "idempotentHint": false,
                "openWorldHint": false
            }
        },
        {
            "name": "disconnect_orchestrator",
            "description": "Explicitly mark an orchestrator instance disconnected without deleting repository identity.",
            "inputSchema": id_schema("instance_id"),
            "annotations": {
                "readOnlyHint": false,
                "destructiveHint": false,
                "idempotentHint": true,
                "openWorldHint": false
            }
        },
        {
            "name": "create_worker_run",
            "description": "Create one exactly scoped worker run. Executor and auditor roles require work_item_id and forbid request_id; butler requires request_id and forbids work_item_id. Work-item attempts are sequential: created, active or completed attempts block another run; retry is allowed after failed or abandoned.",
            "inputSchema": {
                "type": "object",
                "additionalProperties": false,
                "properties": {
                    "instance_id": { "type": "string", "format": "uuid" },
                    "role": { "type": "string", "enum": ["executor", "butler", "auditor"] },
                    "task_summary": {
                        "type": "string",
                        "maxLength": max_summary_bytes,
                        "description": "Compact summary. The server enforces max_summary_bytes as UTF-8 bytes; put full context in artifacts."
                    },
                    "context_refs": {
                        "type": "array",
                        "maxItems": MAX_CONTEXT_REFS,
                        "items": { "type": "object" }
                    },
                    "idempotency_key": {
                        "type": "string",
                        "minLength": 1,
                        "maxLength": 128,
                        "description": "Optional actor-scoped retry key. Reuse requires an identical payload."
                    },
                    "addresses_audit_run_id": {
                        "type": "string",
                        "format": "uuid",
                        "description": "Only for one executor run that remediates the complete report of a completed auditor run."
                    },
                    "work_item_id": {
                        "type": "string",
                        "format": "uuid",
                        "description": "Required for executor and auditor, forbidden for butler. The claimed implementation, audit or remediation kind must match the worker role."
                    },
                    "request_id": {
                        "type": "string",
                        "format": "uuid",
                        "description": "Required for butler and forbidden for executor/auditor. The butler may claim and answer only this request."
                    }
                },
                "required": ["instance_id", "role", "task_summary"]
            },
            "annotations": {
                "readOnlyHint": false,
                "destructiveHint": false,
                "idempotentHint": false,
                "openWorldHint": false
            }
        },
        {
            "name": "activate_worker_run",
            "description": "Activate a worker run from the spawned child before it starts work.",
            "inputSchema": id_schema("worker_run_id"),
            "annotations": {
                "readOnlyHint": false,
                "destructiveHint": false,
                "idempotentHint": false,
                "openWorldHint": false
            }
        },
        {
            "name": "heartbeat_worker_run",
            "description": "Extend the lease of an active worker run at a natural work checkpoint.",
            "inputSchema": id_schema("worker_run_id"),
            "annotations": {
                "readOnlyHint": false,
                "destructiveHint": false,
                "idempotentHint": false,
                "openWorldHint": false
            }
        },
        {
            "name": "complete_worker_run",
            "description": "Complete an active worker run with a compact result summary.",
            "inputSchema": worker_finish_schema("result_summary", max_summary_bytes),
            "annotations": {
                "readOnlyHint": false,
                "destructiveHint": false,
                "idempotentHint": true,
                "openWorldHint": false
            }
        },
        {
            "name": "fail_worker_run",
            "description": "Fail a created or active worker run with a compact error summary.",
            "inputSchema": worker_finish_schema("error_summary", max_summary_bytes),
            "annotations": {
                "readOnlyHint": false,
                "destructiveHint": false,
                "idempotentHint": true,
                "openWorldHint": false
            }
        },
        {
            "name": "get_worker_run",
            "description": "Read one worker run without exposing it as a repository or mailbox identity.",
            "inputSchema": id_schema("worker_run_id"),
            "annotations": {
                "readOnlyHint": true,
                "destructiveHint": false,
                "idempotentHint": true,
                "openWorldHint": false
            }
        },
        {
            "name": "create_feature",
            "description": "Create a project-scoped root feature card.",
            "inputSchema": create_feature_schema(max_summary_bytes),
            "annotations": mutation_annotations(false)
        },
        {
            "name": "get_feature",
            "description": "Read one compact feature card.",
            "inputSchema": id_schema("feature_id"),
            "annotations": mutation_annotations(true)
        },
        {
            "name": "list_features",
            "description": "List one stable bounded page of compact feature cards in a project.",
            "inputSchema": list_features_schema(),
            "annotations": mutation_annotations(true)
        },
        {
            "name": "complete_feature",
            "description": "Complete an active feature after all linked work, repository requests and worker runs are terminal, including request-linked butlers, and every published implementation report is independently verified. Only its coordinator repository may close it.",
            "inputSchema": feature_transition_schema(),
            "annotations": mutation_annotations(false)
        },
        {
            "name": "cancel_feature",
            "description": "Cancel an active feature and its unclaimed work. Open requests, active workers or claimed work must be resolved first.",
            "inputSchema": feature_transition_schema(),
            "annotations": mutation_annotations(false)
        },
        {
            "name": "create_work_item",
            "description": "Create a repository-owned work item with dependencies and audit policy.",
            "inputSchema": create_work_item_schema(max_summary_bytes),
            "annotations": mutation_annotations(false)
        },
        {
            "name": "get_work_item",
            "description": "Read one work item with dependencies, revision, claim and artifact links.",
            "inputSchema": id_schema("work_item_id"),
            "annotations": mutation_annotations(true)
        },
        {
            "name": "list_work_items",
            "description": "List one bounded page of work items for a feature. Higher numeric priority values appear first.",
            "inputSchema": list_work_items_schema(),
            "annotations": mutation_annotations(true)
        },
        {
            "name": "update_work_item",
            "description": "Update a work item using optimistic expected_revision. Completing implementation or remediation requires implementation_artifact_id from its completed executor; completing audit requires its completed auditor and audit_report. An implementation item cannot be cancelled after a linked executor publishes implementation_report.",
            "inputSchema": update_work_item_schema(max_summary_bytes),
            "annotations": mutation_annotations(false)
        },
        {
            "name": "increase_audit_depth",
            "description": "Explicitly reopen verification after audit depth is exhausted. Requires the current revision and a durable reason.",
            "inputSchema": increase_audit_depth_schema(max_summary_bytes),
            "annotations": mutation_annotations(false)
        },
        {
            "name": "claim_work_item",
            "description": "Atomically claim a ready work item for its owner repository.",
            "inputSchema": work_item_claim_schema(),
            "annotations": mutation_annotations(false)
        },
        {
            "name": "renew_work_item_claim",
            "description": "Renew an unexpired work-item claim owned by this orchestrator instance.",
            "inputSchema": work_item_claim_schema(),
            "annotations": mutation_annotations(false)
        },
        {
            "name": "release_work_item_claim",
            "description": "Release an owned work-item claim back to ready.",
            "inputSchema": work_item_claim_schema(),
            "annotations": mutation_annotations(false)
        },
        {
            "name": "publish_message",
            "description": "Publish an immutable message envelope and optional artifact, then atomically fan it out to repository mailboxes.",
            "inputSchema": publish_message_schema(max_summary_bytes, max_artifact_bytes),
            "annotations": {
                "readOnlyHint": false,
                "destructiveHint": false,
                "idempotentHint": false,
                "openWorldHint": false
            }
        },
        {
            "name": "wait_for_events",
            "description": "Wait for compact append-only event headers addressed to the actor's repository, optionally filtered by Feature and owner repository. An explicit cursor wins; otherwise consumer_key uses its durable checkpoint, or zero is used when both are absent. Returned pages never auto-commit a checkpoint.",
            "inputSchema": wait_for_events_schema(max_wait_seconds),
            "annotations": {
                "readOnlyHint": false,
                "destructiveHint": false,
                "idempotentHint": false,
                "openWorldHint": false
            }
        },
        {
            "name": "get_event_checkpoint",
            "description": "Read one repository-scoped named event checkpoint. An unknown consumer starts at cursor zero without creating state.",
            "inputSchema": event_checkpoint_schema(false),
            "annotations": mutation_annotations(true)
        },
        {
            "name": "commit_event_cursor",
            "description": "Monotonically commit a processed event cursor for one repository-scoped named consumer. The server never calls this automatically.",
            "inputSchema": event_checkpoint_schema(true),
            "annotations": {
                "readOnlyHint": false,
                "destructiveHint": false,
                "idempotentHint": true,
                "openWorldHint": false
            }
        },
        {
            "name": "ask_repository",
            "description": "Create a durable question for one repository. Select whether coordination-state answers are allowed or a linked request-scoped butler is required.",
            "inputSchema": ask_repository_schema(max_summary_bytes, max_artifact_bytes),
            "annotations": mutation_annotations(false)
        },
        {
            "name": "read_request",
            "description": "Read request state from its requester or target repository without renewing actor or claim leases.",
            "inputSchema": actor_entity_schema("request_id"),
            "annotations": mutation_annotations(true)
        },
        {
            "name": "claim_request",
            "description": "Atomically claim a pending request, or take over an expired claim. butler_required requests accept only their active linked butler.",
            "inputSchema": actor_entity_schema("request_id"),
            "annotations": mutation_annotations(false)
        },
        {
            "name": "renew_request_claim",
            "description": "Extend an unexpired request claim owned by this exact actor.",
            "inputSchema": actor_entity_schema("request_id"),
            "annotations": mutation_annotations(false)
        },
        {
            "name": "release_request_claim",
            "description": "Release an owned request claim back to pending and notify the target repository.",
            "inputSchema": actor_entity_schema("request_id"),
            "annotations": mutation_annotations(false)
        },
        {
            "name": "answer_request",
            "description": "Atomically close an owned request and deliver its answer. artifact_required requests require a non-empty body; provenance is recorded from the answering actor.",
            "inputSchema": answer_request_schema(max_summary_bytes, max_artifact_bytes),
            "annotations": mutation_annotations(false)
        },
        {
            "name": "fail_request",
            "description": "Fail an owned request. Retryable failures return it to pending; terminal failures notify the requester.",
            "inputSchema": fail_request_schema(max_summary_bytes),
            "annotations": mutation_annotations(false)
        },
        {
            "name": "list_inbox",
            "description": "List pending message headers for the actor's repository. Full artifact content is never included.",
            "inputSchema": inbox_schema(),
            "annotations": {
                "readOnlyHint": true,
                "destructiveHint": false,
                "idempotentHint": true,
                "openWorldHint": false
            }
        },
        {
            "name": "list_worker_runs",
            "description": "Discover compact worker-run history with stable cursor pagination. Orchestrators see their project; workers are limited to their repository.",
            "inputSchema": list_worker_runs_schema(),
            "annotations": mutation_annotations(true)
        },
        {
            "name": "list_requests",
            "description": "Discover compact durable requests visible to the actor repository, including handler_requirement and answer_requirement for routing without per-request reads.",
            "inputSchema": list_requests_schema(),
            "annotations": mutation_annotations(true)
        },
        {
            "name": "list_deliveries",
            "description": "List pending, acknowledged or all deliveries for the actor repository.",
            "inputSchema": list_deliveries_schema(),
            "annotations": mutation_annotations(true)
        },
        {
            "name": "list_messages",
            "description": "Discover accessible message headers by linked entity, thread, kind, producer or recipient.",
            "inputSchema": list_messages_schema(),
            "annotations": mutation_annotations(true)
        },
        {
            "name": "list_artifacts",
            "description": "Discover accessible artifact metadata without content. Batch audit metadata separates audit_work_item_id from deterministic scoped_work_item_ids. Orchestrators may recover explicitly project-shared WorkItem reports and linked coordination artifacts; request artifacts remain participant-private. Use read_artifact for the body.",
            "inputSchema": list_artifacts_schema(),
            "annotations": mutation_annotations(true)
        },
        {
            "name": "read_message",
            "description": "Read one accessible immutable message envelope. Read its artifact separately when needed.",
            "inputSchema": actor_entity_schema("message_id"),
            "annotations": {
                "readOnlyHint": true,
                "destructiveHint": false,
                "idempotentHint": true,
                "openWorldHint": false
            }
        },
        {
            "name": "read_artifact",
            "description": "Read full artifact content after actor-scoped authorization. Orchestrators may also read same-project artifacts accepted as WorkItem implementation/audit reports or attached to a non-request message with a Feature/WorkItem link. Request artifacts stay limited to their participants; workers retain narrower repository/run access.",
            "inputSchema": actor_entity_schema("artifact_id"),
            "annotations": {
                "readOnlyHint": true,
                "destructiveHint": false,
                "idempotentHint": true,
                "openWorldHint": false
            }
        },
        {
            "name": "ack_message",
            "description": "Idempotently acknowledge one message in the actor repository's mailbox after required content has been processed.",
            "inputSchema": actor_entity_schema("message_id"),
            "annotations": {
                "readOnlyHint": false,
                "destructiveHint": false,
                "idempotentHint": true,
                "openWorldHint": false
            }
        },
        {
            "name": "list_repositories",
            "description": "List repositories in a project with their current online instance counts.",
            "inputSchema": list_repositories_schema(),
            "annotations": {
                "readOnlyHint": true,
                "destructiveHint": false,
                "idempotentHint": true,
                "openWorldHint": false
            }
        },
        {
            "name": "get_project_overview",
            "description": "Read the compact project and repository presence overview.",
            "inputSchema": id_schema("project_id"),
            "annotations": {
                "readOnlyHint": true,
                "destructiveHint": false,
                "idempotentHint": true,
                "openWorldHint": false
            }
        }
    ])
}

pub(super) fn id_schema(field_name: &str) -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "properties": {
            field_name: { "type": "string", "format": "uuid" }
        },
        "required": [field_name]
    })
}

pub(super) fn create_feature_schema(max_summary_bytes: u16) -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "properties": {
            "instance_id": { "type": "string", "format": "uuid" },
            "title": { "type": "string", "maxLength": max_summary_bytes },
            "summary": { "type": "string", "maxLength": max_summary_bytes },
            "idempotency_key": idempotency_key_schema()
        },
        "required": ["instance_id", "title", "summary"]
    })
}

pub(super) fn feature_transition_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "properties": {
            "instance_id": { "type": "string", "format": "uuid" },
            "feature_id": { "type": "string", "format": "uuid" },
            "expected_revision": { "type": "integer", "minimum": 1 }
        },
        "required": ["instance_id", "feature_id", "expected_revision"]
    })
}

pub(super) fn create_work_item_schema(max_summary_bytes: u16) -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "properties": {
            "instance_id": { "type": "string", "format": "uuid" },
            "feature_id": { "type": "string", "format": "uuid" },
            "parent_work_item_id": { "type": "string", "format": "uuid" },
            "owner_repository_id": { "type": "string", "format": "uuid" },
            "title": { "type": "string", "maxLength": max_summary_bytes },
            "summary": { "type": "string", "maxLength": max_summary_bytes },
            "kind": {
                "type": "string",
                "enum": ["implementation", "audit", "remediation"],
                "default": "implementation"
            },
            "audit_scope_work_item_ids": {
                "type": "array",
                "minItems": 1,
                "maxItems": MAX_AUDIT_WORK_ITEMS,
                "uniqueItems": true,
                "items": { "type": "string", "format": "uuid" },
                "description": "Required only for audit work; every item must be a completed implementation owned by owner_repository_id."
            },
            "addresses_audit_run_id": {
                "type": "string",
                "format": "uuid",
                "description": "Required only for remediation work and identifies the completed audit being addressed."
            },
            "dependency_ids": {
                "type": "array",
                "maxItems": MAX_WORK_ITEM_DEPENDENCIES,
                "uniqueItems": true,
                "items": { "type": "string", "format": "uuid" }
            },
            "priority": {
                "type": "integer",
                "minimum": -32768,
                "maximum": 32767,
                "default": 0,
                "description": "Higher numeric values are processed before lower values when other eligibility rules are equal."
            },
            "max_audit_iterations": { "type": "integer", "minimum": 1, "maximum": 10, "default": 2 },
            "idempotency_key": idempotency_key_schema()
        },
        "required": ["instance_id", "feature_id", "owner_repository_id", "title", "summary"]
    })
}

pub(super) fn update_work_item_schema(max_summary_bytes: u16) -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "properties": {
            "instance_id": { "type": "string", "format": "uuid" },
            "work_item_id": { "type": "string", "format": "uuid" },
            "expected_revision": { "type": "integer", "minimum": 1 },
            "parent_work_item_id": {
                "oneOf": [
                    { "type": "string", "format": "uuid" },
                    { "type": "null" }
                ]
            },
            "owner_repository_id": { "type": "string", "format": "uuid" },
            "title": { "type": "string", "maxLength": max_summary_bytes },
            "summary": { "type": "string", "maxLength": max_summary_bytes },
            "dependency_ids": {
                "type": "array",
                "maxItems": MAX_WORK_ITEM_DEPENDENCIES,
                "uniqueItems": true,
                "items": { "type": "string", "format": "uuid" }
            },
            "priority": {
                "type": "integer",
                "minimum": -32768,
                "maximum": 32767,
                "description": "Higher numeric values are processed before lower values when other eligibility rules are equal."
            },
            "status": {
                "type": "string",
                "enum": ["planned", "ready", "claimed", "in_progress", "blocked", "completed", "failed", "cancelled"]
            },
            "implementation_artifact_id": {
                "type": "string",
                "format": "uuid",
                "description": "Accepted only when completing implementation or remediation work; audit completion derives its report from the linked auditor."
            }
        },
        "required": ["instance_id", "work_item_id", "expected_revision"]
    })
}

pub(super) fn work_item_claim_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "properties": {
            "instance_id": { "type": "string", "format": "uuid" },
            "work_item_id": { "type": "string", "format": "uuid" },
            "expected_revision": { "type": "integer", "minimum": 1 }
        },
        "required": ["instance_id", "work_item_id", "expected_revision"]
    })
}

pub(super) fn increase_audit_depth_schema(max_summary_bytes: u16) -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "properties": {
            "instance_id": { "type": "string", "format": "uuid" },
            "work_item_id": { "type": "string", "format": "uuid" },
            "expected_revision": { "type": "integer", "minimum": 1 },
            "max_audit_iterations": { "type": "integer", "minimum": 2, "maximum": 10 },
            "reason": { "type": "string", "minLength": 1, "maxLength": max_summary_bytes }
        },
        "required": ["instance_id", "work_item_id", "expected_revision", "max_audit_iterations", "reason"]
    })
}

pub(super) fn worker_finish_schema(summary_field: &str, max_summary_bytes: u16) -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "properties": {
            "worker_run_id": { "type": "string", "format": "uuid" },
            summary_field: {
                "type": "string",
                "maxLength": max_summary_bytes,
                "description": "Compact summary limited by the server's max_summary_bytes setting."
            }
        },
        "required": ["worker_run_id", summary_field]
    })
}

pub(super) fn actor_properties() -> JsonMap<String, Value> {
    let mut properties = JsonMap::new();
    properties.insert(
        "instance_id".to_owned(),
        json!({
            "type": "string",
            "format": "uuid",
            "description": "Orchestrator actor identity. Provide exactly one of instance_id or worker_run_id."
        }),
    );
    properties.insert(
        "worker_run_id".to_owned(),
        json!({
            "type": "string",
            "format": "uuid",
            "description": "Worker actor identity. Provide exactly one of instance_id or worker_run_id."
        }),
    );
    properties
}

pub(super) fn actor_entity_schema(entity_field: &str) -> Value {
    let mut properties = actor_properties();
    properties.insert(
        entity_field.to_owned(),
        json!({ "type": "string", "format": "uuid" }),
    );
    json!({
        "type": "object",
        "additionalProperties": false,
        "properties": properties,
        "required": [entity_field]
    })
}

pub(super) fn inbox_schema() -> Value {
    let mut properties = actor_properties();
    properties.insert(
        "after_delivery_id".to_owned(),
        json!({ "type": "integer", "minimum": 0 }),
    );
    properties.insert(
        "limit".to_owned(),
        json!({ "type": "integer", "minimum": 1, "maximum": 100, "default": 20 }),
    );
    properties.insert(
        "kinds".to_owned(),
        json!({
            "type": "array",
            "maxItems": 3,
            "uniqueItems": true,
            "items": { "type": "string", "enum": ["report", "announcement", "command"] }
        }),
    );
    json!({
        "type": "object",
        "additionalProperties": false,
        "properties": properties
    })
}

fn history_properties(cursor_name: &str) -> JsonMap<String, Value> {
    let mut properties = actor_properties();
    properties.insert(
        cursor_name.to_owned(),
        json!({ "type": "string", "format": "uuid" }),
    );
    properties.insert(
        "limit".to_owned(),
        json!({ "type": "integer", "minimum": 1, "maximum": 50, "default": 20 }),
    );
    properties
}

pub(super) fn list_features_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "properties": {
            "project_id": { "type": "string", "format": "uuid" },
            "status": { "type": "string", "enum": ["active", "completed", "cancelled"] },
            "after_feature_id": { "type": "string", "format": "uuid" },
            "limit": { "type": "integer", "minimum": 1, "maximum": 50, "default": 20 }
        },
        "required": ["project_id"]
    })
}

pub(super) fn list_worker_runs_schema() -> Value {
    let mut properties = history_properties("after_worker_run_id");
    for name in [
        "repository_id",
        "feature_id",
        "work_item_id",
        "addresses_audit_run_id",
    ] {
        properties.insert(
            name.to_owned(),
            json!({ "type": "string", "format": "uuid" }),
        );
    }
    properties.insert(
        "role".to_owned(),
        json!({ "type": "string", "enum": ["executor", "butler", "auditor"] }),
    );
    properties.insert(
        "status".to_owned(),
        json!({ "type": "string", "enum": ["created", "active", "completed", "failed", "abandoned"] }),
    );
    json!({ "type": "object", "additionalProperties": false, "properties": properties })
}

pub(super) fn list_requests_schema() -> Value {
    let mut properties = history_properties("after_request_id");
    for name in [
        "requester_repository_id",
        "target_repository_id",
        "feature_id",
        "work_item_id",
    ] {
        properties.insert(
            name.to_owned(),
            json!({ "type": "string", "format": "uuid" }),
        );
    }
    properties.insert(
        "status".to_owned(),
        json!({ "type": "string", "enum": ["pending", "claimed", "answered", "failed"] }),
    );
    json!({ "type": "object", "additionalProperties": false, "properties": properties })
}

pub(super) fn list_deliveries_schema() -> Value {
    let mut properties = actor_properties();
    properties.insert(
        "status".to_owned(),
        json!({ "type": "string", "enum": ["pending", "acknowledged", "all"], "default": "all" }),
    );
    properties.insert(
        "after_delivery_id".to_owned(),
        json!({ "type": "integer", "minimum": 0 }),
    );
    properties.insert(
        "limit".to_owned(),
        json!({ "type": "integer", "minimum": 1, "maximum": 50, "default": 20 }),
    );
    json!({ "type": "object", "additionalProperties": false, "properties": properties })
}

pub(super) fn list_messages_schema() -> Value {
    let mut properties = history_properties("after_message_id");
    for name in [
        "feature_id",
        "work_item_id",
        "request_id",
        "thread_id",
        "producer_repository_id",
        "recipient_repository_id",
    ] {
        properties.insert(
            name.to_owned(),
            json!({ "type": "string", "format": "uuid" }),
        );
    }
    properties.insert(
        "kind".to_owned(),
        json!({ "type": "string", "enum": ["report", "announcement", "command"] }),
    );
    json!({ "type": "object", "additionalProperties": false, "properties": properties })
}

pub(super) fn list_artifacts_schema() -> Value {
    let mut properties = history_properties("after_artifact_id");
    for name in [
        "feature_id",
        "work_item_id",
        "request_id",
        "producer_repository_id",
        "producer_worker_run_id",
    ] {
        properties.insert(
            name.to_owned(),
            json!({ "type": "string", "format": "uuid" }),
        );
    }
    properties.insert(
        "kind".to_owned(),
        json!({
            "type": "string",
            "enum": ["implementation_report", "api_contract", "answer", "audit_report", "note"]
        }),
    );
    json!({ "type": "object", "additionalProperties": false, "properties": properties })
}

pub(super) fn list_work_items_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "properties": {
            "feature_id": { "type": "string", "format": "uuid" },
            "after_work_item_id": { "type": "string", "format": "uuid" },
            "limit": { "type": "integer", "minimum": 1, "maximum": 100, "default": 20 }
        },
        "required": ["feature_id"]
    })
}

pub(super) fn list_repositories_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "properties": {
            "project_id": { "type": "string", "format": "uuid" },
            "presence_filter": {
                "type": "string",
                "enum": ["all", "online", "offline"],
                "default": "all"
            }
        },
        "required": ["project_id"]
    })
}

pub(super) fn wait_for_events_schema(max_wait_seconds: u16) -> Value {
    let mut properties = actor_properties();
    properties.insert(
        "cursor".to_owned(),
        json!({
            "type": "integer",
            "minimum": 0,
            "description": "Explicit last-processed event ID. Overrides consumer_key when both are provided."
        }),
    );
    properties.insert("consumer_key".to_owned(), consumer_key_schema());
    properties.insert(
        "feature_id".to_owned(),
        json!({
            "type": "string",
            "format": "uuid",
            "description": "Return only event headers linked to this Feature."
        }),
    );
    properties.insert(
        "owner_repository_id".to_owned(),
        json!({
            "type": "string",
            "format": "uuid",
            "description": "Return only event headers owned by this repository."
        }),
    );
    properties.insert(
        "kinds".to_owned(),
        json!({
            "type": "array",
            "maxItems": 16,
            "uniqueItems": true,
            "items": {
                "type": "string",
                "enum": [
                    "message_available",
                    "worker_run_created",
                    "worker_run_active",
                    "worker_run_completed",
                    "worker_run_failed",
                    "request_available",
                    "request_claimed",
                    "request_answered",
                    "request_failed",
                    "feature_created",
                    "feature_updated",
                    "feature_completed",
                    "feature_cancelled",
                    "work_item_created",
                    "work_item_updated",
                    "audit_depth_increased"
                ]
            }
        }),
    );
    properties.insert(
        "timeout_seconds".to_owned(),
        json!({
            "type": "integer",
            "minimum": 0,
            "maximum": max_wait_seconds,
            "description": "Omission uses the server maximum; values above it are clamped by runtime validation, and zero performs a non-blocking event check."
        }),
    );
    properties.insert(
        "limit".to_owned(),
        json!({ "type": "integer", "minimum": 1, "maximum": 50, "default": 20 }),
    );
    json!({
        "type": "object",
        "additionalProperties": false,
        "properties": properties
    })
}

fn consumer_key_schema() -> Value {
    json!({
        "type": "string",
        "minLength": 1,
        "maxLength": 64,
        "pattern": "^[A-Za-z0-9._-]+$"
    })
}

pub(super) fn event_checkpoint_schema(commit: bool) -> Value {
    let mut properties = actor_properties();
    properties.insert("consumer_key".to_owned(), consumer_key_schema());
    let mut required = vec!["consumer_key"];
    if commit {
        properties.insert(
            "cursor".to_owned(),
            json!({ "type": "integer", "minimum": 0 }),
        );
        required.push("cursor");
    }
    json!({
        "type": "object",
        "additionalProperties": false,
        "properties": properties,
        "required": required
    })
}

pub(super) fn ask_repository_schema(max_summary_bytes: u16, max_artifact_bytes: u32) -> Value {
    let mut properties = actor_properties();
    properties.insert(
        "target_repository_id".to_owned(),
        json!({ "type": "string", "format": "uuid" }),
    );
    properties.insert(
        "feature_id".to_owned(),
        json!({ "type": "string", "format": "uuid" }),
    );
    properties.insert(
        "work_item_id".to_owned(),
        json!({
            "type": "string",
            "format": "uuid",
            "description": "Optional work-item scope. The server infers feature_id and rejects a conflicting explicit feature_id."
        }),
    );
    properties.insert(
        "question_summary".to_owned(),
        json!({ "type": "string", "maxLength": max_summary_bytes }),
    );
    properties.insert(
        "body".to_owned(),
        json!({ "type": "string", "maxLength": max_artifact_bytes }),
    );
    properties.insert(
        "context_refs".to_owned(),
        json!({
            "type": "array",
            "maxItems": MAX_CONTEXT_REFS,
            "items": { "type": "object" }
        }),
    );
    properties.insert(
        "handler_requirement".to_owned(),
        json!({
            "type": "string",
            "enum": ["orchestrator_allowed", "butler_required"],
            "default": "orchestrator_allowed",
            "description": "Use butler_required when answering requires current repository investigation."
        }),
    );
    properties.insert(
        "answer_requirement".to_owned(),
        json!({
            "type": "string",
            "enum": ["summary_allowed", "artifact_required"],
            "default": "summary_allowed",
            "description": "artifact_required makes answer_request require a non-empty body."
        }),
    );
    properties.insert("idempotency_key".to_owned(), idempotency_key_schema());
    json!({
        "type": "object",
        "additionalProperties": false,
        "properties": properties,
        "required": ["target_repository_id", "question_summary"]
    })
}

pub(super) fn answer_request_schema(max_summary_bytes: u16, max_artifact_bytes: u32) -> Value {
    let mut properties = actor_properties();
    properties.insert(
        "request_id".to_owned(),
        json!({ "type": "string", "format": "uuid" }),
    );
    properties.insert(
        "answer_summary".to_owned(),
        json!({ "type": "string", "maxLength": max_summary_bytes }),
    );
    properties.insert(
        "body".to_owned(),
        json!({ "type": "string", "maxLength": max_artifact_bytes }),
    );
    properties.insert(
        "content_type".to_owned(),
        json!({ "type": "string", "default": "text/markdown" }),
    );
    json!({
        "type": "object",
        "additionalProperties": false,
        "properties": properties,
        "required": ["request_id", "answer_summary"]
    })
}

pub(super) fn fail_request_schema(max_summary_bytes: u16) -> Value {
    let mut properties = actor_properties();
    properties.insert(
        "request_id".to_owned(),
        json!({ "type": "string", "format": "uuid" }),
    );
    properties.insert("retryable".to_owned(), json!({ "type": "boolean" }));
    properties.insert(
        "summary".to_owned(),
        json!({ "type": "string", "maxLength": max_summary_bytes }),
    );
    json!({
        "type": "object",
        "additionalProperties": false,
        "properties": properties,
        "required": ["request_id", "retryable", "summary"]
    })
}

pub(super) fn mutation_annotations(read_only: bool) -> Value {
    json!({
        "readOnlyHint": read_only,
        "destructiveHint": false,
        "idempotentHint": read_only,
        "openWorldHint": false
    })
}

pub(super) fn publish_message_schema(max_summary_bytes: u16, max_artifact_bytes: u32) -> Value {
    let mut properties = actor_properties();
    properties.extend(JsonMap::from_iter([
        (
            "kind".to_owned(),
            json!({ "type": "string", "enum": ["report", "announcement", "command"] }),
        ),
        (
            "broadcast".to_owned(),
            json!({
                "type": "boolean",
                "description": "Set true for project-wide delivery. Provide exactly one of broadcast=true or target_repository_ids."
            }),
        ),
        (
            "target_repository_ids".to_owned(),
            json!({
                "type": "array",
                "minItems": 1,
                "maxItems": MAX_MESSAGE_RECIPIENTS,
                "uniqueItems": true,
                "items": { "type": "string", "format": "uuid" },
                "description": "Direct recipients. Provide exactly one of target_repository_ids or broadcast=true."
            }),
        ),
        (
            "summary".to_owned(),
            json!({ "type": "string", "maxLength": max_summary_bytes }),
        ),
        (
            "body".to_owned(),
            json!({
                "type": "string",
                "maxLength": max_artifact_bytes,
                "description": "Optional full content stored as a separate artifact. Server limit is UTF-8 bytes."
            }),
        ),
        (
            "artifact_kind".to_owned(),
            json!({
                "type": "string",
                "enum": ["implementation_report", "api_contract", "answer", "audit_report", "note"],
                "default": "note"
            }),
        ),
        ("artifact_title".to_owned(), json!({ "type": "string" })),
        (
            "content_type".to_owned(),
            json!({ "type": "string", "default": "text/markdown" }),
        ),
        (
            "feature_id".to_owned(),
            json!({ "type": "string", "format": "uuid" }),
        ),
        (
            "work_item_id".to_owned(),
            json!({ "type": "string", "format": "uuid" }),
        ),
        (
            "request_id".to_owned(),
            json!({ "type": "string", "format": "uuid" }),
        ),
        (
            "thread_id".to_owned(),
            json!({ "type": "string", "format": "uuid" }),
        ),
        (
            "correlation_id".to_owned(),
            json!({ "type": "string", "maxLength": 128 }),
        ),
        (
            "in_reply_to".to_owned(),
            json!({ "type": "string", "format": "uuid" }),
        ),
        (
            "audit_outcomes".to_owned(),
            json!({
                "type": "array",
                "minItems": 1,
                "maxItems": MAX_AUDIT_WORK_ITEMS,
                "items": {
                    "type": "object",
                    "additionalProperties": false,
                    "properties": {
                        "work_item_id": { "type": "string", "format": "uuid" },
                        "verdict": {
                            "type": "string",
                            "enum": ["passed", "changes_required", "blocked"]
                        },
                        "summary": { "type": "string", "minLength": 1, "maxLength": max_summary_bytes }
                    },
                    "required": ["work_item_id", "verdict", "summary"]
                },
                "description": "Required with audit_report. Must cover the auditor run scope exactly once."
            }),
        ),
        ("idempotency_key".to_owned(), idempotency_key_schema()),
    ]));
    json!({
        "type": "object",
        "additionalProperties": false,
        "properties": properties,
        "required": ["kind", "summary"]
    })
}

pub(super) fn idempotency_key_schema() -> Value {
    json!({
        "type": "string",
        "minLength": 1,
        "maxLength": 128,
        "description": "Optional actor-scoped retry key. Reuse requires an identical payload."
    })
}

pub(super) fn list_resources() -> Value {
    json!([{
        "uri": "hub://guide",
        "name": "Coordination hub guide",
        "description": "Machine-readable bootstrap guidance and current implementation scope.",
        "mimeType": "application/json"
    }])
}

pub(super) fn list_resource_templates() -> Value {
    json!([
        {
            "uriTemplate": "hub://project/{project_id}/overview",
            "name": "Project overview",
            "description": "Project metadata and repository presence.",
            "mimeType": "application/json"
        },
        {
            "uriTemplate": "hub://project/{project_id}/repositories",
            "name": "Project repositories",
            "description": "Repositories and online orchestrator counts.",
            "mimeType": "application/json"
        },
        {
            "uriTemplate": "hub://feature/{feature_id}",
            "name": "Feature card",
            "description": "One compact project feature card.",
            "mimeType": "application/json"
        },
        {
            "uriTemplate": "hub://work-item/{work_item_id}",
            "name": "Work-item card",
            "description": "One compact work-item card with revision, dependencies and artifact links.",
            "mimeType": "application/json"
        }
    ])
}

pub(super) fn resource_contents(uri: &str, payload: Value) -> Value {
    json!({
        "contents": [{
            "uri": uri,
            "mimeType": "application/json",
            "text": serde_json::to_string_pretty(&payload).unwrap_or_else(|_| "{}".to_owned())
        }]
    })
}
