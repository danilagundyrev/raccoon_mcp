pub const SCHEMA_VERSION: &str = "1";

pub const CREATE_SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS schema_meta (
    key TEXT PRIMARY KEY NOT NULL,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS projects (
    project_id TEXT PRIMARY KEY NOT NULL,
    project_key TEXT NOT NULL UNIQUE,
    display_name TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS repositories (
    repository_id TEXT PRIMARY KEY NOT NULL,
    project_id TEXT NOT NULL REFERENCES projects(project_id) ON DELETE CASCADE,
    repository_key TEXT NOT NULL,
    display_name TEXT NOT NULL,
    canonical_path TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(project_id, repository_key)
);

CREATE INDEX IF NOT EXISTS repositories_project_idx
    ON repositories(project_id, repository_key);

CREATE TABLE IF NOT EXISTS agent_instances (
    instance_id TEXT PRIMARY KEY NOT NULL,
    repository_id TEXT NOT NULL REFERENCES repositories(repository_id) ON DELETE CASCADE,
    kind TEXT NOT NULL CHECK(kind = 'orchestrator'),
    status TEXT NOT NULL CHECK(status IN ('online', 'disconnected')),
    capabilities_json TEXT NOT NULL,
    started_at TEXT NOT NULL,
    last_heartbeat_at TEXT NOT NULL,
    lease_expires_at TEXT NOT NULL,
    disconnected_at TEXT
);

CREATE INDEX IF NOT EXISTS agent_instances_presence_idx
    ON agent_instances(repository_id, status, lease_expires_at);

CREATE TABLE IF NOT EXISTS worker_runs (
    worker_run_id TEXT PRIMARY KEY NOT NULL,
    repository_id TEXT NOT NULL REFERENCES repositories(repository_id) ON DELETE CASCADE,
    parent_instance_id TEXT NOT NULL REFERENCES agent_instances(instance_id) ON DELETE RESTRICT,
    role TEXT NOT NULL CHECK(role IN ('executor', 'butler', 'auditor')),
    status TEXT NOT NULL CHECK(status IN ('created', 'active', 'completed', 'failed', 'abandoned')),
    task_summary TEXT NOT NULL,
    context_refs_json TEXT NOT NULL,
    result_summary TEXT,
    error_summary TEXT,
    addresses_audit_run_id TEXT REFERENCES worker_runs(worker_run_id) ON DELETE RESTRICT,
    created_at TEXT NOT NULL,
    activated_at TEXT,
    last_heartbeat_at TEXT NOT NULL,
    lease_expires_at TEXT NOT NULL,
    finished_at TEXT
);

CREATE INDEX IF NOT EXISTS worker_runs_repository_status_idx
    ON worker_runs(repository_id, status, lease_expires_at);

CREATE INDEX IF NOT EXISTS worker_runs_parent_status_idx
    ON worker_runs(parent_instance_id, status);

CREATE UNIQUE INDEX IF NOT EXISTS worker_runs_one_remediation_per_audit_idx
    ON worker_runs(addresses_audit_run_id)
    WHERE addresses_audit_run_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS worker_run_audit_items (
    worker_run_id TEXT NOT NULL REFERENCES worker_runs(worker_run_id) ON DELETE CASCADE,
    work_item_id TEXT NOT NULL,
    audit_iteration INTEGER NOT NULL CHECK(audit_iteration BETWEEN 1 AND 10),
    max_iterations INTEGER NOT NULL CHECK(max_iterations BETWEEN 1 AND 10),
    PRIMARY KEY(worker_run_id, work_item_id),
    UNIQUE(work_item_id, audit_iteration)
);

CREATE INDEX IF NOT EXISTS worker_run_audit_items_work_item_idx
    ON worker_run_audit_items(work_item_id, audit_iteration);

CREATE TABLE IF NOT EXISTS artifacts (
    artifact_id TEXT PRIMARY KEY NOT NULL,
    project_id TEXT NOT NULL REFERENCES projects(project_id) ON DELETE CASCADE,
    producer_repository_id TEXT NOT NULL REFERENCES repositories(repository_id) ON DELETE CASCADE,
    producer_instance_id TEXT REFERENCES agent_instances(instance_id) ON DELETE SET NULL,
    producer_worker_run_id TEXT REFERENCES worker_runs(worker_run_id) ON DELETE SET NULL,
    kind TEXT NOT NULL CHECK(kind IN ('implementation_report', 'api_contract', 'answer', 'audit_report', 'note')),
    title TEXT NOT NULL,
    content_type TEXT NOT NULL,
    content TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS artifacts_project_created_idx
    ON artifacts(project_id, created_at);

CREATE TABLE IF NOT EXISTS messages (
    message_id TEXT PRIMARY KEY NOT NULL,
    project_id TEXT NOT NULL REFERENCES projects(project_id) ON DELETE CASCADE,
    sender_repository_id TEXT NOT NULL REFERENCES repositories(repository_id) ON DELETE CASCADE,
    sender_instance_id TEXT REFERENCES agent_instances(instance_id) ON DELETE SET NULL,
    sender_worker_run_id TEXT REFERENCES worker_runs(worker_run_id) ON DELETE SET NULL,
    kind TEXT NOT NULL CHECK(kind IN ('report', 'announcement', 'command')),
    thread_id TEXT NOT NULL,
    correlation_id TEXT,
    in_reply_to TEXT REFERENCES messages(message_id) ON DELETE SET NULL,
    summary TEXT NOT NULL,
    artifact_id TEXT REFERENCES artifacts(artifact_id) ON DELETE SET NULL,
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS messages_project_thread_idx
    ON messages(project_id, thread_id, created_at);

CREATE INDEX IF NOT EXISTS messages_correlation_idx
    ON messages(project_id, correlation_id);

CREATE TABLE IF NOT EXISTS message_targets (
    message_id TEXT NOT NULL REFERENCES messages(message_id) ON DELETE CASCADE,
    repository_id TEXT NOT NULL REFERENCES repositories(repository_id) ON DELETE CASCADE,
    PRIMARY KEY(message_id, repository_id)
);

CREATE TABLE IF NOT EXISTS deliveries (
    delivery_id INTEGER PRIMARY KEY AUTOINCREMENT,
    message_id TEXT NOT NULL REFERENCES messages(message_id) ON DELETE CASCADE,
    recipient_repository_id TEXT NOT NULL REFERENCES repositories(repository_id) ON DELETE CASCADE,
    status TEXT NOT NULL CHECK(status IN ('pending', 'acked')),
    created_at TEXT NOT NULL,
    acked_at TEXT,
    acked_by_instance_id TEXT REFERENCES agent_instances(instance_id) ON DELETE SET NULL,
    acked_by_worker_run_id TEXT REFERENCES worker_runs(worker_run_id) ON DELETE SET NULL,
    UNIQUE(message_id, recipient_repository_id)
);

CREATE INDEX IF NOT EXISTS deliveries_inbox_idx
    ON deliveries(recipient_repository_id, status, delivery_id);

CREATE TABLE IF NOT EXISTS requests (
    request_id TEXT PRIMARY KEY NOT NULL,
    project_id TEXT NOT NULL REFERENCES projects(project_id) ON DELETE CASCADE,
    requester_repository_id TEXT NOT NULL REFERENCES repositories(repository_id) ON DELETE CASCADE,
    target_repository_id TEXT NOT NULL REFERENCES repositories(repository_id) ON DELETE CASCADE,
    created_by_instance_id TEXT REFERENCES agent_instances(instance_id) ON DELETE SET NULL,
    created_by_worker_run_id TEXT REFERENCES worker_runs(worker_run_id) ON DELETE SET NULL,
    status TEXT NOT NULL CHECK(status IN ('pending', 'claimed', 'answered', 'failed')),
    question_summary TEXT NOT NULL,
    question_artifact_id TEXT REFERENCES artifacts(artifact_id) ON DELETE SET NULL,
    context_refs_json TEXT NOT NULL,
    claimed_by_instance_id TEXT REFERENCES agent_instances(instance_id) ON DELETE SET NULL,
    claimed_by_worker_run_id TEXT REFERENCES worker_runs(worker_run_id) ON DELETE SET NULL,
    claim_expires_at TEXT,
    failure_summary TEXT,
    failure_retryable INTEGER CHECK(failure_retryable IN (0, 1)),
    answer_message_id TEXT REFERENCES messages(message_id) ON DELETE SET NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    finished_at TEXT,
    CHECK(requester_repository_id <> target_repository_id),
    CHECK(NOT (claimed_by_instance_id IS NOT NULL AND claimed_by_worker_run_id IS NOT NULL))
);

CREATE INDEX IF NOT EXISTS requests_target_status_idx
    ON requests(target_repository_id, status, claim_expires_at, created_at);

CREATE INDEX IF NOT EXISTS requests_requester_status_idx
    ON requests(requester_repository_id, status, created_at);

CREATE TABLE IF NOT EXISTS events (
    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
    project_id TEXT NOT NULL REFERENCES projects(project_id) ON DELETE CASCADE,
    recipient_repository_id TEXT NOT NULL REFERENCES repositories(repository_id) ON DELETE CASCADE,
    kind TEXT NOT NULL CHECK(kind IN (
        'message_available',
        'worker_run_created',
        'worker_run_active',
        'worker_run_completed',
        'worker_run_failed',
        'request_available',
        'request_claimed',
        'request_answered',
        'request_failed'
    )),
    entity_type TEXT NOT NULL CHECK(entity_type IN ('message', 'worker_run', 'request')),
    entity_id TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS events_repository_cursor_idx
    ON events(recipient_repository_id, event_id);
"#;
