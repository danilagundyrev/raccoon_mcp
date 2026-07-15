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

CREATE INDEX IF NOT EXISTS agent_instances_expiry_idx
    ON agent_instances(status, lease_expires_at)
    WHERE status = 'online';

CREATE INDEX IF NOT EXISTS agent_instances_online_metrics_idx
    ON agent_instances(status, lease_expires_at, repository_id)
    WHERE status = 'online';

CREATE TABLE IF NOT EXISTS features (
    feature_id TEXT PRIMARY KEY NOT NULL,
    project_id TEXT NOT NULL REFERENCES projects(project_id) ON DELETE CASCADE,
    created_by_instance_id TEXT NOT NULL REFERENCES agent_instances(instance_id) ON DELETE RESTRICT,
    title TEXT NOT NULL,
    summary TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS features_project_created_idx
    ON features(project_id, created_at);

CREATE TABLE IF NOT EXISTS work_items (
    work_item_id TEXT PRIMARY KEY NOT NULL,
    feature_id TEXT NOT NULL REFERENCES features(feature_id) ON DELETE CASCADE,
    parent_work_item_id TEXT REFERENCES work_items(work_item_id) ON DELETE RESTRICT,
    owner_repository_id TEXT NOT NULL REFERENCES repositories(repository_id) ON DELETE RESTRICT,
    assignee_instance_id TEXT REFERENCES agent_instances(instance_id) ON DELETE SET NULL,
    title TEXT NOT NULL,
    summary TEXT NOT NULL,
    priority INTEGER NOT NULL,
    status TEXT NOT NULL CHECK(status IN (
        'planned', 'ready', 'claimed', 'in_progress',
        'blocked', 'completed', 'failed', 'cancelled'
    )),
    revision INTEGER NOT NULL CHECK(revision >= 1),
    max_audit_iterations INTEGER NOT NULL CHECK(max_audit_iterations BETWEEN 1 AND 10),
    claim_expires_at TEXT,
    implementation_artifact_id TEXT REFERENCES artifacts(artifact_id) ON DELETE SET NULL,
    latest_audit_artifact_id TEXT REFERENCES artifacts(artifact_id) ON DELETE SET NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    finished_at TEXT
);

CREATE INDEX IF NOT EXISTS work_items_feature_status_idx
    ON work_items(feature_id, status, priority, created_at);

CREATE INDEX IF NOT EXISTS work_items_owner_status_idx
    ON work_items(owner_repository_id, status, priority, created_at);

CREATE INDEX IF NOT EXISTS work_items_active_claim_metrics_idx
    ON work_items(status, claim_expires_at)
    WHERE status IN ('claimed', 'in_progress', 'blocked');

CREATE INDEX IF NOT EXISTS work_items_claim_owner_idx
    ON work_items(assignee_instance_id, claim_expires_at)
    WHERE status IN ('claimed', 'in_progress', 'blocked')
      AND assignee_instance_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS work_item_dependencies (
    work_item_id TEXT NOT NULL REFERENCES work_items(work_item_id) ON DELETE CASCADE,
    dependency_work_item_id TEXT NOT NULL REFERENCES work_items(work_item_id) ON DELETE RESTRICT,
    PRIMARY KEY(work_item_id, dependency_work_item_id),
    CHECK(work_item_id <> dependency_work_item_id)
);

CREATE INDEX IF NOT EXISTS work_item_dependencies_reverse_idx
    ON work_item_dependencies(dependency_work_item_id, work_item_id);

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
    work_item_id TEXT REFERENCES work_items(work_item_id) ON DELETE RESTRICT,
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

CREATE INDEX IF NOT EXISTS worker_runs_expiry_idx
    ON worker_runs(status, lease_expires_at)
    WHERE status IN ('created', 'active');

CREATE INDEX IF NOT EXISTS worker_runs_active_metrics_idx
    ON worker_runs(status, lease_expires_at)
    WHERE status = 'active';

CREATE UNIQUE INDEX IF NOT EXISTS worker_runs_one_remediation_per_audit_idx
    ON worker_runs(addresses_audit_run_id)
    WHERE addresses_audit_run_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS worker_run_audit_items (
    worker_run_id TEXT NOT NULL REFERENCES worker_runs(worker_run_id) ON DELETE CASCADE,
    work_item_id TEXT NOT NULL REFERENCES work_items(work_item_id) ON DELETE RESTRICT,
    audit_iteration INTEGER NOT NULL CHECK(audit_iteration BETWEEN 1 AND 10),
    max_iterations INTEGER NOT NULL CHECK(max_iterations BETWEEN 1 AND 10),
    PRIMARY KEY(worker_run_id, work_item_id),
    UNIQUE(work_item_id, audit_iteration)
);

CREATE INDEX IF NOT EXISTS worker_run_audit_items_work_item_idx
    ON worker_run_audit_items(work_item_id, audit_iteration);

CREATE TRIGGER IF NOT EXISTS worker_run_audit_items_same_project
BEFORE INSERT ON worker_run_audit_items
WHEN (
    SELECT repositories.project_id
    FROM worker_runs
    JOIN repositories ON repositories.repository_id = worker_runs.repository_id
    WHERE worker_runs.worker_run_id = NEW.worker_run_id
) <> (
    SELECT features.project_id
    FROM work_items
    JOIN features ON features.feature_id = work_items.feature_id
    WHERE work_items.work_item_id = NEW.work_item_id
)
BEGIN
    SELECT RAISE(ABORT, 'audit worker and work item must belong to the same project');
END;

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

CREATE INDEX IF NOT EXISTS artifacts_producer_kind_idx
    ON artifacts(producer_worker_run_id, kind)
    WHERE producer_worker_run_id IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS artifacts_one_audit_report_per_worker_idx
    ON artifacts(producer_worker_run_id)
    WHERE kind = 'audit_report' AND producer_worker_run_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS messages (
    message_id TEXT PRIMARY KEY NOT NULL,
    project_id TEXT NOT NULL REFERENCES projects(project_id) ON DELETE CASCADE,
    sender_repository_id TEXT NOT NULL REFERENCES repositories(repository_id) ON DELETE CASCADE,
    sender_instance_id TEXT REFERENCES agent_instances(instance_id) ON DELETE SET NULL,
    sender_worker_run_id TEXT REFERENCES worker_runs(worker_run_id) ON DELETE SET NULL,
    kind TEXT NOT NULL CHECK(kind IN ('report', 'announcement', 'command')),
    feature_id TEXT REFERENCES features(feature_id) ON DELETE SET NULL,
    work_item_id TEXT REFERENCES work_items(work_item_id) ON DELETE SET NULL,
    request_id TEXT REFERENCES requests(request_id) ON DELETE SET NULL,
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

CREATE INDEX IF NOT EXISTS messages_feature_idx
    ON messages(feature_id, created_at)
    WHERE feature_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS messages_work_item_idx
    ON messages(work_item_id, created_at)
    WHERE work_item_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS messages_request_idx
    ON messages(request_id, created_at)
    WHERE request_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS messages_artifact_idx
    ON messages(artifact_id)
    WHERE artifact_id IS NOT NULL;

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

CREATE INDEX IF NOT EXISTS deliveries_pending_metrics_idx
    ON deliveries(created_at)
    WHERE status = 'pending';

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

CREATE INDEX IF NOT EXISTS requests_claim_expiry_idx
    ON requests(status, claim_expires_at)
    WHERE status = 'claimed';

CREATE INDEX IF NOT EXISTS requests_claimed_instance_idx
    ON requests(claimed_by_instance_id, claim_expires_at)
    WHERE status = 'claimed' AND claimed_by_instance_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS requests_claimed_worker_idx
    ON requests(claimed_by_worker_run_id, claim_expires_at)
    WHERE status = 'claimed' AND claimed_by_worker_run_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS requests_question_artifact_idx
    ON requests(question_artifact_id)
    WHERE question_artifact_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS requests_open_metrics_idx
    ON requests(status, claim_expires_at)
    WHERE status IN ('pending', 'claimed');

CREATE TABLE IF NOT EXISTS idempotency_records (
    actor_key TEXT NOT NULL,
    operation TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    request_digest TEXT NOT NULL,
    result_entity_id TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY(actor_key, operation, idempotency_key)
);

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
        'request_failed',
        'feature_created',
        'work_item_created',
        'work_item_updated'
    )),
    entity_type TEXT NOT NULL CHECK(entity_type IN (
        'message', 'worker_run', 'request', 'feature', 'work_item'
    )),
    entity_id TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS events_repository_cursor_idx
    ON events(recipient_repository_id, event_id);

CREATE INDEX IF NOT EXISTS events_repository_kind_cursor_idx
    ON events(recipient_repository_id, kind, event_id);

CREATE INDEX IF NOT EXISTS events_retention_idx
    ON events(created_at, event_id);

CREATE INDEX IF NOT EXISTS idempotency_records_retention_idx
    ON idempotency_records(created_at);
"#;
