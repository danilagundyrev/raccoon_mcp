use std::{
    collections::{BTreeSet, HashMap},
    path::Path,
    str::FromStr,
    sync::{Arc, Mutex as StdMutex},
    time::Duration,
};

use chrono::{SecondsFormat, Utc};
use sha2::{Digest, Sha256};
use sqlx::{
    QueryBuilder, Row, Sqlite, SqlitePool, Transaction,
    sqlite::{
        SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteRow, SqliteSynchronous,
    },
};
use thiserror::Error;
use tokio::{
    sync::{Mutex, Notify},
    time::{Instant, interval_at, sleep_until, timeout_at},
};

use crate::domain::{
    ActorRef, AgentInstance, AnswerRequest, AnswerRequestResult, AnswerRequirement, Artifact,
    ArtifactId, ArtifactKind, ArtifactListFilter, ArtifactMetadata, ArtifactPage, AskRepository,
    AuditOutcomeInput, AuditScopeItem, AuditVerdict, CancelFeature, CompleteFeature,
    ConnectOrchestrator, ConnectResult, CoordinationRequest, CreateFeature, CreateWorkItem,
    CreateWorkerRun, DeliveryAck, DeliveryHistoryItem, DeliveryListStatus, DeliveryPage,
    EventBatch, EventCheckpoint, EventEntityType, EventKind, FailRequest, Feature, FeatureId,
    FeaturePage, FeatureStatus, HandlerRequirement, HubEvent, InboxItem, InboxPage,
    IncreaseAuditDepth, InstanceId, LeaseMaintenanceReport, MAX_AUDIT_WORK_ITEMS, MAX_CONTEXT_REFS,
    MAX_MESSAGE_RECIPIENTS, MAX_WORK_ITEM_DEPENDENCIES, Message, MessageId, MessageKind,
    MessageListFilter, MessagePage, MessageTarget, OperationalMetrics, PresenceStatus, Project,
    ProjectId, ProjectOverview, PublishMessage, PublishResult, Repository, RepositoryId,
    RepositoryPresence, RepositoryPresenceFilter, RequestId, RequestListFilter, RequestPage,
    RequestStatus, RequestSummary, RetentionReport, ThreadId, UpdateWorkItem, VerificationStatus,
    WaitOutcome, WorkItem, WorkItemId, WorkItemKind, WorkItemPage, WorkItemStatus,
    WorkItemWorkerProjection, WorkerRole, WorkerRun, WorkerRunId, WorkerRunListFilter,
    WorkerRunPage, WorkerRunStatus, WorkerRunSummary,
};

use super::schema::{CREATE_SCHEMA, SCHEMA_VERSION};

mod events;
mod history;
mod messages;
mod operations;
mod presence;
mod requests;
mod work;
mod workers;

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("not found: {0}")]
    NotFound(String),
    #[error("conflict: {0}")]
    Conflict(String),
    #[error("work item {work_item_id} revision conflict: expected {expected}, actual {actual}")]
    RevisionConflict {
        work_item_id: WorkItemId,
        expected: u64,
        actual: u64,
        current: Box<WorkItem>,
    },
    #[error("feature {feature_id} revision conflict: expected {expected}, actual {actual}")]
    FeatureRevisionConflict {
        feature_id: FeatureId,
        expected: u64,
        actual: u64,
        current: Box<Feature>,
    },
    #[error("claim conflict for {resource_type} {resource_id}: {message}")]
    ClaimConflict {
        resource_type: &'static str,
        resource_id: String,
        message: String,
        retry_at: Option<String>,
        retry_after_seconds: Option<u64>,
    },
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),
    #[error("filesystem error: {0}")]
    Io(#[from] std::io::Error),
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("stored identifier is invalid: {0}")]
    InvalidUuid(#[from] uuid::Error),
}

#[derive(Clone)]
pub struct SqliteStore {
    pool: SqlitePool,
    lease_duration: chrono::Duration,
    worker_lease_duration: chrono::Duration,
    request_claim_lease_duration: chrono::Duration,
    work_item_claim_lease_duration: chrono::Duration,
    write_lock: Arc<Mutex<()>>,
    notifications: Arc<NotificationHub>,
}

pub struct EventWaitOptions<'a> {
    pub cursor: Option<i64>,
    pub consumer_key: Option<&'a str>,
    pub kinds: &'a [EventKind],
    pub feature_id: Option<FeatureId>,
    pub owner_repository_id: Option<RepositoryId>,
    pub timeout: Duration,
    pub limit: u8,
}

#[derive(Default)]
struct NotificationHub {
    by_repository: StdMutex<HashMap<RepositoryId, Arc<Notify>>>,
}

impl NotificationHub {
    fn for_repository(&self, repository_id: RepositoryId) -> Arc<Notify> {
        self.by_repository
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .entry(repository_id)
            .or_insert_with(|| Arc::new(Notify::new()))
            .clone()
    }

    fn notify(&self, repository_id: RepositoryId) {
        self.for_repository(repository_id).notify_waiters();
    }
}

struct ResolvedActor {
    project_id: ProjectId,
    repository_id: RepositoryId,
    instance_id: Option<InstanceId>,
    worker_run_id: Option<WorkerRunId>,
}

#[derive(Default)]
struct EventMetadata {
    feature_id: Option<FeatureId>,
    owner_repository_id: Option<RepositoryId>,
    entity_status: Option<String>,
    entity_revision: Option<u64>,
    work_item_id: Option<WorkItemId>,
    request_id: Option<RequestId>,
}

struct NewEvent<'a> {
    project_id: ProjectId,
    recipient_repository_id: RepositoryId,
    kind: EventKind,
    entity_type: EventEntityType,
    entity_id: &'a str,
    created_at: &'a str,
}

impl SqliteStore {
    pub async fn open(
        database_path: &Path,
        max_connections: u32,
        lease_seconds: u16,
        worker_lease_seconds: u16,
        request_claim_lease_seconds: u16,
        work_item_claim_lease_seconds: u16,
    ) -> Result<Self, StoreError> {
        if let Some(parent) = database_path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent)?;
            }
        }

        let options = SqliteConnectOptions::from_str(&format!(
            "sqlite://{}",
            database_path.to_string_lossy().replace('\\', "/")
        ))?
        .create_if_missing(true)
        .foreign_keys(true)
        .journal_mode(SqliteJournalMode::Wal)
        .synchronous(SqliteSynchronous::Normal)
        .busy_timeout(Duration::from_secs(5));

        let pool = SqlitePoolOptions::new()
            .max_connections(max_connections)
            .acquire_timeout(Duration::from_secs(5))
            .connect_with(options)
            .await?;

        sqlx::raw_sql(CREATE_SCHEMA).execute(&pool).await?;
        ensure_schema_version(&pool).await?;

        Ok(Self {
            pool,
            lease_duration: chrono::Duration::seconds(lease_seconds as i64),
            worker_lease_duration: chrono::Duration::seconds(worker_lease_seconds as i64),
            request_claim_lease_duration: chrono::Duration::seconds(
                request_claim_lease_seconds as i64,
            ),
            work_item_claim_lease_duration: chrono::Duration::seconds(
                work_item_claim_lease_seconds as i64,
            ),
            write_lock: Arc::new(Mutex::new(())),
            notifications: Arc::new(NotificationHub::default()),
        })
    }

    pub async fn health_check(&self) -> Result<(), StoreError> {
        let _write_guard = self.write_lock.lock().await;
        let mut transaction = self.pool.begin().await?;
        sqlx::query(
            "INSERT INTO schema_meta(key, value) VALUES ('readiness_probe', 'ok')
             ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        )
        .execute(&mut *transaction)
        .await?;
        transaction.rollback().await?;
        Ok(())
    }

    async fn resolve_actor(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        actor_ref: ActorRef,
        now: &str,
    ) -> Result<ResolvedActor, StoreError> {
        match actor_ref {
            ActorRef::Orchestrator(instance_id) => {
                let row = sqlx::query(
                    "SELECT i.status, i.repository_id, r.project_id
                     FROM agent_instances i
                     JOIN repositories r ON r.repository_id = i.repository_id
                     WHERE i.instance_id = ?",
                )
                .bind(instance_id.to_string())
                .fetch_optional(&mut **transaction)
                .await?
                .ok_or_else(|| StoreError::NotFound(format!("instance {instance_id}")))?;
                let status: String = row.try_get("status")?;
                if status == "disconnected" {
                    return Err(StoreError::Conflict(format!(
                        "instance {instance_id} is disconnected"
                    )));
                }
                sqlx::query(
                    "UPDATE agent_instances
                     SET status = 'online', last_heartbeat_at = ?, lease_expires_at = ?
                     WHERE instance_id = ?",
                )
                .bind(now)
                .bind(lease_deadline(self.lease_duration))
                .bind(instance_id.to_string())
                .execute(&mut **transaction)
                .await?;
                let actor = ResolvedActor {
                    project_id: ProjectId::parse(row.try_get("project_id")?)?,
                    repository_id: RepositoryId::parse(row.try_get("repository_id")?)?,
                    instance_id: Some(instance_id),
                    worker_run_id: None,
                };
                renew_request_claims_for_actor(
                    transaction,
                    &actor,
                    self.request_claim_lease_duration,
                    now,
                )
                .await?;
                renew_work_item_claims_for_owner(
                    transaction,
                    instance_id,
                    self.work_item_claim_lease_duration,
                    now,
                )
                .await?;
                trace_resolved_actor(&actor);
                Ok(actor)
            }
            ActorRef::Worker(worker_run_id) => {
                let row = sqlx::query(
                    "SELECT w.status, w.lease_expires_at, w.repository_id, r.project_id
                     FROM worker_runs w
                     JOIN repositories r ON r.repository_id = w.repository_id
                     WHERE w.worker_run_id = ?",
                )
                .bind(worker_run_id.to_string())
                .fetch_optional(&mut **transaction)
                .await?
                .ok_or_else(|| StoreError::NotFound(format!("worker run {worker_run_id}")))?;
                let status: String = row.try_get("status")?;
                if status != "active" {
                    return Err(StoreError::Conflict(format!(
                        "worker run {worker_run_id} is '{status}', expected 'active'"
                    )));
                }
                let lease_expires_at: String = row.try_get("lease_expires_at")?;
                if lease_expires_at.as_str() <= now {
                    return Err(expired_worker_lease(worker_run_id));
                }
                sqlx::query(
                    "UPDATE worker_runs
                     SET last_heartbeat_at = ?, lease_expires_at = ?
                     WHERE worker_run_id = ? AND status = 'active'",
                )
                .bind(now)
                .bind(lease_deadline(self.worker_lease_duration))
                .bind(worker_run_id.to_string())
                .execute(&mut **transaction)
                .await?;
                let actor = ResolvedActor {
                    project_id: ProjectId::parse(row.try_get("project_id")?)?,
                    repository_id: RepositoryId::parse(row.try_get("repository_id")?)?,
                    instance_id: None,
                    worker_run_id: Some(worker_run_id),
                };
                renew_request_claims_for_actor(
                    transaction,
                    &actor,
                    self.request_claim_lease_duration,
                    now,
                )
                .await?;
                trace_resolved_actor(&actor);
                Ok(actor)
            }
        }
    }

    async fn resolve_actor_readonly(
        &self,
        actor_ref: ActorRef,
        now: &str,
    ) -> Result<ResolvedActor, StoreError> {
        match actor_ref {
            ActorRef::Orchestrator(instance_id) => {
                let row = sqlx::query(
                    "SELECT i.status, i.repository_id, r.project_id
                     FROM agent_instances i
                     JOIN repositories r ON r.repository_id = i.repository_id
                     WHERE i.instance_id = ?",
                )
                .bind(instance_id.to_string())
                .fetch_optional(&self.pool)
                .await?
                .ok_or_else(|| StoreError::NotFound(format!("instance {instance_id}")))?;
                let status: String = row.try_get("status")?;
                if status == "disconnected" {
                    return Err(StoreError::Conflict(format!(
                        "instance {instance_id} is disconnected"
                    )));
                }
                let actor = ResolvedActor {
                    project_id: ProjectId::parse(row.try_get("project_id")?)?,
                    repository_id: RepositoryId::parse(row.try_get("repository_id")?)?,
                    instance_id: Some(instance_id),
                    worker_run_id: None,
                };
                trace_resolved_actor(&actor);
                Ok(actor)
            }
            ActorRef::Worker(worker_run_id) => {
                let row = sqlx::query(
                    "SELECT w.status, w.lease_expires_at, w.repository_id, r.project_id
                     FROM worker_runs w
                     JOIN repositories r ON r.repository_id = w.repository_id
                     WHERE w.worker_run_id = ?",
                )
                .bind(worker_run_id.to_string())
                .fetch_optional(&self.pool)
                .await?
                .ok_or_else(|| StoreError::NotFound(format!("worker run {worker_run_id}")))?;
                let status: String = row.try_get("status")?;
                if status != "active" {
                    return Err(StoreError::Conflict(format!(
                        "worker run {worker_run_id} is '{status}', expected 'active'"
                    )));
                }
                let lease_expires_at: String = row.try_get("lease_expires_at")?;
                if lease_expires_at.as_str() <= now {
                    return Err(expired_worker_lease(worker_run_id));
                }
                let actor = ResolvedActor {
                    project_id: ProjectId::parse(row.try_get("project_id")?)?,
                    repository_id: RepositoryId::parse(row.try_get("repository_id")?)?,
                    instance_id: None,
                    worker_run_id: Some(worker_run_id),
                };
                trace_resolved_actor(&actor);
                Ok(actor)
            }
        }
    }
}

fn trace_resolved_actor(actor: &ResolvedActor) {
    trace_resolved_scope(actor.project_id, actor.repository_id);
    tracing::debug!(
        project_id = %actor.project_id,
        repository_id = %actor.repository_id,
        instance_id = actor.instance_id.map(|id| id.to_string()).unwrap_or_default(),
        worker_run_id = actor.worker_run_id.map(|id| id.to_string()).unwrap_or_default(),
        "actor scope resolved"
    );
}

fn trace_resolved_scope(project_id: ProjectId, repository_id: RepositoryId) {
    let span = tracing::Span::current();
    span.record("project_id", tracing::field::display(project_id));
    span.record("repository_id", tracing::field::display(repository_id));
}

fn validate_history_limit(limit: u8) -> Result<(), StoreError> {
    if (1..=50).contains(&limit) {
        Ok(())
    } else {
        Err(StoreError::Conflict(
            "history page limit must be between 1 and 50".to_owned(),
        ))
    }
}

fn push_artifact_access_predicate<'a>(
    query: &mut QueryBuilder<'a, Sqlite>,
    repository_id: RepositoryId,
    allow_project_shared: bool,
) {
    query
        .push("(a.producer_repository_id = ")
        .push_bind(repository_id.to_string())
        .push(
            " OR EXISTS (
                SELECT 1 FROM messages access_message JOIN deliveries access_delivery
                  ON access_delivery.message_id = access_message.message_id
                WHERE access_message.artifact_id = a.artifact_id
                  AND access_message.project_id = a.project_id
                  AND access_delivery.recipient_repository_id = ",
        )
        .push_bind(repository_id.to_string())
        .push(
            ") OR EXISTS (SELECT 1 FROM requests access_request
                WHERE access_request.question_artifact_id = a.artifact_id
                  AND access_request.project_id = a.project_id
                  AND (access_request.requester_repository_id = ",
        )
        .push_bind(repository_id.to_string())
        .push(" OR access_request.target_repository_id = ")
        .push_bind(repository_id.to_string())
        .push("))");
    if allow_project_shared {
        query.push(
            " OR EXISTS (
                SELECT 1 FROM work_items shared_implementation
                JOIN features shared_feature
                  ON shared_feature.feature_id = shared_implementation.feature_id
                WHERE shared_implementation.implementation_artifact_id = a.artifact_id
                  AND shared_feature.project_id = a.project_id
            ) OR EXISTS (
                SELECT 1 FROM audit_outcomes shared_audit
                JOIN work_items shared_audit_item
                  ON shared_audit_item.work_item_id = shared_audit.audit_work_item_id
                JOIN features shared_audit_feature
                  ON shared_audit_feature.feature_id = shared_audit_item.feature_id
                WHERE shared_audit.audit_artifact_id = a.artifact_id
                  AND shared_audit_feature.project_id = a.project_id
            ) OR EXISTS (
                SELECT 1 FROM messages shared_message
                WHERE shared_message.artifact_id = a.artifact_id
                  AND shared_message.project_id = a.project_id
                  AND shared_message.request_id IS NULL
                  AND (shared_message.feature_id IS NOT NULL
                       OR shared_message.work_item_id IS NOT NULL)
            )",
        );
    }
    query.push(")");
}

async fn wait_until(deadline: Instant) {
    sleep_until(deadline).await;
}

async fn repository_project_id(
    transaction: &mut Transaction<'_, Sqlite>,
    repository_id: RepositoryId,
) -> Result<ProjectId, StoreError> {
    let row = sqlx::query("SELECT project_id FROM repositories WHERE repository_id = ?")
        .bind(repository_id.to_string())
        .fetch_one(&mut **transaction)
        .await?;
    Ok(ProjectId::parse(row.try_get("project_id")?)?)
}

async fn append_event(
    transaction: &mut Transaction<'_, Sqlite>,
    project_id: ProjectId,
    recipient_repository_id: RepositoryId,
    kind: EventKind,
    entity_type: EventEntityType,
    entity_id: &str,
    created_at: &str,
) -> Result<(), StoreError> {
    let metadata = load_event_metadata(transaction, entity_type, entity_id).await?;
    insert_event(
        transaction,
        NewEvent {
            project_id,
            recipient_repository_id,
            kind,
            entity_type,
            entity_id,
            created_at,
        },
        &metadata,
    )
    .await
}

async fn append_routed_event(
    transaction: &mut Transaction<'_, Sqlite>,
    project_id: ProjectId,
    kind: EventKind,
    entity_type: EventEntityType,
    entity_id: &str,
    created_at: &str,
) -> Result<Vec<RepositoryId>, StoreError> {
    let metadata = load_event_metadata(transaction, entity_type, entity_id).await?;
    let recipients =
        routed_event_recipients(transaction, project_id, entity_type, entity_id, &metadata).await?;
    for recipient_repository_id in &recipients {
        insert_event(
            transaction,
            NewEvent {
                project_id,
                recipient_repository_id: *recipient_repository_id,
                kind,
                entity_type,
                entity_id,
                created_at,
            },
            &metadata,
        )
        .await?;
    }
    Ok(recipients)
}

async fn routed_event_recipients(
    transaction: &mut Transaction<'_, Sqlite>,
    project_id: ProjectId,
    entity_type: EventEntityType,
    entity_id: &str,
    metadata: &EventMetadata,
) -> Result<Vec<RepositoryId>, StoreError> {
    let mut recipients = Vec::new();
    match entity_type {
        EventEntityType::Feature => {
            let rows = sqlx::query(
                "SELECT repository_id FROM repositories
                 WHERE project_id = ? ORDER BY repository_id",
            )
            .bind(project_id.to_string())
            .fetch_all(&mut **transaction)
            .await?;
            for row in rows {
                push_event_recipient(
                    &mut recipients,
                    RepositoryId::parse(row.try_get("repository_id")?)?,
                );
            }
        }
        EventEntityType::WorkItem => {
            if let Some(owner_repository_id) = metadata.owner_repository_id {
                push_event_recipient(&mut recipients, owner_repository_id);
            }
            append_feature_coordinator(
                transaction,
                project_id,
                metadata.feature_id,
                &mut recipients,
            )
            .await?;
        }
        EventEntityType::WorkerRun => {
            if metadata.work_item_id.is_some() {
                if let Some(owner_repository_id) = metadata.owner_repository_id {
                    push_event_recipient(&mut recipients, owner_repository_id);
                }
                append_feature_coordinator(
                    transaction,
                    project_id,
                    metadata.feature_id,
                    &mut recipients,
                )
                .await?;
            }
            if let Some(request_id) = metadata.request_id {
                append_request_participants(transaction, project_id, request_id, &mut recipients)
                    .await?;
            }
            if metadata.work_item_id.is_none()
                && metadata.request_id.is_none()
                && let Some(owner_repository_id) = metadata.owner_repository_id
            {
                push_event_recipient(&mut recipients, owner_repository_id);
            }
        }
        EventEntityType::Request => {
            append_request_participants(
                transaction,
                project_id,
                RequestId::parse(entity_id)?,
                &mut recipients,
            )
            .await?;
        }
        EventEntityType::Message => {
            return Err(StoreError::Conflict(
                "message events require explicit delivery recipients".to_owned(),
            ));
        }
    }
    Ok(recipients)
}

async fn append_feature_coordinator(
    transaction: &mut Transaction<'_, Sqlite>,
    project_id: ProjectId,
    feature_id: Option<FeatureId>,
    recipients: &mut Vec<RepositoryId>,
) -> Result<(), StoreError> {
    let Some(feature_id) = feature_id else {
        return Ok(());
    };
    let coordinator_repository_id: String = sqlx::query_scalar(
        "SELECT coordinator_repository_id FROM features
         WHERE feature_id = ? AND project_id = ?",
    )
    .bind(feature_id.to_string())
    .bind(project_id.to_string())
    .fetch_one(&mut **transaction)
    .await?;
    push_event_recipient(recipients, RepositoryId::parse(&coordinator_repository_id)?);
    Ok(())
}

async fn append_request_participants(
    transaction: &mut Transaction<'_, Sqlite>,
    project_id: ProjectId,
    request_id: RequestId,
    recipients: &mut Vec<RepositoryId>,
) -> Result<(), StoreError> {
    let row = sqlx::query(
        "SELECT requester_repository_id, target_repository_id FROM requests
         WHERE request_id = ? AND project_id = ?",
    )
    .bind(request_id.to_string())
    .bind(project_id.to_string())
    .fetch_one(&mut **transaction)
    .await?;
    push_event_recipient(
        recipients,
        RepositoryId::parse(row.try_get("requester_repository_id")?)?,
    );
    push_event_recipient(
        recipients,
        RepositoryId::parse(row.try_get("target_repository_id")?)?,
    );
    Ok(())
}

fn push_event_recipient(recipients: &mut Vec<RepositoryId>, repository_id: RepositoryId) {
    if !recipients.contains(&repository_id) {
        recipients.push(repository_id);
    }
}

async fn insert_event(
    transaction: &mut Transaction<'_, Sqlite>,
    event: NewEvent<'_>,
    metadata: &EventMetadata,
) -> Result<(), StoreError> {
    sqlx::query(
        "INSERT INTO events(
             project_id, recipient_repository_id, kind, entity_type, entity_id,
             feature_id, owner_repository_id, entity_status, entity_revision,
             work_item_id, request_id, created_at
         ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(event.project_id.to_string())
    .bind(event.recipient_repository_id.to_string())
    .bind(event.kind.as_str())
    .bind(event.entity_type.as_str())
    .bind(event.entity_id)
    .bind(metadata.feature_id.map(|id| id.to_string()))
    .bind(metadata.owner_repository_id.map(|id| id.to_string()))
    .bind(&metadata.entity_status)
    .bind(metadata.entity_revision.map(|revision| revision as i64))
    .bind(metadata.work_item_id.map(|id| id.to_string()))
    .bind(metadata.request_id.map(|id| id.to_string()))
    .bind(event.created_at)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

async fn load_event_metadata(
    transaction: &mut Transaction<'_, Sqlite>,
    entity_type: EventEntityType,
    entity_id: &str,
) -> Result<EventMetadata, StoreError> {
    let row = match entity_type {
        EventEntityType::Feature => {
            sqlx::query(
                "SELECT feature_id, coordinator_repository_id AS owner_repository_id,
                    status AS entity_status, revision AS entity_revision,
                    NULL AS work_item_id, NULL AS request_id
             FROM features WHERE feature_id = ?",
            )
            .bind(entity_id)
            .fetch_optional(&mut **transaction)
            .await?
        }
        EventEntityType::WorkItem => {
            sqlx::query(
                "SELECT feature_id, owner_repository_id, status AS entity_status,
                    revision AS entity_revision, work_item_id, NULL AS request_id
             FROM work_items WHERE work_item_id = ?",
            )
            .bind(entity_id)
            .fetch_optional(&mut **transaction)
            .await?
        }
        EventEntityType::WorkerRun => {
            sqlx::query(
                "SELECT COALESCE(item.feature_id, request.feature_id) AS feature_id,
                    worker.repository_id AS owner_repository_id,
                    worker.status AS entity_status, NULL AS entity_revision,
                    worker.work_item_id, worker.request_id
             FROM worker_runs worker
             LEFT JOIN work_items item ON item.work_item_id = worker.work_item_id
             LEFT JOIN requests request ON request.request_id = worker.request_id
             WHERE worker.worker_run_id = ?",
            )
            .bind(entity_id)
            .fetch_optional(&mut **transaction)
            .await?
        }
        EventEntityType::Request => {
            sqlx::query(
                "SELECT feature_id, target_repository_id AS owner_repository_id,
                    status AS entity_status, NULL AS entity_revision,
                    work_item_id, request_id
             FROM requests WHERE request_id = ?",
            )
            .bind(entity_id)
            .fetch_optional(&mut **transaction)
            .await?
        }
        EventEntityType::Message => {
            sqlx::query(
                "SELECT feature_id, NULL AS owner_repository_id,
                    NULL AS entity_status, NULL AS entity_revision,
                    work_item_id, request_id
             FROM messages WHERE message_id = ?",
            )
            .bind(entity_id)
            .fetch_optional(&mut **transaction)
            .await?
        }
    }
    .ok_or_else(|| {
        StoreError::NotFound(format!("{} event entity {entity_id}", entity_type.as_str()))
    })?;
    event_metadata_from_row(&row)
}

fn event_metadata_from_row(row: &SqliteRow) -> Result<EventMetadata, StoreError> {
    let revision = row
        .try_get::<Option<i64>, _>("entity_revision")?
        .map(|revision| {
            u64::try_from(revision).map_err(|_| {
                StoreError::Conflict(format!("stored event revision {revision} is invalid"))
            })
        })
        .transpose()?;
    Ok(EventMetadata {
        feature_id: parse_optional_id(row.try_get("feature_id")?, FeatureId::parse)?,
        owner_repository_id: parse_optional_id(
            row.try_get("owner_repository_id")?,
            RepositoryId::parse,
        )?,
        entity_status: row.try_get("entity_status")?,
        entity_revision: revision,
        work_item_id: parse_optional_id(row.try_get("work_item_id")?, WorkItemId::parse)?,
        request_id: parse_optional_id(row.try_get("request_id")?, RequestId::parse)?,
    })
}

fn resolved_actor_key(actor: &ResolvedActor) -> String {
    match (actor.instance_id, actor.worker_run_id) {
        (Some(instance_id), None) => format!("orchestrator:{instance_id}"),
        (None, Some(worker_run_id)) => format!("worker:{worker_run_id}"),
        _ => unreachable!("resolved actor has exactly one identity"),
    }
}

async fn find_idempotent_result(
    transaction: &mut Transaction<'_, Sqlite>,
    actor_key: &str,
    operation: &str,
    idempotency_key: Option<&str>,
    request_digest: &str,
) -> Result<Option<String>, StoreError> {
    let Some(idempotency_key) = idempotency_key else {
        return Ok(None);
    };
    validate_idempotency_key(idempotency_key)?;
    let row = sqlx::query(
        "SELECT request_digest, result_entity_id
         FROM idempotency_records
         WHERE actor_key = ? AND operation = ? AND idempotency_key = ?",
    )
    .bind(actor_key)
    .bind(operation)
    .bind(idempotency_key)
    .fetch_optional(&mut **transaction)
    .await?;
    let Some(row) = row else {
        return Ok(None);
    };
    let stored_digest: String = row.try_get("request_digest")?;
    if stored_digest != request_digest {
        return Err(StoreError::Conflict(format!(
            "idempotency key '{idempotency_key}' was already used for a different {operation} payload"
        )));
    }
    Ok(Some(row.try_get("result_entity_id")?))
}

async fn record_idempotent_result(
    transaction: &mut Transaction<'_, Sqlite>,
    actor_key: &str,
    operation: &str,
    idempotency_key: Option<&str>,
    request_digest: &str,
    result_entity_id: &str,
    created_at: &str,
) -> Result<(), StoreError> {
    let Some(idempotency_key) = idempotency_key else {
        return Ok(());
    };
    validate_idempotency_key(idempotency_key)?;
    sqlx::query(
        "INSERT INTO idempotency_records(
             actor_key, operation, idempotency_key, request_digest,
             result_entity_id, created_at
         ) VALUES (?, ?, ?, ?, ?, ?)",
    )
    .bind(actor_key)
    .bind(operation)
    .bind(idempotency_key)
    .bind(request_digest)
    .bind(result_entity_id)
    .bind(created_at)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

fn validate_idempotency_key(value: &str) -> Result<(), StoreError> {
    if value.is_empty() || value.len() > 128 {
        return Err(StoreError::Conflict(
            "idempotency key must contain between 1 and 128 UTF-8 bytes".to_owned(),
        ));
    }
    Ok(())
}

fn worker_run_fingerprint(command: &CreateWorkerRun) -> Result<String, StoreError> {
    payload_digest(&serde_json::json!({
        "role": command.role.as_str(),
        "task_summary": command.task_summary,
        "context_refs": command.context_refs,
        "addresses_audit_run_id": command.addresses_audit_run_id,
        "work_item_id": command.work_item_id,
        "request_id": command.request_id,
    }))
}

fn publish_message_fingerprint(
    command: &PublishMessage,
    audit_outcomes: &[AuditOutcomeInput],
) -> Result<String, StoreError> {
    let target = match &command.target {
        MessageTarget::Repositories(repository_ids) => serde_json::json!({
            "repositories": repository_ids,
        }),
        MessageTarget::Broadcast => serde_json::json!({ "broadcast": true }),
    };
    payload_digest(&serde_json::json!({
        "kind": command.kind.as_str(),
        "target": target,
        "summary": command.summary,
        "body": command.body,
        "artifact_kind": command.artifact_kind.as_str(),
        "artifact_title": command.artifact_title,
        "content_type": command.content_type,
        "feature_id": command.feature_id,
        "work_item_id": command.work_item_id,
        "request_id": command.request_id,
        "thread_id": command.thread_id,
        "correlation_id": command.correlation_id,
        "in_reply_to": command.in_reply_to,
        "audit_outcomes": audit_outcomes,
    }))
}

fn ask_repository_fingerprint(command: &AskRepository) -> Result<String, StoreError> {
    payload_digest(&serde_json::json!({
        "target_repository_id": command.target_repository_id,
        "feature_id": command.feature_id,
        "work_item_id": command.work_item_id,
        "question_summary": command.question_summary,
        "body": command.body,
        "context_refs": command.context_refs,
        "handler_requirement": command.handler_requirement,
        "answer_requirement": command.answer_requirement,
    }))
}

fn payload_digest(payload: &serde_json::Value) -> Result<String, StoreError> {
    let encoded = serde_json::to_vec(payload)?;
    Ok(format!("{:x}", Sha256::digest(encoded)))
}

async fn insert_delivery(
    transaction: &mut Transaction<'_, Sqlite>,
    message_id: MessageId,
    recipient_repository_id: RepositoryId,
    created_at: &str,
) -> Result<(), StoreError> {
    sqlx::query("INSERT INTO message_targets(message_id, repository_id) VALUES (?, ?)")
        .bind(message_id.to_string())
        .bind(recipient_repository_id.to_string())
        .execute(&mut **transaction)
        .await?;
    sqlx::query(
        "INSERT INTO deliveries(
             message_id, recipient_repository_id, status, created_at,
             acked_at, acked_by_instance_id, acked_by_worker_run_id
         ) VALUES (?, ?, 'pending', ?, NULL, NULL, NULL)",
    )
    .bind(message_id.to_string())
    .bind(recipient_repository_id.to_string())
    .bind(created_at)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

fn actor_owner(actor: &ResolvedActor) -> (&'static str, String) {
    match (actor.instance_id, actor.worker_run_id) {
        (Some(instance_id), None) => ("claimed_by_instance_id", instance_id.to_string()),
        (None, Some(worker_run_id)) => ("claimed_by_worker_run_id", worker_run_id.to_string()),
        _ => unreachable!("resolved actor has exactly one identity"),
    }
}

async fn renew_request_claims_for_actor(
    transaction: &mut Transaction<'_, Sqlite>,
    actor: &ResolvedActor,
    claim_duration: chrono::Duration,
    now: &str,
) -> Result<(), StoreError> {
    let (owner_column, owner_id) = actor_owner(actor);
    renew_request_claims_for_owner(transaction, owner_column, &owner_id, claim_duration, now).await
}

async fn renew_request_claims_for_owner(
    transaction: &mut Transaction<'_, Sqlite>,
    owner_column: &str,
    owner_id: &str,
    claim_duration: chrono::Duration,
    now: &str,
) -> Result<(), StoreError> {
    debug_assert!(matches!(
        owner_column,
        "claimed_by_instance_id" | "claimed_by_worker_run_id"
    ));
    let claim_expires_at =
        (Utc::now() + claim_duration).to_rfc3339_opts(SecondsFormat::Micros, true);
    let query = format!(
        "UPDATE requests SET claim_expires_at = ?, updated_at = ?
         WHERE status = 'claimed' AND {owner_column} = ? AND claim_expires_at > ?"
    );
    sqlx::query(&query)
        .bind(claim_expires_at)
        .bind(now)
        .bind(owner_id)
        .bind(now)
        .execute(&mut **transaction)
        .await?;
    Ok(())
}

async fn renew_work_item_claims_for_owner(
    transaction: &mut Transaction<'_, Sqlite>,
    instance_id: InstanceId,
    claim_duration: chrono::Duration,
    now: &str,
) -> Result<(), StoreError> {
    let claim_expires_at =
        (Utc::now() + claim_duration).to_rfc3339_opts(SecondsFormat::Micros, true);
    sqlx::query(
        "UPDATE work_items SET claim_expires_at = ?, updated_at = ?
         WHERE status IN ('claimed', 'in_progress', 'blocked')
           AND assignee_instance_id = ? AND claim_expires_at > ?",
    )
    .bind(claim_expires_at)
    .bind(now)
    .bind(instance_id.to_string())
    .bind(now)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

fn ensure_active_request_claim(
    request: &CoordinationRequest,
    actor: &ResolvedActor,
    now: &str,
) -> Result<(), StoreError> {
    let owns_claim = match (actor.instance_id, actor.worker_run_id) {
        (Some(instance_id), None) => request.claimed_by_instance_id == Some(instance_id),
        (None, Some(worker_run_id)) => request.claimed_by_worker_run_id == Some(worker_run_id),
        _ => false,
    };
    if request.project_id != actor.project_id
        || request.target_repository_id != actor.repository_id
        || request.status != RequestStatus::Claimed
        || !owns_claim
        || request
            .claim_expires_at
            .as_deref()
            .is_none_or(|expires| expires <= now)
    {
        return Err(request_transition_conflict(request.request_id, "use claim"));
    }
    Ok(())
}

fn request_transition_conflict(request_id: RequestId, action: &str) -> StoreError {
    StoreError::Conflict(format!(
        "request {request_id} cannot perform '{action}': status, target, owner or claim lease does not match"
    ))
}

fn claim_conflict(
    resource_type: &'static str,
    resource_id: String,
    message: impl Into<String>,
    retry_at: Option<String>,
    now: &str,
) -> StoreError {
    let retry_after_seconds = retry_at.as_deref().and_then(|retry_at| {
        let retry_at = chrono::DateTime::parse_from_rfc3339(retry_at).ok()?;
        let now = chrono::DateTime::parse_from_rfc3339(now).ok()?;
        let milliseconds = retry_at.signed_duration_since(now).num_milliseconds();
        (milliseconds > 0).then(|| ((milliseconds + 999) / 1_000) as u64)
    });
    StoreError::ClaimConflict {
        resource_type,
        resource_id,
        message: message.into(),
        retry_at,
        retry_after_seconds,
    }
}

fn validate_worker_run_command(command: &CreateWorkerRun) -> Result<(), StoreError> {
    validate_context_refs_value(&command.context_refs)?;
    match command.role {
        WorkerRole::Auditor => {
            if command.work_item_id.is_none() {
                return Err(StoreError::Conflict(
                    "auditor run requires a claimed audit work_item_id".to_owned(),
                ));
            }
            if command.addresses_audit_run_id.is_some() {
                return Err(StoreError::Conflict(
                    "auditor run cannot address another audit run".to_owned(),
                ));
            }
            if command.request_id.is_some() {
                return Err(StoreError::Conflict(
                    "auditor run cannot link a repository request".to_owned(),
                ));
            }
        }
        WorkerRole::Executor => {
            if command.work_item_id.is_none() {
                return Err(StoreError::Conflict(
                    "executor run requires a claimed implementation or remediation work_item_id"
                        .to_owned(),
                ));
            }
            if command.request_id.is_some() {
                return Err(StoreError::Conflict(
                    "executor run cannot link a repository request".to_owned(),
                ));
            }
        }
        WorkerRole::Butler => {
            if command.addresses_audit_run_id.is_some() || command.work_item_id.is_some() {
                return Err(StoreError::Conflict(
                    "butler run cannot declare audit scope or remediation link".to_owned(),
                ));
            }
            if command.request_id.is_none() {
                return Err(StoreError::Conflict(
                    "butler run requires one request_id".to_owned(),
                ));
            }
        }
    }
    Ok(())
}

async fn validate_remediation_audit(
    transaction: &mut Transaction<'_, Sqlite>,
    repository_id: RepositoryId,
    feature_id: FeatureId,
    audit_run_id: WorkerRunId,
) -> Result<(), StoreError> {
    let audit = sqlx::query(
        "SELECT run.repository_id, run.role, run.status, item.feature_id,
                item.kind AS work_item_kind, item.status AS work_item_status,
                (SELECT COUNT(*) FROM artifacts
                 WHERE producer_worker_run_id = run.worker_run_id
                   AND kind = 'audit_report') AS report_count
         FROM worker_runs run
         JOIN work_items item ON item.work_item_id = run.work_item_id
         WHERE run.worker_run_id = ?",
    )
    .bind(audit_run_id.to_string())
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or_else(|| StoreError::NotFound(format!("audit worker run {audit_run_id}")))?;
    let audit_repository_id = RepositoryId::parse(audit.try_get("repository_id")?)?;
    let role: String = audit.try_get("role")?;
    let status: String = audit.try_get("status")?;
    let audit_feature_id = FeatureId::parse(audit.try_get("feature_id")?)?;
    let work_item_kind: String = audit.try_get("work_item_kind")?;
    let work_item_status: String = audit.try_get("work_item_status")?;
    let report_count: i64 = audit.try_get("report_count")?;
    if audit_repository_id != repository_id
        || audit_feature_id != feature_id
        || role != "auditor"
        || status != "completed"
        || work_item_kind != WorkItemKind::Audit.as_str()
        || work_item_status != WorkItemStatus::Completed.as_str()
        || report_count != 1
    {
        return Err(StoreError::Conflict(format!(
            "worker run {audit_run_id} must be a completed repository-local audit work item with exactly one report"
        )));
    }
    let actionable = sqlx::query(
        "SELECT COUNT(*) AS total_count,
                COALESCE(SUM(CASE
                    WHEN scope.audit_iteration < item.max_audit_iterations
                     AND item.verification_status = 'remediation_required'
                    THEN 1 ELSE 0 END), 0) AS eligible_count
         FROM audit_outcomes outcome
         JOIN worker_run_audit_items scope
           ON scope.worker_run_id = outcome.audit_worker_run_id
          AND scope.work_item_id = outcome.scoped_work_item_id
         JOIN work_items item ON item.work_item_id = outcome.scoped_work_item_id
         WHERE outcome.audit_worker_run_id = ?
           AND outcome.verdict = 'changes_required'",
    )
    .bind(audit_run_id.to_string())
    .fetch_one(&mut **transaction)
    .await?;
    let total_count: i64 = actionable.try_get("total_count")?;
    let eligible_count: i64 = actionable.try_get("eligible_count")?;
    if total_count == 0 {
        return Err(StoreError::Conflict(format!(
            "audit worker run {audit_run_id} has no changes_required outcome to remediate"
        )));
    }
    if eligible_count != total_count {
        return Err(StoreError::Conflict(format!(
            "all {total_count} changes_required outcomes from audit worker run {audit_run_id} must have remaining audit depth before its single remediation can start"
        )));
    }
    let inactive_features: i64 = sqlx::query_scalar(
        "SELECT COUNT(DISTINCT features.feature_id)
         FROM worker_run_audit_items
         JOIN work_items
           ON work_items.work_item_id = worker_run_audit_items.work_item_id
         JOIN features ON features.feature_id = work_items.feature_id
         WHERE worker_run_audit_items.worker_run_id = ?
           AND features.status <> 'active'",
    )
    .bind(audit_run_id.to_string())
    .fetch_one(&mut **transaction)
    .await?;
    if inactive_features != 0 {
        return Err(StoreError::Conflict(format!(
            "audit worker run {audit_run_id} belongs to a terminal feature"
        )));
    }
    let existing =
        sqlx::query("SELECT worker_run_id FROM worker_runs WHERE addresses_audit_run_id = ?")
            .bind(audit_run_id.to_string())
            .fetch_optional(&mut **transaction)
            .await?;
    if existing.is_some() {
        return Err(StoreError::Conflict(format!(
            "audit worker run {audit_run_id} already has a remediation executor run"
        )));
    }
    let existing_work_item = sqlx::query(
        "SELECT work_item_id FROM work_items
         WHERE kind = 'remediation' AND addresses_audit_run_id = ?",
    )
    .bind(audit_run_id.to_string())
    .fetch_optional(&mut **transaction)
    .await?;
    if existing_work_item.is_some() {
        return Err(StoreError::Conflict(format!(
            "audit worker run {audit_run_id} already has a remediation work item"
        )));
    }
    let later_iteration_exists: i64 = sqlx::query_scalar(
        "SELECT EXISTS(
             SELECT 1
             FROM worker_runs current_run
             JOIN work_item_audit_scope current_scope
               ON current_scope.audit_work_item_id = current_run.work_item_id
             JOIN work_item_audit_scope later
               ON later.scoped_work_item_id = current_scope.scoped_work_item_id
              AND later.audit_iteration > current_scope.audit_iteration
             WHERE current_run.worker_run_id = ?
         )",
    )
    .bind(audit_run_id.to_string())
    .fetch_one(&mut **transaction)
    .await?;
    if later_iteration_exists != 0 {
        return Err(StoreError::Conflict(format!(
            "audit worker run {audit_run_id} cannot start remediation after a later audit iteration"
        )));
    }
    Ok(())
}

async fn resolve_recipients(
    transaction: &mut Transaction<'_, Sqlite>,
    project_id: ProjectId,
    target: MessageTarget,
) -> Result<Vec<RepositoryId>, StoreError> {
    match target {
        MessageTarget::Broadcast => {
            let rows = sqlx::query(
                "SELECT repository_id FROM repositories
                 WHERE project_id = ? ORDER BY repository_key",
            )
            .bind(project_id.to_string())
            .fetch_all(&mut **transaction)
            .await?;
            if rows.len() > MAX_MESSAGE_RECIPIENTS {
                return Err(StoreError::Conflict(format!(
                    "a message may target at most {MAX_MESSAGE_RECIPIENTS} repositories"
                )));
            }
            rows.iter()
                .map(|row| Ok(RepositoryId::parse(row.try_get("repository_id")?)?))
                .collect()
        }
        MessageTarget::Repositories(repository_ids) => {
            let unique: BTreeSet<String> = repository_ids
                .into_iter()
                .map(|id| id.to_string())
                .collect();
            if unique.is_empty() {
                return Err(StoreError::Conflict(
                    "at least one target repository is required".to_owned(),
                ));
            }
            if unique.len() > MAX_MESSAGE_RECIPIENTS {
                return Err(StoreError::Conflict(format!(
                    "a message may target at most {MAX_MESSAGE_RECIPIENTS} repositories"
                )));
            }
            let mut query = QueryBuilder::<Sqlite>::new(
                "SELECT repository_id, project_id FROM repositories WHERE repository_id IN (",
            );
            let mut separated = query.separated(", ");
            for repository_id in &unique {
                separated.push_bind(repository_id);
            }
            separated.push_unseparated(") ORDER BY repository_id");
            let rows = query.build().fetch_all(&mut **transaction).await?;
            if rows.len() != unique.len() {
                return Err(StoreError::NotFound(
                    "one or more target repositories do not exist".to_owned(),
                ));
            }
            let mut recipients = Vec::with_capacity(rows.len());
            for row in rows {
                let repository_id: String = row.try_get("repository_id")?;
                let recipient_project_id = ProjectId::parse(row.try_get("project_id")?)?;
                if recipient_project_id != project_id {
                    return Err(StoreError::Conflict(format!(
                        "repository {repository_id} belongs to another project"
                    )));
                }
                recipients.push(RepositoryId::parse(&repository_id)?);
            }
            Ok(recipients)
        }
    }
}

fn validate_context_refs_value(context_refs: &serde_json::Value) -> Result<(), StoreError> {
    let values = context_refs.as_array().ok_or_else(|| {
        StoreError::Conflict("context_refs must be an array of objects".to_owned())
    })?;
    if values.len() > MAX_CONTEXT_REFS {
        return Err(StoreError::Conflict(format!(
            "context_refs must contain at most {MAX_CONTEXT_REFS} items"
        )));
    }
    if values.iter().any(|value| !value.is_object()) {
        return Err(StoreError::Conflict(
            "context_refs must contain only objects".to_owned(),
        ));
    }
    Ok(())
}

async fn insert_artifact(
    transaction: &mut Transaction<'_, Sqlite>,
    artifact: &Artifact,
) -> Result<(), StoreError> {
    sqlx::query(
        "INSERT INTO artifacts(
             artifact_id, project_id, producer_repository_id, producer_instance_id,
             producer_worker_run_id, kind, title, content_type, content, created_at
         ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(artifact.artifact_id.to_string())
    .bind(artifact.project_id.to_string())
    .bind(artifact.producer_repository_id.to_string())
    .bind(artifact.producer_instance_id.map(|id| id.to_string()))
    .bind(artifact.producer_worker_run_id.map(|id| id.to_string()))
    .bind(artifact.kind.as_str())
    .bind(&artifact.title)
    .bind(&artifact.content_type)
    .bind(&artifact.content)
    .bind(&artifact.created_at)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

async fn insert_message(
    transaction: &mut Transaction<'_, Sqlite>,
    message: &Message,
) -> Result<(), StoreError> {
    sqlx::query(
        "INSERT INTO messages(
             message_id, project_id, sender_repository_id, sender_instance_id,
             sender_worker_run_id, kind, feature_id, work_item_id, request_id,
             thread_id, correlation_id, in_reply_to,
             summary, artifact_id, created_at
         ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(message.message_id.to_string())
    .bind(message.project_id.to_string())
    .bind(message.sender_repository_id.to_string())
    .bind(message.sender_instance_id.map(|id| id.to_string()))
    .bind(message.sender_worker_run_id.map(|id| id.to_string()))
    .bind(message.kind.as_str())
    .bind(message.feature_id.map(|id| id.to_string()))
    .bind(message.work_item_id.map(|id| id.to_string()))
    .bind(message.request_id.map(|id| id.to_string()))
    .bind(message.thread_id.to_string())
    .bind(&message.correlation_id)
    .bind(message.in_reply_to.map(|id| id.to_string()))
    .bind(&message.summary)
    .bind(message.artifact_id.map(|id| id.to_string()))
    .bind(&message.created_at)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

async fn ensure_schema_version(pool: &SqlitePool) -> Result<(), StoreError> {
    let current = sqlx::query("SELECT value FROM schema_meta WHERE key = 'schema_version'")
        .fetch_optional(pool)
        .await?;
    match current {
        None => {
            sqlx::query("INSERT INTO schema_meta(key, value) VALUES ('schema_version', ?)")
                .bind(SCHEMA_VERSION)
                .execute(pool)
                .await?;
            Ok(())
        }
        Some(row) => {
            let version: String = row.try_get("value")?;
            if version == SCHEMA_VERSION {
                Ok(())
            } else {
                Err(StoreError::Conflict(format!(
                    "database schema version {version} is unsupported; expected {SCHEMA_VERSION}"
                )))
            }
        }
    }
}

async fn upsert_project(
    transaction: &mut Transaction<'_, Sqlite>,
    command: &ConnectOrchestrator,
    now: &str,
) -> Result<Project, StoreError> {
    let existing = sqlx::query("SELECT * FROM projects WHERE project_key = ?")
        .bind(&command.project_key)
        .fetch_optional(&mut **transaction)
        .await?;
    if let Some(row) = existing {
        let project = project_from_row(&row)?;
        if let Some(display_name) = command.project_display_name.as_deref()
            && display_name != project.display_name
        {
            sqlx::query(
                "UPDATE projects SET display_name = ?, updated_at = ? WHERE project_id = ?",
            )
            .bind(display_name)
            .bind(now)
            .bind(project.project_id.to_string())
            .execute(&mut **transaction)
            .await?;
            return Ok(Project {
                display_name: display_name.to_owned(),
                updated_at: now.to_owned(),
                ..project
            });
        }
        return Ok(project);
    }

    let project = Project {
        project_id: ProjectId::new(),
        project_key: command.project_key.clone(),
        display_name: command
            .project_display_name
            .clone()
            .unwrap_or_else(|| command.project_key.clone()),
        created_at: now.to_owned(),
        updated_at: now.to_owned(),
    };
    sqlx::query(
        "INSERT INTO projects(project_id, project_key, display_name, created_at, updated_at)
         VALUES (?, ?, ?, ?, ?)",
    )
    .bind(project.project_id.to_string())
    .bind(&project.project_key)
    .bind(&project.display_name)
    .bind(&project.created_at)
    .bind(&project.updated_at)
    .execute(&mut **transaction)
    .await?;
    Ok(project)
}

async fn upsert_repository(
    transaction: &mut Transaction<'_, Sqlite>,
    project: &Project,
    command: &ConnectOrchestrator,
    now: &str,
) -> Result<(Repository, Option<String>), StoreError> {
    let existing =
        sqlx::query("SELECT * FROM repositories WHERE project_id = ? AND repository_key = ?")
            .bind(project.project_id.to_string())
            .bind(&command.repository_key)
            .fetch_optional(&mut **transaction)
            .await?;
    if let Some(row) = existing {
        let mut repository = repository_from_row(&row)?;
        let warning = (repository.canonical_path != command.canonical_path).then(|| {
            format!(
                "repository '{}' is registered at '{}' but connected from '{}'",
                repository.repository_key, repository.canonical_path, command.canonical_path
            )
        });
        if let Some(display_name) = command.repository_display_name.as_deref()
            && display_name != repository.display_name
        {
            sqlx::query(
                "UPDATE repositories SET display_name = ?, updated_at = ? WHERE repository_id = ?",
            )
            .bind(display_name)
            .bind(now)
            .bind(repository.repository_id.to_string())
            .execute(&mut **transaction)
            .await?;
            repository.display_name = display_name.to_owned();
            repository.updated_at = now.to_owned();
        }
        return Ok((repository, warning));
    }

    let repository = Repository {
        repository_id: RepositoryId::new(),
        project_id: project.project_id,
        repository_key: command.repository_key.clone(),
        display_name: command
            .repository_display_name
            .clone()
            .unwrap_or_else(|| command.repository_key.clone()),
        canonical_path: command.canonical_path.clone(),
        created_at: now.to_owned(),
        updated_at: now.to_owned(),
    };
    sqlx::query(
        "INSERT INTO repositories(
             repository_id, project_id, repository_key, display_name,
             canonical_path, created_at, updated_at
         ) VALUES (?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(repository.repository_id.to_string())
    .bind(repository.project_id.to_string())
    .bind(&repository.repository_key)
    .bind(&repository.display_name)
    .bind(&repository.canonical_path)
    .bind(&repository.created_at)
    .bind(&repository.updated_at)
    .execute(&mut **transaction)
    .await?;
    Ok((repository, None))
}

async fn upsert_instance(
    transaction: &mut Transaction<'_, Sqlite>,
    repository: &Repository,
    instance_id: InstanceId,
    capabilities_json: &str,
    now: &str,
    lease_expires_at: &str,
) -> Result<AgentInstance, StoreError> {
    let existing =
        sqlx::query("SELECT repository_id, started_at FROM agent_instances WHERE instance_id = ?")
            .bind(instance_id.to_string())
            .fetch_optional(&mut **transaction)
            .await?;

    let started_at = if let Some(row) = existing {
        let existing_repository_id: String = row.try_get("repository_id")?;
        if existing_repository_id != repository.repository_id.to_string() {
            return Err(StoreError::Conflict(format!(
                "instance {instance_id} already belongs to another repository"
            )));
        }
        row.try_get("started_at")?
    } else {
        now.to_owned()
    };

    sqlx::query(
        "INSERT INTO agent_instances(
             instance_id, repository_id, kind, status, capabilities_json,
             started_at, last_heartbeat_at, lease_expires_at, disconnected_at
         ) VALUES (?, ?, 'orchestrator', 'online', ?, ?, ?, ?, NULL)
         ON CONFLICT(instance_id) DO UPDATE SET
             status = 'online',
             capabilities_json = excluded.capabilities_json,
             last_heartbeat_at = excluded.last_heartbeat_at,
             lease_expires_at = excluded.lease_expires_at,
             disconnected_at = NULL",
    )
    .bind(instance_id.to_string())
    .bind(repository.repository_id.to_string())
    .bind(capabilities_json)
    .bind(&started_at)
    .bind(now)
    .bind(lease_expires_at)
    .execute(&mut **transaction)
    .await?;

    Ok(AgentInstance {
        instance_id,
        repository_id: repository.repository_id,
        kind: "orchestrator".to_owned(),
        status: PresenceStatus::Online,
        capabilities: serde_json::from_str(capabilities_json)?,
        started_at,
        last_heartbeat_at: now.to_owned(),
        lease_expires_at: lease_expires_at.to_owned(),
        disconnected_at: None,
    })
}

fn project_from_row(row: &SqliteRow) -> Result<Project, StoreError> {
    Ok(Project {
        project_id: ProjectId::parse(row.try_get("project_id")?)?,
        project_key: row.try_get("project_key")?,
        display_name: row.try_get("display_name")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

fn repository_from_row(row: &SqliteRow) -> Result<Repository, StoreError> {
    Ok(Repository {
        repository_id: RepositoryId::parse(row.try_get("repository_id")?)?,
        project_id: ProjectId::parse(row.try_get("project_id")?)?,
        repository_key: row.try_get("repository_key")?,
        display_name: row.try_get("display_name")?,
        canonical_path: row.try_get("canonical_path")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

fn instance_from_row(row: &SqliteRow, now: &str) -> Result<AgentInstance, StoreError> {
    let stored_status: String = row.try_get("status")?;
    let lease_expires_at: String = row.try_get("lease_expires_at")?;
    let status = if stored_status == "disconnected" {
        PresenceStatus::Disconnected
    } else if lease_expires_at.as_str() <= now {
        PresenceStatus::Offline
    } else {
        PresenceStatus::Online
    };
    let capabilities_json: String = row.try_get("capabilities_json")?;
    Ok(AgentInstance {
        instance_id: InstanceId::parse(row.try_get("instance_id")?)?,
        repository_id: RepositoryId::parse(row.try_get("repository_id")?)?,
        kind: row.try_get("kind")?,
        status,
        capabilities: serde_json::from_str(&capabilities_json)?,
        started_at: row.try_get("started_at")?,
        last_heartbeat_at: row.try_get("last_heartbeat_at")?,
        lease_expires_at,
        disconnected_at: row.try_get("disconnected_at")?,
    })
}

fn worker_run_from_row(row: &SqliteRow) -> Result<WorkerRun, StoreError> {
    let role: String = row.try_get("role")?;
    let status: String = row.try_get("status")?;
    let context_refs_json: String = row.try_get("context_refs_json")?;
    Ok(WorkerRun {
        worker_run_id: WorkerRunId::parse(row.try_get("worker_run_id")?)?,
        repository_id: RepositoryId::parse(row.try_get("repository_id")?)?,
        parent_instance_id: InstanceId::parse(row.try_get("parent_instance_id")?)?,
        role: role.parse().map_err(StoreError::Conflict)?,
        status: status.parse().map_err(StoreError::Conflict)?,
        task_summary: row.try_get("task_summary")?,
        context_refs: serde_json::from_str(&context_refs_json)?,
        result_summary: row.try_get("result_summary")?,
        error_summary: row.try_get("error_summary")?,
        audit_scope: Vec::new(),
        addresses_audit_run_id: parse_optional_id(
            row.try_get("addresses_audit_run_id")?,
            WorkerRunId::parse,
        )?,
        work_item_id: parse_optional_id(row.try_get("work_item_id")?, WorkItemId::parse)?,
        request_id: parse_optional_id(row.try_get("request_id")?, RequestId::parse)?,
        created_at: row.try_get("created_at")?,
        activated_at: row.try_get("activated_at")?,
        last_heartbeat_at: row.try_get("last_heartbeat_at")?,
        lease_expires_at: row.try_get("lease_expires_at")?,
        finished_at: row.try_get("finished_at")?,
    })
}

fn message_from_row(row: &SqliteRow) -> Result<Message, StoreError> {
    let kind: String = row.try_get("kind")?;
    Ok(Message {
        message_id: MessageId::parse(row.try_get("message_id")?)?,
        project_id: ProjectId::parse(row.try_get("project_id")?)?,
        sender_repository_id: RepositoryId::parse(row.try_get("sender_repository_id")?)?,
        sender_instance_id: parse_optional_id(
            row.try_get("sender_instance_id")?,
            InstanceId::parse,
        )?,
        sender_worker_run_id: parse_optional_id(
            row.try_get("sender_worker_run_id")?,
            WorkerRunId::parse,
        )?,
        kind: kind.parse().map_err(StoreError::Conflict)?,
        feature_id: parse_optional_id(row.try_get("feature_id")?, FeatureId::parse)?,
        work_item_id: parse_optional_id(row.try_get("work_item_id")?, WorkItemId::parse)?,
        request_id: parse_optional_id(row.try_get("request_id")?, RequestId::parse)?,
        thread_id: ThreadId::parse(row.try_get("thread_id")?)?,
        correlation_id: row.try_get("correlation_id")?,
        in_reply_to: parse_optional_id(row.try_get("in_reply_to")?, MessageId::parse)?,
        summary: row.try_get("summary")?,
        artifact_id: parse_optional_id(row.try_get("artifact_id")?, ArtifactId::parse)?,
        created_at: row.try_get("created_at")?,
    })
}

fn artifact_from_row(row: &SqliteRow) -> Result<Artifact, StoreError> {
    let kind: String = row.try_get("kind")?;
    Ok(Artifact {
        artifact_id: ArtifactId::parse(row.try_get("artifact_id")?)?,
        project_id: ProjectId::parse(row.try_get("project_id")?)?,
        producer_repository_id: RepositoryId::parse(row.try_get("producer_repository_id")?)?,
        producer_instance_id: parse_optional_id(
            row.try_get("producer_instance_id")?,
            InstanceId::parse,
        )?,
        producer_worker_run_id: parse_optional_id(
            row.try_get("producer_worker_run_id")?,
            WorkerRunId::parse,
        )?,
        kind: kind.parse().map_err(StoreError::Conflict)?,
        title: row.try_get("title")?,
        content_type: row.try_get("content_type")?,
        content: row.try_get("content")?,
        created_at: row.try_get("created_at")?,
    })
}

fn delivery_ack_from_row(row: &SqliteRow) -> Result<DeliveryAck, StoreError> {
    Ok(DeliveryAck {
        delivery_id: row.try_get("delivery_id")?,
        message_id: MessageId::parse(row.try_get("message_id")?)?,
        recipient_repository_id: RepositoryId::parse(row.try_get("recipient_repository_id")?)?,
        status: row.try_get("status")?,
        acked_at: row.try_get("acked_at")?,
        acked_by_instance_id: parse_optional_id(
            row.try_get("acked_by_instance_id")?,
            InstanceId::parse,
        )?,
        acked_by_worker_run_id: parse_optional_id(
            row.try_get("acked_by_worker_run_id")?,
            WorkerRunId::parse,
        )?,
    })
}

fn event_from_row(row: &SqliteRow) -> Result<HubEvent, StoreError> {
    let kind: String = row.try_get("kind")?;
    let entity_type: String = row.try_get("entity_type")?;
    Ok(HubEvent {
        event_id: row.try_get("event_id")?,
        project_id: ProjectId::parse(row.try_get("project_id")?)?,
        recipient_repository_id: RepositoryId::parse(row.try_get("recipient_repository_id")?)?,
        kind: kind.parse().map_err(StoreError::Conflict)?,
        entity_type: entity_type.parse().map_err(StoreError::Conflict)?,
        entity_id: row.try_get("entity_id")?,
        feature_id: parse_optional_id(row.try_get("feature_id")?, FeatureId::parse)?,
        owner_repository_id: parse_optional_id(
            row.try_get("owner_repository_id")?,
            RepositoryId::parse,
        )?,
        entity_status: row.try_get("entity_status")?,
        entity_revision: row
            .try_get::<Option<i64>, _>("entity_revision")?
            .map(u64::try_from)
            .transpose()
            .map_err(|_| StoreError::Conflict("stored event revision is invalid".to_owned()))?,
        work_item_id: parse_optional_id(row.try_get("work_item_id")?, WorkItemId::parse)?,
        request_id: parse_optional_id(row.try_get("request_id")?, RequestId::parse)?,
        created_at: row.try_get("created_at")?,
    })
}

fn request_from_row(row: &SqliteRow) -> Result<CoordinationRequest, StoreError> {
    let status: String = row.try_get("status")?;
    let handler_requirement: String = row.try_get("handler_requirement")?;
    let answer_requirement: String = row.try_get("answer_requirement")?;
    let answer_provenance: Option<String> = row.try_get("answer_provenance")?;
    let context_refs_json: String = row.try_get("context_refs_json")?;
    Ok(CoordinationRequest {
        request_id: RequestId::parse(row.try_get("request_id")?)?,
        project_id: ProjectId::parse(row.try_get("project_id")?)?,
        requester_repository_id: RepositoryId::parse(row.try_get("requester_repository_id")?)?,
        target_repository_id: RepositoryId::parse(row.try_get("target_repository_id")?)?,
        feature_id: parse_optional_id(row.try_get("feature_id")?, FeatureId::parse)?,
        work_item_id: parse_optional_id(row.try_get("work_item_id")?, WorkItemId::parse)?,
        created_by_instance_id: parse_optional_id(
            row.try_get("created_by_instance_id")?,
            InstanceId::parse,
        )?,
        created_by_worker_run_id: parse_optional_id(
            row.try_get("created_by_worker_run_id")?,
            WorkerRunId::parse,
        )?,
        status: status.parse().map_err(StoreError::Conflict)?,
        handler_requirement: handler_requirement.parse().map_err(StoreError::Conflict)?,
        answer_requirement: answer_requirement.parse().map_err(StoreError::Conflict)?,
        question_summary: row.try_get("question_summary")?,
        question_artifact_id: parse_optional_id(
            row.try_get("question_artifact_id")?,
            ArtifactId::parse,
        )?,
        context_refs: serde_json::from_str(&context_refs_json)?,
        claimed_by_instance_id: parse_optional_id(
            row.try_get("claimed_by_instance_id")?,
            InstanceId::parse,
        )?,
        claimed_by_worker_run_id: parse_optional_id(
            row.try_get("claimed_by_worker_run_id")?,
            WorkerRunId::parse,
        )?,
        claim_expires_at: row.try_get("claim_expires_at")?,
        failure_summary: row.try_get("failure_summary")?,
        failure_retryable: row.try_get::<Option<bool>, _>("failure_retryable")?,
        answer_message_id: parse_optional_id(row.try_get("answer_message_id")?, MessageId::parse)?,
        answer_provenance: answer_provenance
            .map(|value| value.parse().map_err(StoreError::Conflict))
            .transpose()?,
        answered_by_instance_id: parse_optional_id(
            row.try_get("answered_by_instance_id")?,
            InstanceId::parse,
        )?,
        answered_by_worker_run_id: parse_optional_id(
            row.try_get("answered_by_worker_run_id")?,
            WorkerRunId::parse,
        )?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
        finished_at: row.try_get("finished_at")?,
    })
}

fn parse_optional_id<T>(
    value: Option<String>,
    parser: impl FnOnce(&str) -> Result<T, uuid::Error>,
) -> Result<Option<T>, StoreError> {
    value
        .as_deref()
        .map(parser)
        .transpose()
        .map_err(StoreError::InvalidUuid)
}

fn invalid_worker_transition(
    worker_run_id: WorkerRunId,
    current_status: WorkerRunStatus,
    requested_action: &str,
) -> StoreError {
    StoreError::Conflict(format!(
        "worker run {worker_run_id} is '{}'; cannot transition to or perform '{requested_action}'",
        current_status.as_str()
    ))
}

fn expired_worker_lease(worker_run_id: WorkerRunId) -> StoreError {
    StoreError::Conflict(format!(
        "worker run {worker_run_id} lease has expired and cannot be renewed"
    ))
}

fn lease_deadline(duration: chrono::Duration) -> String {
    (Utc::now() + duration).to_rfc3339_opts(SecondsFormat::Micros, true)
}

fn utc_now() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Micros, true)
}

#[cfg(test)]
mod tests;
