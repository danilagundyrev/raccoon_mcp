use std::{
    collections::{BTreeSet, HashMap},
    path::Path,
    str::FromStr,
    sync::{Arc, Mutex as StdMutex},
    time::Duration,
};

use chrono::{SecondsFormat, Utc};
use sqlx::{
    QueryBuilder, Row, Sqlite, SqlitePool, Transaction,
    sqlite::{
        SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteRow, SqliteSynchronous,
    },
};
use thiserror::Error;
use tokio::{
    sync::{Mutex, Notify},
    time::{Instant, interval_at, sleep_until},
};

use crate::domain::{
    ActorRef, AgentInstance, Artifact, ArtifactId, AuditScopeItem, ConnectOrchestrator,
    ConnectResult, CreateWorkerRun, DeliveryAck, EventBatch, EventEntityType, EventKind, HubEvent,
    InboxItem, InboxPage, InstanceId, Message, MessageId, MessageTarget, PresenceStatus, Project,
    ProjectId, ProjectOverview, PublishMessage, PublishResult, Repository, RepositoryId,
    RepositoryPresence, ThreadId, WaitOutcome, WorkItemId, WorkerRole, WorkerRun, WorkerRunId,
    WorkerRunStatus,
};

use super::schema::{CREATE_SCHEMA, SCHEMA_VERSION};

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("not found: {0}")]
    NotFound(String),
    #[error("conflict: {0}")]
    Conflict(String),
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
    write_lock: Arc<Mutex<()>>,
    notifications: Arc<NotificationHub>,
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

impl SqliteStore {
    pub async fn open(
        database_path: &Path,
        max_connections: u32,
        lease_seconds: u16,
        worker_lease_seconds: u16,
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
            write_lock: Arc::new(Mutex::new(())),
            notifications: Arc::new(NotificationHub::default()),
        })
    }

    pub async fn health_check(&self) -> Result<(), StoreError> {
        sqlx::query("SELECT 1").execute(&self.pool).await?;
        Ok(())
    }

    pub async fn connect_orchestrator(
        &self,
        command: ConnectOrchestrator,
    ) -> Result<ConnectResult, StoreError> {
        let _write_guard = self.write_lock.lock().await;
        let now = utc_now();
        let lease_expires_at =
            (Utc::now() + self.lease_duration).to_rfc3339_opts(SecondsFormat::Micros, true);
        let capabilities_json = serde_json::to_string(&command.capabilities)?;
        let mut transaction = self.pool.begin().await?;

        let project = upsert_project(&mut transaction, &command, &now).await?;
        let (repository, warning) =
            upsert_repository(&mut transaction, &project, &command, &now).await?;
        let instance = upsert_instance(
            &mut transaction,
            &repository,
            command.instance_id.unwrap_or_default(),
            &capabilities_json,
            &now,
            &lease_expires_at,
        )
        .await?;

        transaction.commit().await?;
        let repositories = self.list_repositories(project.project_id).await?;

        Ok(ConnectResult {
            project,
            repository,
            instance,
            repositories,
            warnings: warning.into_iter().collect(),
            next_actions: vec![
                "Call heartbeat before the returned lease expires.".to_owned(),
                "Inspect the project overview to discover peer repositories.".to_owned(),
                "Before spawning a child, call create_worker_run and pass its worker_run_id to the child."
                    .to_owned(),
                "Use list_inbox for compact pending headers and read_artifact only when full content is needed."
                    .to_owned(),
            ],
        })
    }

    pub async fn heartbeat(&self, instance_id: InstanceId) -> Result<AgentInstance, StoreError> {
        let _write_guard = self.write_lock.lock().await;
        let row = sqlx::query("SELECT status FROM agent_instances WHERE instance_id = ?")
            .bind(instance_id.to_string())
            .fetch_optional(&self.pool)
            .await?
            .ok_or_else(|| StoreError::NotFound(format!("instance {instance_id}")))?;
        let status: String = row.try_get("status")?;
        if status == "disconnected" {
            return Err(StoreError::Conflict(format!(
                "instance {instance_id} is disconnected; connect again with a new instance_id"
            )));
        }

        let now = utc_now();
        let lease_expires_at =
            (Utc::now() + self.lease_duration).to_rfc3339_opts(SecondsFormat::Micros, true);
        sqlx::query(
            "UPDATE agent_instances
             SET status = 'online', last_heartbeat_at = ?, lease_expires_at = ?
             WHERE instance_id = ?",
        )
        .bind(&now)
        .bind(&lease_expires_at)
        .bind(instance_id.to_string())
        .execute(&self.pool)
        .await?;

        self.get_instance(instance_id).await
    }

    pub async fn disconnect_orchestrator(
        &self,
        instance_id: InstanceId,
    ) -> Result<AgentInstance, StoreError> {
        let _write_guard = self.write_lock.lock().await;
        let now = utc_now();
        let result = sqlx::query(
            "UPDATE agent_instances
             SET status = 'disconnected', disconnected_at = ?, lease_expires_at = ?
             WHERE instance_id = ?",
        )
        .bind(&now)
        .bind(&now)
        .bind(instance_id.to_string())
        .execute(&self.pool)
        .await?;
        if result.rows_affected() == 0 {
            return Err(StoreError::NotFound(format!("instance {instance_id}")));
        }
        self.get_instance(instance_id).await
    }

    pub async fn get_instance(&self, instance_id: InstanceId) -> Result<AgentInstance, StoreError> {
        let row = sqlx::query("SELECT * FROM agent_instances WHERE instance_id = ?")
            .bind(instance_id.to_string())
            .fetch_optional(&self.pool)
            .await?
            .ok_or_else(|| StoreError::NotFound(format!("instance {instance_id}")))?;
        instance_from_row(&row, &utc_now())
    }

    pub async fn create_worker_run(
        &self,
        command: CreateWorkerRun,
    ) -> Result<WorkerRun, StoreError> {
        let _write_guard = self.write_lock.lock().await;
        validate_worker_run_command(&command)?;
        let now = utc_now();
        let worker_lease_expires_at = lease_deadline(self.worker_lease_duration);
        let orchestrator_lease_expires_at = lease_deadline(self.lease_duration);
        let context_refs_json = serde_json::to_string(&command.context_refs)?;
        let worker_run_id = WorkerRunId::new();
        let mut transaction = self.pool.begin().await?;

        let parent =
            sqlx::query("SELECT repository_id, status FROM agent_instances WHERE instance_id = ?")
                .bind(command.parent_instance_id.to_string())
                .fetch_optional(&mut *transaction)
                .await?
                .ok_or_else(|| {
                    StoreError::NotFound(format!("parent instance {}", command.parent_instance_id))
                })?;
        let parent_status: String = parent.try_get("status")?;
        if parent_status == "disconnected" {
            return Err(StoreError::Conflict(format!(
                "parent instance {} is disconnected",
                command.parent_instance_id
            )));
        }
        let repository_id = RepositoryId::parse(parent.try_get("repository_id")?)?;
        let project_id = ProjectId::parse(
            sqlx::query("SELECT project_id FROM repositories WHERE repository_id = ?")
                .bind(repository_id.to_string())
                .fetch_one(&mut *transaction)
                .await?
                .try_get("project_id")?,
        )?;
        let audit_scope = prepare_audit_scope(&mut transaction, &command).await?;
        if let Some(audit_run_id) = command.addresses_audit_run_id {
            validate_remediation_audit(&mut transaction, repository_id, audit_run_id).await?;
        }

        sqlx::query(
            "UPDATE agent_instances
             SET status = 'online', last_heartbeat_at = ?, lease_expires_at = ?
             WHERE instance_id = ?",
        )
        .bind(&now)
        .bind(orchestrator_lease_expires_at)
        .bind(command.parent_instance_id.to_string())
        .execute(&mut *transaction)
        .await?;

        sqlx::query(
            "INSERT INTO worker_runs(
                 worker_run_id, repository_id, parent_instance_id, role, status,
                 task_summary, context_refs_json, result_summary, error_summary, addresses_audit_run_id,
                 created_at, activated_at, last_heartbeat_at, lease_expires_at, finished_at
             ) VALUES (?, ?, ?, ?, 'created', ?, ?, NULL, NULL, ?, ?, NULL, ?, ?, NULL)",
        )
        .bind(worker_run_id.to_string())
        .bind(repository_id.to_string())
        .bind(command.parent_instance_id.to_string())
        .bind(command.role.as_str())
        .bind(&command.task_summary)
        .bind(&context_refs_json)
        .bind(command.addresses_audit_run_id.map(|id| id.to_string()))
        .bind(&now)
        .bind(&now)
        .bind(&worker_lease_expires_at)
        .execute(&mut *transaction)
        .await?;

        for scope_item in &audit_scope {
            sqlx::query(
                "INSERT INTO worker_run_audit_items(
                     worker_run_id, work_item_id, audit_iteration, max_iterations
                 ) VALUES (?, ?, ?, ?)",
            )
            .bind(worker_run_id.to_string())
            .bind(scope_item.work_item_id.to_string())
            .bind(i64::from(scope_item.iteration))
            .bind(i64::from(scope_item.max_iterations))
            .execute(&mut *transaction)
            .await?;
        }

        append_event(
            &mut transaction,
            project_id,
            repository_id,
            EventKind::WorkerRunCreated,
            EventEntityType::WorkerRun,
            &worker_run_id.to_string(),
            &now,
        )
        .await?;

        transaction.commit().await?;
        self.notifications.notify(repository_id);
        self.get_worker_run(worker_run_id).await
    }

    pub async fn activate_worker_run(
        &self,
        worker_run_id: WorkerRunId,
    ) -> Result<WorkerRun, StoreError> {
        let write_guard = self.write_lock.lock().await;
        let worker = self.get_worker_run(worker_run_id).await?;
        if worker.lease_expires_at <= utc_now() {
            return Err(expired_worker_lease(worker_run_id));
        }
        match worker.status {
            WorkerRunStatus::Created => {
                let now = utc_now();
                let lease_expires_at = lease_deadline(self.worker_lease_duration);
                let mut transaction = self.pool.begin().await?;
                sqlx::query(
                    "UPDATE worker_runs
                     SET status = 'active', activated_at = ?, last_heartbeat_at = ?,
                         lease_expires_at = ?
                     WHERE worker_run_id = ? AND status = 'created'",
                )
                .bind(&now)
                .bind(&now)
                .bind(lease_expires_at)
                .bind(worker_run_id.to_string())
                .execute(&mut *transaction)
                .await?;
                let project_id =
                    repository_project_id(&mut transaction, worker.repository_id).await?;
                append_event(
                    &mut transaction,
                    project_id,
                    worker.repository_id,
                    EventKind::WorkerRunActive,
                    EventEntityType::WorkerRun,
                    &worker_run_id.to_string(),
                    &now,
                )
                .await?;
                transaction.commit().await?;
                self.notifications.notify(worker.repository_id);
                self.get_worker_run(worker_run_id).await
            }
            WorkerRunStatus::Active => {
                drop(write_guard);
                self.heartbeat_worker_run(worker_run_id).await
            }
            status => Err(invalid_worker_transition(worker_run_id, status, "active")),
        }
    }

    pub async fn heartbeat_worker_run(
        &self,
        worker_run_id: WorkerRunId,
    ) -> Result<WorkerRun, StoreError> {
        let _write_guard = self.write_lock.lock().await;
        let now = utc_now();
        let lease_expires_at = lease_deadline(self.worker_lease_duration);
        let result = sqlx::query(
            "UPDATE worker_runs
             SET last_heartbeat_at = ?, lease_expires_at = ?
             WHERE worker_run_id = ? AND status = 'active' AND lease_expires_at > ?",
        )
        .bind(&now)
        .bind(lease_expires_at)
        .bind(worker_run_id.to_string())
        .bind(&now)
        .execute(&self.pool)
        .await?;
        if result.rows_affected() == 0 {
            let worker = self.get_worker_run(worker_run_id).await?;
            return Err(invalid_worker_transition(
                worker_run_id,
                worker.status,
                "heartbeat",
            ));
        }
        self.get_worker_run(worker_run_id).await
    }

    pub async fn complete_worker_run(
        &self,
        worker_run_id: WorkerRunId,
        result_summary: &str,
    ) -> Result<WorkerRun, StoreError> {
        self.finish_worker_run(
            worker_run_id,
            WorkerRunStatus::Completed,
            Some(result_summary),
            None,
        )
        .await
    }

    pub async fn fail_worker_run(
        &self,
        worker_run_id: WorkerRunId,
        error_summary: &str,
    ) -> Result<WorkerRun, StoreError> {
        self.finish_worker_run(
            worker_run_id,
            WorkerRunStatus::Failed,
            None,
            Some(error_summary),
        )
        .await
    }

    pub async fn get_worker_run(
        &self,
        worker_run_id: WorkerRunId,
    ) -> Result<WorkerRun, StoreError> {
        let row = sqlx::query("SELECT * FROM worker_runs WHERE worker_run_id = ?")
            .bind(worker_run_id.to_string())
            .fetch_optional(&self.pool)
            .await?
            .ok_or_else(|| StoreError::NotFound(format!("worker run {worker_run_id}")))?;
        let mut worker = worker_run_from_row(&row)?;
        let audit_rows = sqlx::query(
            "SELECT work_item_id, audit_iteration, max_iterations
             FROM worker_run_audit_items
             WHERE worker_run_id = ?
             ORDER BY work_item_id",
        )
        .bind(worker_run_id.to_string())
        .fetch_all(&self.pool)
        .await?;
        worker.audit_scope = audit_rows
            .iter()
            .map(|row| {
                Ok(AuditScopeItem {
                    work_item_id: WorkItemId::parse(row.try_get("work_item_id")?)?,
                    iteration: row.try_get::<i64, _>("audit_iteration")? as u8,
                    max_iterations: row.try_get::<i64, _>("max_iterations")? as u8,
                })
            })
            .collect::<Result<Vec<_>, StoreError>>()?;
        Ok(worker)
    }

    async fn finish_worker_run(
        &self,
        worker_run_id: WorkerRunId,
        target_status: WorkerRunStatus,
        result_summary: Option<&str>,
        error_summary: Option<&str>,
    ) -> Result<WorkerRun, StoreError> {
        let _write_guard = self.write_lock.lock().await;
        let worker = self.get_worker_run(worker_run_id).await?;
        if worker.status == target_status {
            return Ok(worker);
        }
        let can_finish = match target_status {
            WorkerRunStatus::Completed => worker.status == WorkerRunStatus::Active,
            WorkerRunStatus::Failed => {
                matches!(
                    worker.status,
                    WorkerRunStatus::Created | WorkerRunStatus::Active
                )
            }
            _ => false,
        };
        if !can_finish {
            return Err(invalid_worker_transition(
                worker_run_id,
                worker.status,
                target_status.as_str(),
            ));
        }

        let now = utc_now();
        let mut transaction = self.pool.begin().await?;
        let result = sqlx::query(
            "UPDATE worker_runs
             SET status = ?, result_summary = ?, error_summary = ?,
                 last_heartbeat_at = ?, lease_expires_at = ?, finished_at = ?
             WHERE worker_run_id = ?
               AND ((? = 'completed' AND status = 'active')
                    OR (? = 'failed' AND status IN ('created', 'active')))",
        )
        .bind(target_status.as_str())
        .bind(result_summary)
        .bind(error_summary)
        .bind(&now)
        .bind(&now)
        .bind(&now)
        .bind(worker_run_id.to_string())
        .bind(target_status.as_str())
        .bind(target_status.as_str())
        .execute(&mut *transaction)
        .await?;
        if result.rows_affected() != 1 {
            transaction.rollback().await?;
            let current = self.get_worker_run(worker_run_id).await?;
            return Err(invalid_worker_transition(
                worker_run_id,
                current.status,
                target_status.as_str(),
            ));
        }
        let project_id = repository_project_id(&mut transaction, worker.repository_id).await?;
        let event_kind = match target_status {
            WorkerRunStatus::Completed => EventKind::WorkerRunCompleted,
            WorkerRunStatus::Failed => EventKind::WorkerRunFailed,
            _ => unreachable!("finish_worker_run validates terminal status"),
        };
        append_event(
            &mut transaction,
            project_id,
            worker.repository_id,
            event_kind,
            EventEntityType::WorkerRun,
            &worker_run_id.to_string(),
            &now,
        )
        .await?;
        transaction.commit().await?;
        self.notifications.notify(worker.repository_id);
        self.get_worker_run(worker_run_id).await
    }

    pub async fn publish_message(
        &self,
        command: PublishMessage,
    ) -> Result<PublishResult, StoreError> {
        let _write_guard = self.write_lock.lock().await;
        let now = utc_now();
        let mut transaction = self.pool.begin().await?;
        let actor = self
            .resolve_actor(&mut transaction, command.actor, &now)
            .await?;

        let (inherited_thread, inherited_correlation_id) =
            if let Some(in_reply_to) = command.in_reply_to {
                let row = sqlx::query(
                    "SELECT m.thread_id, m.correlation_id
                 FROM messages m
                 WHERE m.message_id = ? AND m.project_id = ?
                   AND (m.sender_repository_id = ? OR EXISTS (
                       SELECT 1 FROM deliveries d
                       WHERE d.message_id = m.message_id
                         AND d.recipient_repository_id = ?
                   ))",
                )
                .bind(in_reply_to.to_string())
                .bind(actor.project_id.to_string())
                .bind(actor.repository_id.to_string())
                .bind(actor.repository_id.to_string())
                .fetch_optional(&mut *transaction)
                .await?
                .ok_or_else(|| StoreError::NotFound(format!("accessible message {in_reply_to}")))?;
                (
                    Some(ThreadId::parse(row.try_get("thread_id")?)?),
                    row.try_get::<Option<String>, _>("correlation_id")?,
                )
            } else {
                (None, None)
            };
        if let (Some(explicit), Some(inherited)) = (command.thread_id, inherited_thread) {
            if explicit != inherited {
                return Err(StoreError::Conflict(
                    "explicit thread_id does not match the replied-to message".to_owned(),
                ));
            }
        }
        let thread_id = command.thread_id.or(inherited_thread).unwrap_or_default();
        if let (Some(explicit), Some(inherited)) = (
            command.correlation_id.as_deref(),
            inherited_correlation_id.as_deref(),
        ) {
            if explicit != inherited {
                return Err(StoreError::Conflict(
                    "explicit correlation_id does not match the replied-to message".to_owned(),
                ));
            }
        }
        let correlation_id = command.correlation_id.or(inherited_correlation_id);

        let recipient_ids =
            resolve_recipients(&mut transaction, actor.project_id, command.target).await?;

        let artifact = if let Some(content) = command.body {
            let artifact = Artifact {
                artifact_id: ArtifactId::new(),
                project_id: actor.project_id,
                producer_repository_id: actor.repository_id,
                producer_instance_id: actor.instance_id,
                producer_worker_run_id: actor.worker_run_id,
                kind: command.artifact_kind,
                title: command
                    .artifact_title
                    .unwrap_or_else(|| command.summary.clone()),
                content_type: command.content_type,
                content,
                created_at: now.clone(),
            };
            insert_artifact(&mut transaction, &artifact).await?;
            Some(artifact)
        } else {
            None
        };

        let message = Message {
            message_id: MessageId::new(),
            project_id: actor.project_id,
            sender_repository_id: actor.repository_id,
            sender_instance_id: actor.instance_id,
            sender_worker_run_id: actor.worker_run_id,
            kind: command.kind,
            thread_id,
            correlation_id,
            in_reply_to: command.in_reply_to,
            summary: command.summary,
            artifact_id: artifact.as_ref().map(|artifact| artifact.artifact_id),
            created_at: now.clone(),
        };
        insert_message(&mut transaction, &message).await?;
        for recipient_id in &recipient_ids {
            sqlx::query("INSERT INTO message_targets(message_id, repository_id) VALUES (?, ?)")
                .bind(message.message_id.to_string())
                .bind(recipient_id.to_string())
                .execute(&mut *transaction)
                .await?;
            sqlx::query(
                "INSERT INTO deliveries(
                     message_id, recipient_repository_id, status, created_at,
                     acked_at, acked_by_instance_id, acked_by_worker_run_id
                 ) VALUES (?, ?, 'pending', ?, NULL, NULL, NULL)",
            )
            .bind(message.message_id.to_string())
            .bind(recipient_id.to_string())
            .bind(&now)
            .execute(&mut *transaction)
            .await?;
            append_event(
                &mut transaction,
                actor.project_id,
                *recipient_id,
                EventKind::MessageAvailable,
                EventEntityType::Message,
                &message.message_id.to_string(),
                &now,
            )
            .await?;
        }

        transaction.commit().await?;
        for recipient_id in &recipient_ids {
            self.notifications.notify(*recipient_id);
        }
        Ok(PublishResult {
            message,
            artifact,
            recipient_count: recipient_ids.len(),
        })
    }

    pub async fn list_inbox(
        &self,
        actor_ref: ActorRef,
        after_delivery_id: Option<i64>,
        limit: u8,
    ) -> Result<InboxPage, StoreError> {
        let now = utc_now();
        let actor = self.resolve_actor_readonly(actor_ref, &now).await?;

        let rows = sqlx::query(
            "SELECT d.delivery_id, m.*
             FROM deliveries d
             JOIN messages m ON m.message_id = d.message_id
             WHERE d.recipient_repository_id = ?
               AND d.status = 'pending'
               AND d.delivery_id > ?
             ORDER BY d.delivery_id
             LIMIT ?",
        )
        .bind(actor.repository_id.to_string())
        .bind(after_delivery_id.unwrap_or(0))
        .bind(i64::from(limit) + 1)
        .fetch_all(&self.pool)
        .await?;

        let has_more = rows.len() > usize::from(limit);
        let items = rows
            .iter()
            .take(usize::from(limit))
            .map(|row| {
                Ok(InboxItem {
                    delivery_id: row.try_get("delivery_id")?,
                    message: message_from_row(row)?,
                })
            })
            .collect::<Result<Vec<_>, StoreError>>()?;
        let next_cursor = items.last().map(|item| item.delivery_id);
        Ok(InboxPage {
            items,
            next_cursor,
            has_more,
        })
    }

    pub async fn read_message(
        &self,
        actor_ref: ActorRef,
        message_id: MessageId,
    ) -> Result<Message, StoreError> {
        let now = utc_now();
        let actor = self.resolve_actor_readonly(actor_ref, &now).await?;

        let row = sqlx::query(
            "SELECT m.* FROM messages m
             WHERE m.message_id = ?
               AND (m.sender_repository_id = ? OR EXISTS (
                   SELECT 1 FROM deliveries d
                   WHERE d.message_id = m.message_id
                     AND d.recipient_repository_id = ?
               ))",
        )
        .bind(message_id.to_string())
        .bind(actor.repository_id.to_string())
        .bind(actor.repository_id.to_string())
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| StoreError::NotFound(format!("accessible message {message_id}")))?;
        message_from_row(&row)
    }

    pub async fn read_artifact(
        &self,
        actor_ref: ActorRef,
        artifact_id: ArtifactId,
    ) -> Result<Artifact, StoreError> {
        let now = utc_now();
        let actor = self.resolve_actor_readonly(actor_ref, &now).await?;

        let row = sqlx::query(
            "SELECT a.* FROM artifacts a
             WHERE a.artifact_id = ?
               AND (a.producer_repository_id = ? OR EXISTS (
                   SELECT 1
                   FROM messages m
                   JOIN deliveries d ON d.message_id = m.message_id
                   WHERE m.artifact_id = a.artifact_id
                     AND d.recipient_repository_id = ?
               ))",
        )
        .bind(artifact_id.to_string())
        .bind(actor.repository_id.to_string())
        .bind(actor.repository_id.to_string())
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| StoreError::NotFound(format!("accessible artifact {artifact_id}")))?;
        artifact_from_row(&row)
    }

    pub async fn ack_message(
        &self,
        actor_ref: ActorRef,
        message_id: MessageId,
    ) -> Result<DeliveryAck, StoreError> {
        let _write_guard = self.write_lock.lock().await;
        let now = utc_now();
        let mut transaction = self.pool.begin().await?;
        let actor = self
            .resolve_actor(&mut transaction, actor_ref, &now)
            .await?;
        let existing = sqlx::query(
            "SELECT * FROM deliveries
             WHERE message_id = ? AND recipient_repository_id = ?",
        )
        .bind(message_id.to_string())
        .bind(actor.repository_id.to_string())
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or_else(|| StoreError::NotFound(format!("inbox message {message_id}")))?;
        let status: String = existing.try_get("status")?;
        if status == "pending" {
            sqlx::query(
                "UPDATE deliveries
                 SET status = 'acked', acked_at = ?, acked_by_instance_id = ?,
                     acked_by_worker_run_id = ?
                 WHERE delivery_id = ? AND status = 'pending'",
            )
            .bind(&now)
            .bind(actor.instance_id.map(|id| id.to_string()))
            .bind(actor.worker_run_id.map(|id| id.to_string()))
            .bind(existing.try_get::<i64, _>("delivery_id")?)
            .execute(&mut *transaction)
            .await?;
        }
        let row = sqlx::query("SELECT * FROM deliveries WHERE delivery_id = ?")
            .bind(existing.try_get::<i64, _>("delivery_id")?)
            .fetch_one(&mut *transaction)
            .await?;
        transaction.commit().await?;
        delivery_ack_from_row(&row)
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
                Ok(ResolvedActor {
                    project_id: ProjectId::parse(row.try_get("project_id")?)?,
                    repository_id: RepositoryId::parse(row.try_get("repository_id")?)?,
                    instance_id: Some(instance_id),
                    worker_run_id: None,
                })
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
                Ok(ResolvedActor {
                    project_id: ProjectId::parse(row.try_get("project_id")?)?,
                    repository_id: RepositoryId::parse(row.try_get("repository_id")?)?,
                    instance_id: None,
                    worker_run_id: Some(worker_run_id),
                })
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
                Ok(ResolvedActor {
                    project_id: ProjectId::parse(row.try_get("project_id")?)?,
                    repository_id: RepositoryId::parse(row.try_get("repository_id")?)?,
                    instance_id: Some(instance_id),
                    worker_run_id: None,
                })
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
                Ok(ResolvedActor {
                    project_id: ProjectId::parse(row.try_get("project_id")?)?,
                    repository_id: RepositoryId::parse(row.try_get("repository_id")?)?,
                    instance_id: None,
                    worker_run_id: Some(worker_run_id),
                })
            }
        }
    }

    pub async fn wait_for_events(
        &self,
        actor_ref: ActorRef,
        cursor: i64,
        kinds: &[EventKind],
        timeout: Option<Duration>,
        limit: u8,
    ) -> Result<EventBatch, StoreError> {
        if cursor < 0 {
            return Err(StoreError::Conflict(
                "event cursor must be non-negative".to_owned(),
            ));
        }
        if !(1..=50).contains(&limit) {
            return Err(StoreError::Conflict(
                "event limit must be between 1 and 50".to_owned(),
            ));
        }

        let actor = self.touch_actor(actor_ref).await?;
        let repository_id = actor.repository_id;
        let notifier = self.notifications.for_repository(repository_id);
        let lease_seconds = match actor_ref {
            ActorRef::Orchestrator(_) => self.lease_duration.num_seconds(),
            ActorRef::Worker(_) => self.worker_lease_duration.num_seconds(),
        };
        let heartbeat_every = Duration::from_secs((lease_seconds / 3).max(1) as u64);
        let mut heartbeat = interval_at(Instant::now() + heartbeat_every, heartbeat_every);
        let deadline = timeout.map(|duration| Instant::now() + duration);

        loop {
            // Register before querying so a commit between the query and await cannot be missed.
            let notified = notifier.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();

            let batch = self
                .fetch_events(repository_id, cursor, kinds, limit)
                .await?;
            if !batch.events.is_empty() {
                return Ok(batch);
            }

            tokio::select! {
                _ = &mut notified => {}
                _ = heartbeat.tick() => {
                    let renewed = self.touch_actor(actor_ref).await?;
                    if renewed.repository_id != repository_id {
                        return Err(StoreError::Conflict(
                            "actor repository changed while waiting for events".to_owned(),
                        ));
                    }
                }
                _ = wait_until(deadline) => {
                    return Ok(EventBatch {
                        outcome: WaitOutcome::Timeout,
                        events: Vec::new(),
                        next_cursor: cursor,
                        has_more: false,
                    });
                }
            }
        }
    }

    async fn touch_actor(&self, actor_ref: ActorRef) -> Result<ResolvedActor, StoreError> {
        let _write_guard = self.write_lock.lock().await;
        let mut transaction = self.pool.begin().await?;
        let actor = self
            .resolve_actor(&mut transaction, actor_ref, &utc_now())
            .await?;
        transaction.commit().await?;
        Ok(actor)
    }

    async fn fetch_events(
        &self,
        repository_id: RepositoryId,
        cursor: i64,
        kinds: &[EventKind],
        limit: u8,
    ) -> Result<EventBatch, StoreError> {
        let mut query = QueryBuilder::<Sqlite>::new(
            "SELECT event_id, project_id, recipient_repository_id, kind, entity_type, entity_id, created_at \
             FROM events WHERE recipient_repository_id = ",
        );
        query
            .push_bind(repository_id.to_string())
            .push(" AND event_id > ")
            .push_bind(cursor);
        if !kinds.is_empty() {
            query.push(" AND kind IN (");
            let mut separated = query.separated(", ");
            for kind in kinds {
                separated.push_bind(kind.as_str());
            }
            separated.push_unseparated(")");
        }
        query
            .push(" ORDER BY event_id LIMIT ")
            .push_bind(i64::from(limit) + 1);

        let rows = query.build().fetch_all(&self.pool).await?;
        let has_more = rows.len() > usize::from(limit);
        let events = rows
            .iter()
            .take(usize::from(limit))
            .map(event_from_row)
            .collect::<Result<Vec<_>, StoreError>>()?;
        let next_cursor = events.last().map_or(cursor, |event| event.event_id);
        Ok(EventBatch {
            outcome: WaitOutcome::Events,
            events,
            next_cursor,
            has_more,
        })
    }

    pub async fn list_repositories(
        &self,
        project_id: ProjectId,
    ) -> Result<Vec<RepositoryPresence>, StoreError> {
        let now = utc_now();
        let rows = sqlx::query(
            "SELECT r.*,
                    COUNT(i.instance_id) AS online_instances
             FROM repositories r
             LEFT JOIN agent_instances i
               ON i.repository_id = r.repository_id
              AND i.status = 'online'
              AND i.lease_expires_at > ?
             WHERE r.project_id = ?
             GROUP BY r.repository_id
             ORDER BY r.repository_key",
        )
        .bind(&now)
        .bind(project_id.to_string())
        .fetch_all(&self.pool)
        .await?;

        rows.iter()
            .map(|row| {
                Ok(RepositoryPresence {
                    repository: repository_from_row(row)?,
                    online_instances: row.try_get::<i64, _>("online_instances")? as u64,
                })
            })
            .collect()
    }

    pub async fn project_overview(
        &self,
        project_id: ProjectId,
    ) -> Result<ProjectOverview, StoreError> {
        let row = sqlx::query("SELECT * FROM projects WHERE project_id = ?")
            .bind(project_id.to_string())
            .fetch_optional(&self.pool)
            .await?
            .ok_or_else(|| StoreError::NotFound(format!("project {project_id}")))?;
        let project = project_from_row(&row)?;
        let repositories = self.list_repositories(project_id).await?;
        Ok(ProjectOverview {
            project,
            repositories,
        })
    }
}

async fn wait_until(deadline: Option<Instant>) {
    match deadline {
        Some(deadline) => sleep_until(deadline).await,
        None => std::future::pending::<()>().await,
    }
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
    sqlx::query(
        "INSERT INTO events(
             project_id, recipient_repository_id, kind, entity_type, entity_id, created_at
         ) VALUES (?, ?, ?, ?, ?, ?)",
    )
    .bind(project_id.to_string())
    .bind(recipient_repository_id.to_string())
    .bind(kind.as_str())
    .bind(entity_type.as_str())
    .bind(entity_id)
    .bind(created_at)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

fn validate_worker_run_command(command: &CreateWorkerRun) -> Result<(), StoreError> {
    let has_audit_scope = !command.audit_work_items.is_empty();
    match command.role {
        WorkerRole::Auditor => {
            if !has_audit_scope {
                return Err(StoreError::Conflict(
                    "auditor run requires at least one audit_work_item".to_owned(),
                ));
            }
            if command.addresses_audit_run_id.is_some() {
                return Err(StoreError::Conflict(
                    "auditor run cannot address another audit run".to_owned(),
                ));
            }
        }
        WorkerRole::Executor => {
            if has_audit_scope {
                return Err(StoreError::Conflict(
                    "executor run cannot declare audit_work_items".to_owned(),
                ));
            }
        }
        WorkerRole::Butler => {
            if has_audit_scope || command.addresses_audit_run_id.is_some() {
                return Err(StoreError::Conflict(
                    "butler run cannot declare audit scope or remediation link".to_owned(),
                ));
            }
        }
    }

    let mut unique_work_items = BTreeSet::new();
    for item in &command.audit_work_items {
        if !(1..=10).contains(&item.max_iterations) {
            return Err(StoreError::Conflict(format!(
                "max_iterations for work item {} must be between 1 and 10",
                item.work_item_id
            )));
        }
        if !unique_work_items.insert(item.work_item_id.to_string()) {
            return Err(StoreError::Conflict(format!(
                "work item {} appears more than once in audit scope",
                item.work_item_id
            )));
        }
    }
    Ok(())
}

async fn prepare_audit_scope(
    transaction: &mut Transaction<'_, Sqlite>,
    command: &CreateWorkerRun,
) -> Result<Vec<AuditScopeItem>, StoreError> {
    if command.role != WorkerRole::Auditor {
        return Ok(Vec::new());
    }

    let mut scope = Vec::with_capacity(command.audit_work_items.len());
    for item in &command.audit_work_items {
        let row = sqlx::query(
            "SELECT MAX(audit_iteration) AS current_iteration,
                    MIN(max_iterations) AS min_max_iterations,
                    MAX(max_iterations) AS max_max_iterations
             FROM worker_run_audit_items
             WHERE work_item_id = ?",
        )
        .bind(item.work_item_id.to_string())
        .fetch_one(&mut **transaction)
        .await?;
        let current_iteration: Option<i64> = row.try_get("current_iteration")?;
        let stored_min: Option<i64> = row.try_get("min_max_iterations")?;
        let stored_max: Option<i64> = row.try_get("max_max_iterations")?;
        if let (Some(stored_min), Some(stored_max)) = (stored_min, stored_max) {
            if stored_min != i64::from(item.max_iterations)
                || stored_max != i64::from(item.max_iterations)
            {
                return Err(StoreError::Conflict(format!(
                    "work item {} audit depth is already fixed at {stored_min}",
                    item.work_item_id
                )));
            }
        }
        let iteration = current_iteration.unwrap_or(0) + 1;
        if iteration > i64::from(item.max_iterations) {
            return Err(StoreError::Conflict(format!(
                "work item {} exhausted audit depth {}",
                item.work_item_id, item.max_iterations
            )));
        }
        scope.push(AuditScopeItem {
            work_item_id: item.work_item_id,
            iteration: iteration as u8,
            max_iterations: item.max_iterations,
        });
    }
    Ok(scope)
}

async fn validate_remediation_audit(
    transaction: &mut Transaction<'_, Sqlite>,
    repository_id: RepositoryId,
    audit_run_id: WorkerRunId,
) -> Result<(), StoreError> {
    let audit =
        sqlx::query("SELECT repository_id, role, status FROM worker_runs WHERE worker_run_id = ?")
            .bind(audit_run_id.to_string())
            .fetch_optional(&mut **transaction)
            .await?
            .ok_or_else(|| StoreError::NotFound(format!("audit worker run {audit_run_id}")))?;
    let audit_repository_id = RepositoryId::parse(audit.try_get("repository_id")?)?;
    let role: String = audit.try_get("role")?;
    let status: String = audit.try_get("status")?;
    if audit_repository_id != repository_id || role != "auditor" || status != "completed" {
        return Err(StoreError::Conflict(format!(
            "worker run {audit_run_id} must be a completed auditor run in the same repository"
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
            let mut recipients = Vec::with_capacity(unique.len());
            for repository_id in unique {
                let row =
                    sqlx::query("SELECT project_id FROM repositories WHERE repository_id = ?")
                        .bind(&repository_id)
                        .fetch_optional(&mut **transaction)
                        .await?
                        .ok_or_else(|| {
                            StoreError::NotFound(format!("repository {repository_id}"))
                        })?;
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
             sender_worker_run_id, kind, thread_id, correlation_id, in_reply_to,
             summary, artifact_id, created_at
         ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(message.message_id.to_string())
    .bind(message.project_id.to_string())
    .bind(message.sender_repository_id.to_string())
    .bind(message.sender_instance_id.map(|id| id.to_string()))
    .bind(message.sender_worker_run_id.map(|id| id.to_string()))
    .bind(message.kind.as_str())
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
        if let Some(display_name) = command.project_display_name.as_deref() {
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
        if let Some(display_name) = command.repository_display_name.as_deref() {
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
        created_at: row.try_get("created_at")?,
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
mod tests {
    use std::{fs, path::PathBuf};

    use serde_json::json;
    use uuid::Uuid;

    use super::*;
    use crate::domain::{AuditWorkItemInput, WorkItemId, WorkerRole};

    fn temp_database_path() -> PathBuf {
        std::env::temp_dir()
            .join("agent-sync-mcp-tests")
            .join(format!("{}.sqlite3", Uuid::new_v4()))
    }

    async fn test_store(lease_seconds: u16) -> (SqliteStore, PathBuf) {
        let path = temp_database_path();
        let store = SqliteStore::open(&path, 2, lease_seconds, 3600)
            .await
            .unwrap();
        (store, path)
    }

    fn connect_command(project: &str, repository: &str, path: &str) -> ConnectOrchestrator {
        ConnectOrchestrator {
            project_key: project.to_owned(),
            project_display_name: None,
            repository_key: repository.to_owned(),
            repository_display_name: None,
            canonical_path: path.to_owned(),
            instance_id: None,
            capabilities: json!({ "spawning": true }),
        }
    }

    fn publish_command(
        actor: ActorRef,
        target: MessageTarget,
        summary: &str,
        body: Option<&str>,
    ) -> PublishMessage {
        PublishMessage {
            actor,
            kind: crate::domain::MessageKind::Report,
            target,
            summary: summary.to_owned(),
            body: body.map(str::to_owned),
            artifact_kind: crate::domain::ArtifactKind::ImplementationReport,
            artifact_title: None,
            content_type: "text/markdown".to_owned(),
            thread_id: None,
            correlation_id: None,
            in_reply_to: None,
        }
    }

    fn remove_database(path: &Path) {
        let _ = fs::remove_file(path);
        let _ = fs::remove_file(path.with_extension("sqlite3-shm"));
        let _ = fs::remove_file(path.with_extension("sqlite3-wal"));
        if let Some(parent) = path.parent() {
            let _ = fs::remove_dir(parent);
        }
    }

    #[tokio::test]
    async fn connects_multiple_repositories_to_one_project() {
        let (store, path) = test_store(90).await;
        let backend = store
            .connect_orchestrator(connect_command("project", "backend", "C:/project/backend"))
            .await
            .unwrap();
        let web = store
            .connect_orchestrator(connect_command("project", "web", "C:/project/web"))
            .await
            .unwrap();

        assert_eq!(backend.project.project_id, web.project.project_id);
        assert_ne!(
            backend.repository.repository_id,
            web.repository.repository_id
        );
        let overview = store
            .project_overview(backend.project.project_id)
            .await
            .unwrap();
        assert_eq!(overview.repositories.len(), 2);
        assert!(
            overview
                .repositories
                .iter()
                .all(|repo| repo.online_instances == 1)
        );

        store.pool.close().await;
        remove_database(&path);
    }

    #[tokio::test]
    async fn disconnect_is_visible_in_presence() {
        let (store, path) = test_store(90).await;
        let connected = store
            .connect_orchestrator(connect_command("project", "backend", "C:/project/backend"))
            .await
            .unwrap();
        let disconnected = store
            .disconnect_orchestrator(connected.instance.instance_id)
            .await
            .unwrap();

        assert_eq!(disconnected.status, PresenceStatus::Disconnected);
        let repositories = store
            .list_repositories(connected.project.project_id)
            .await
            .unwrap();
        assert_eq!(repositories[0].online_instances, 0);

        store.pool.close().await;
        remove_database(&path);
    }

    #[tokio::test]
    async fn rejects_instance_reuse_across_repositories() {
        let (store, path) = test_store(90).await;
        let connected = store
            .connect_orchestrator(connect_command("project", "backend", "C:/project/backend"))
            .await
            .unwrap();
        let mut command = connect_command("project", "web", "C:/project/web");
        command.instance_id = Some(connected.instance.instance_id);

        let error = store.connect_orchestrator(command).await.unwrap_err();
        assert!(matches!(error, StoreError::Conflict(_)));

        store.pool.close().await;
        remove_database(&path);
    }

    #[tokio::test]
    async fn worker_run_completes_full_lifecycle_in_parent_repository() {
        let (store, path) = test_store(90).await;
        let connected = store
            .connect_orchestrator(connect_command("project", "backend", "C:/project/backend"))
            .await
            .unwrap();
        let created = store
            .create_worker_run(CreateWorkerRun {
                parent_instance_id: connected.instance.instance_id,
                role: WorkerRole::Executor,
                task_summary: "Implement a fake endpoint".to_owned(),
                context_refs: json!([{ "kind": "message", "id": "message-1" }]),
                audit_work_items: Vec::new(),
                addresses_audit_run_id: None,
            })
            .await
            .unwrap();

        assert_eq!(created.repository_id, connected.repository.repository_id);
        assert_eq!(created.status, WorkerRunStatus::Created);
        let active = store
            .activate_worker_run(created.worker_run_id)
            .await
            .unwrap();
        assert_eq!(active.status, WorkerRunStatus::Active);
        assert!(active.activated_at.is_some());
        let completed = store
            .complete_worker_run(active.worker_run_id, "Fake endpoint implemented")
            .await
            .unwrap();
        assert_eq!(completed.status, WorkerRunStatus::Completed);
        assert_eq!(
            completed.result_summary.as_deref(),
            Some("Fake endpoint implemented")
        );
        assert!(completed.finished_at.is_some());
        let lifecycle_events = store
            .wait_for_events(
                ActorRef::Orchestrator(connected.instance.instance_id),
                0,
                &[
                    EventKind::WorkerRunCreated,
                    EventKind::WorkerRunActive,
                    EventKind::WorkerRunCompleted,
                ],
                Some(Duration::ZERO),
                20,
            )
            .await
            .unwrap();
        assert_eq!(
            lifecycle_events
                .events
                .iter()
                .map(|event| event.kind)
                .collect::<Vec<_>>(),
            vec![
                EventKind::WorkerRunCreated,
                EventKind::WorkerRunActive,
                EventKind::WorkerRunCompleted,
            ]
        );

        store.pool.close().await;
        remove_database(&path);
    }

    #[tokio::test]
    async fn completed_worker_run_rejects_reactivation() {
        let (store, path) = test_store(90).await;
        let connected = store
            .connect_orchestrator(connect_command("project", "backend", "C:/project/backend"))
            .await
            .unwrap();
        let created = store
            .create_worker_run(CreateWorkerRun {
                parent_instance_id: connected.instance.instance_id,
                role: WorkerRole::Butler,
                task_summary: "Inspect a fake contract".to_owned(),
                context_refs: json!([]),
                audit_work_items: Vec::new(),
                addresses_audit_run_id: None,
            })
            .await
            .unwrap();
        store
            .activate_worker_run(created.worker_run_id)
            .await
            .unwrap();
        store
            .complete_worker_run(created.worker_run_id, "Contract summarized")
            .await
            .unwrap();

        let error = store
            .activate_worker_run(created.worker_run_id)
            .await
            .unwrap_err();
        assert!(matches!(error, StoreError::Conflict(_)));

        store.pool.close().await;
        remove_database(&path);
    }

    #[tokio::test]
    async fn concurrent_finish_transitions_cannot_overwrite_each_other() {
        let (store, path) = test_store(90).await;
        let connected = store
            .connect_orchestrator(connect_command("project", "backend", "C:/project/backend"))
            .await
            .unwrap();
        let created = store
            .create_worker_run(CreateWorkerRun {
                parent_instance_id: connected.instance.instance_id,
                role: WorkerRole::Executor,
                task_summary: "Race two terminal transitions".to_owned(),
                context_refs: json!([]),
                audit_work_items: Vec::new(),
                addresses_audit_run_id: None,
            })
            .await
            .unwrap();
        store
            .activate_worker_run(created.worker_run_id)
            .await
            .unwrap();

        let (completed, failed) = tokio::join!(
            store.complete_worker_run(created.worker_run_id, "completed"),
            store.fail_worker_run(created.worker_run_id, "failed")
        );
        assert_ne!(completed.is_ok(), failed.is_ok());
        let final_run = store.get_worker_run(created.worker_run_id).await.unwrap();
        assert!(matches!(
            final_run.status,
            WorkerRunStatus::Completed | WorkerRunStatus::Failed
        ));

        store.pool.close().await;
        remove_database(&path);
    }

    #[tokio::test]
    async fn direct_message_exposes_header_then_artifact_and_ack_is_idempotent() {
        let (store, path) = test_store(90).await;
        let backend = store
            .connect_orchestrator(connect_command("project", "backend", "C:/project/backend"))
            .await
            .unwrap();
        let web = store
            .connect_orchestrator(connect_command("project", "web", "C:/project/web"))
            .await
            .unwrap();
        let android = store
            .connect_orchestrator(connect_command("project", "android", "C:/project/android"))
            .await
            .unwrap();

        let published = store
            .publish_message(publish_command(
                ActorRef::Orchestrator(backend.instance.instance_id),
                MessageTarget::Repositories(vec![web.repository.repository_id]),
                "Backend contract is ready",
                Some("# Full contract\n\nGET /example"),
            ))
            .await
            .unwrap();
        assert_eq!(published.recipient_count, 1);
        let web_inbox = store
            .list_inbox(ActorRef::Orchestrator(web.instance.instance_id), None, 20)
            .await
            .unwrap();
        assert_eq!(web_inbox.items.len(), 1);
        assert_eq!(
            web_inbox.items[0].message.summary,
            "Backend contract is ready"
        );
        assert!(
            store
                .list_inbox(
                    ActorRef::Orchestrator(android.instance.instance_id),
                    None,
                    20
                )
                .await
                .unwrap()
                .items
                .is_empty()
        );
        let artifact = store
            .read_artifact(
                ActorRef::Orchestrator(web.instance.instance_id),
                published.artifact.unwrap().artifact_id,
            )
            .await
            .unwrap();
        assert_eq!(artifact.content, "# Full contract\n\nGET /example");
        assert!(matches!(
            store
                .read_artifact(
                    ActorRef::Orchestrator(android.instance.instance_id),
                    artifact.artifact_id,
                )
                .await,
            Err(StoreError::NotFound(_))
        ));

        let first_ack = store
            .ack_message(
                ActorRef::Orchestrator(web.instance.instance_id),
                published.message.message_id,
            )
            .await
            .unwrap();
        let second_ack = store
            .ack_message(
                ActorRef::Orchestrator(web.instance.instance_id),
                published.message.message_id,
            )
            .await
            .unwrap();
        assert_eq!(first_ack.delivery_id, second_ack.delivery_id);
        assert!(
            store
                .list_inbox(ActorRef::Orchestrator(web.instance.instance_id), None, 20)
                .await
                .unwrap()
                .items
                .is_empty()
        );

        store.pool.close().await;
        remove_database(&path);
    }

    #[tokio::test]
    async fn broadcast_deliveries_are_acknowledged_independently() {
        let (store, path) = test_store(90).await;
        let backend = store
            .connect_orchestrator(connect_command("project", "backend", "C:/project/backend"))
            .await
            .unwrap();
        let web = store
            .connect_orchestrator(connect_command("project", "web", "C:/project/web"))
            .await
            .unwrap();
        let published = store
            .publish_message(publish_command(
                ActorRef::Orchestrator(backend.instance.instance_id),
                MessageTarget::Broadcast,
                "Project announcement",
                None,
            ))
            .await
            .unwrap();
        assert_eq!(published.recipient_count, 2);

        store
            .ack_message(
                ActorRef::Orchestrator(web.instance.instance_id),
                published.message.message_id,
            )
            .await
            .unwrap();
        assert!(
            store
                .list_inbox(ActorRef::Orchestrator(web.instance.instance_id), None, 20)
                .await
                .unwrap()
                .items
                .is_empty()
        );
        assert_eq!(
            store
                .list_inbox(
                    ActorRef::Orchestrator(backend.instance.instance_id),
                    None,
                    20
                )
                .await
                .unwrap()
                .items
                .len(),
            1
        );

        store.pool.close().await;
        remove_database(&path);
    }

    #[tokio::test]
    async fn active_worker_can_publish_without_creating_repository() {
        let (store, path) = test_store(90).await;
        let backend = store
            .connect_orchestrator(connect_command("project", "backend", "C:/project/backend"))
            .await
            .unwrap();
        let web = store
            .connect_orchestrator(connect_command("project", "web", "C:/project/web"))
            .await
            .unwrap();
        let worker = store
            .create_worker_run(CreateWorkerRun {
                parent_instance_id: backend.instance.instance_id,
                role: WorkerRole::Executor,
                task_summary: "Publish fake report".to_owned(),
                context_refs: json!([]),
                audit_work_items: Vec::new(),
                addresses_audit_run_id: None,
            })
            .await
            .unwrap();
        store
            .activate_worker_run(worker.worker_run_id)
            .await
            .unwrap();

        let published = store
            .publish_message(publish_command(
                ActorRef::Worker(worker.worker_run_id),
                MessageTarget::Repositories(vec![web.repository.repository_id]),
                "Worker report",
                Some("Done"),
            ))
            .await
            .unwrap();
        assert_eq!(
            published.message.sender_worker_run_id,
            Some(worker.worker_run_id)
        );
        assert_eq!(
            store
                .project_overview(backend.project.project_id)
                .await
                .unwrap()
                .repositories
                .len(),
            2
        );

        store.pool.close().await;
        remove_database(&path);
    }

    #[tokio::test]
    async fn direct_message_cannot_cross_project_boundary() {
        let (store, path) = test_store(90).await;
        let first = store
            .connect_orchestrator(connect_command("first", "backend", "C:/first/backend"))
            .await
            .unwrap();
        let second = store
            .connect_orchestrator(connect_command("second", "web", "C:/second/web"))
            .await
            .unwrap();

        let error = store
            .publish_message(publish_command(
                ActorRef::Orchestrator(first.instance.instance_id),
                MessageTarget::Repositories(vec![second.repository.repository_id]),
                "Invalid cross-project message",
                None,
            ))
            .await
            .unwrap_err();
        assert!(matches!(error, StoreError::Conflict(_)));

        store.pool.close().await;
        remove_database(&path);
    }

    #[tokio::test]
    async fn pending_inbox_survives_store_restart() {
        let (store, path) = test_store(90).await;
        let backend = store
            .connect_orchestrator(connect_command("project", "backend", "C:/project/backend"))
            .await
            .unwrap();
        let web = store
            .connect_orchestrator(connect_command("project", "web", "C:/project/web"))
            .await
            .unwrap();
        let published = store
            .publish_message(publish_command(
                ActorRef::Orchestrator(backend.instance.instance_id),
                MessageTarget::Repositories(vec![web.repository.repository_id]),
                "Persist across restart",
                Some("Persistent artifact"),
            ))
            .await
            .unwrap();
        store.pool.close().await;

        let reopened = SqliteStore::open(&path, 2, 90, 3600).await.unwrap();
        let inbox = reopened
            .list_inbox(ActorRef::Orchestrator(web.instance.instance_id), None, 20)
            .await
            .unwrap();
        assert_eq!(
            inbox.items[0].message.message_id,
            published.message.message_id
        );
        assert_eq!(inbox.items[0].message.summary, "Persist across restart");

        reopened.pool.close().await;
        remove_database(&path);
    }

    #[tokio::test]
    async fn concurrent_connects_to_new_project_do_not_lock_database() {
        let (store, path) = test_store(90).await;
        let mut tasks = tokio::task::JoinSet::new();
        for index in 0..20 {
            let store = store.clone();
            tasks.spawn(async move {
                store
                    .connect_orchestrator(connect_command(
                        "parallel-project",
                        &format!("repository-{index}"),
                        &format!("C:/parallel/repository-{index}"),
                    ))
                    .await
            });
        }

        let mut connected = Vec::new();
        while let Some(result) = tasks.join_next().await {
            connected.push(result.unwrap().unwrap());
        }
        assert_eq!(connected.len(), 20);
        let project_id = connected[0].project.project_id;
        assert!(
            connected
                .iter()
                .all(|result| result.project.project_id == project_id)
        );
        assert_eq!(store.list_repositories(project_id).await.unwrap().len(), 20);

        store.pool.close().await;
        remove_database(&path);
    }

    #[tokio::test]
    async fn expired_worker_cannot_renew_or_publish() {
        let (store, path) = test_store(90).await;
        let backend = store
            .connect_orchestrator(connect_command("project", "backend", "C:/project/backend"))
            .await
            .unwrap();
        let worker = store
            .create_worker_run(CreateWorkerRun {
                parent_instance_id: backend.instance.instance_id,
                role: WorkerRole::Executor,
                task_summary: "Expire this worker".to_owned(),
                context_refs: json!([]),
                audit_work_items: Vec::new(),
                addresses_audit_run_id: None,
            })
            .await
            .unwrap();
        store
            .activate_worker_run(worker.worker_run_id)
            .await
            .unwrap();
        sqlx::query("UPDATE worker_runs SET lease_expires_at = ? WHERE worker_run_id = ?")
            .bind("2000-01-01T00:00:00.000000Z")
            .bind(worker.worker_run_id.to_string())
            .execute(&store.pool)
            .await
            .unwrap();

        assert!(matches!(
            store.heartbeat_worker_run(worker.worker_run_id).await,
            Err(StoreError::Conflict(_))
        ));
        assert!(matches!(
            store
                .publish_message(publish_command(
                    ActorRef::Worker(worker.worker_run_id),
                    MessageTarget::Broadcast,
                    "Expired worker must not publish",
                    None,
                ))
                .await,
            Err(StoreError::Conflict(_))
        ));

        store.pool.close().await;
        remove_database(&path);
    }

    #[tokio::test]
    async fn repository_cannot_reply_to_message_it_cannot_access() {
        let (store, path) = test_store(90).await;
        let backend = store
            .connect_orchestrator(connect_command("project", "backend", "C:/project/backend"))
            .await
            .unwrap();
        let web = store
            .connect_orchestrator(connect_command("project", "web", "C:/project/web"))
            .await
            .unwrap();
        let android = store
            .connect_orchestrator(connect_command("project", "android", "C:/project/android"))
            .await
            .unwrap();
        let mut original = publish_command(
            ActorRef::Orchestrator(backend.instance.instance_id),
            MessageTarget::Repositories(vec![web.repository.repository_id]),
            "Private backend-to-web message",
            None,
        );
        original.correlation_id = Some("private-correlation".to_owned());
        let original = store.publish_message(original).await.unwrap();

        let mut unauthorized_reply = publish_command(
            ActorRef::Orchestrator(android.instance.instance_id),
            MessageTarget::Repositories(vec![backend.repository.repository_id]),
            "Unauthorized reply",
            None,
        );
        unauthorized_reply.in_reply_to = Some(original.message.message_id);
        assert!(matches!(
            store.publish_message(unauthorized_reply).await,
            Err(StoreError::NotFound(_))
        ));

        let mut authorized_reply = publish_command(
            ActorRef::Orchestrator(web.instance.instance_id),
            MessageTarget::Repositories(vec![backend.repository.repository_id]),
            "Authorized reply",
            None,
        );
        authorized_reply.in_reply_to = Some(original.message.message_id);
        let reply = store.publish_message(authorized_reply).await.unwrap();
        assert_eq!(reply.message.thread_id, original.message.thread_id);
        assert_eq!(
            reply.message.correlation_id.as_deref(),
            Some("private-correlation")
        );

        store.pool.close().await;
        remove_database(&path);
    }

    #[tokio::test]
    async fn inbox_and_content_reads_do_not_renew_actor_lease() {
        let (store, path) = test_store(90).await;
        let backend = store
            .connect_orchestrator(connect_command("project", "backend", "C:/project/backend"))
            .await
            .unwrap();
        let web = store
            .connect_orchestrator(connect_command("project", "web", "C:/project/web"))
            .await
            .unwrap();
        let published = store
            .publish_message(publish_command(
                ActorRef::Orchestrator(backend.instance.instance_id),
                MessageTarget::Repositories(vec![web.repository.repository_id]),
                "Read without heartbeat",
                Some("Read-only artifact"),
            ))
            .await
            .unwrap();
        let before = store.get_instance(web.instance.instance_id).await.unwrap();

        store
            .list_inbox(ActorRef::Orchestrator(web.instance.instance_id), None, 20)
            .await
            .unwrap();
        store
            .read_message(
                ActorRef::Orchestrator(web.instance.instance_id),
                published.message.message_id,
            )
            .await
            .unwrap();
        store
            .read_artifact(
                ActorRef::Orchestrator(web.instance.instance_id),
                published.artifact.unwrap().artifact_id,
            )
            .await
            .unwrap();
        let after = store.get_instance(web.instance.instance_id).await.unwrap();
        assert_eq!(before.last_heartbeat_at, after.last_heartbeat_at);
        assert_eq!(before.lease_expires_at, after.lease_expires_at);

        store.pool.close().await;
        remove_database(&path);
    }

    #[tokio::test]
    async fn auditor_batches_items_and_enforces_per_item_depth() {
        let (store, path) = test_store(90).await;
        let connected = store
            .connect_orchestrator(connect_command("project", "backend", "C:/project/backend"))
            .await
            .unwrap();
        let first_item = WorkItemId::new();
        let second_item = WorkItemId::new();
        let audit_items = || {
            vec![
                AuditWorkItemInput {
                    work_item_id: first_item,
                    max_iterations: 2,
                },
                AuditWorkItemInput {
                    work_item_id: second_item,
                    max_iterations: 2,
                },
            ]
        };

        let first_audit = store
            .create_worker_run(CreateWorkerRun {
                parent_instance_id: connected.instance.instance_id,
                role: WorkerRole::Auditor,
                task_summary: "Audit two completed work items".to_owned(),
                context_refs: json!([]),
                audit_work_items: audit_items(),
                addresses_audit_run_id: None,
            })
            .await
            .unwrap();
        assert_eq!(first_audit.audit_scope.len(), 2);
        assert!(
            first_audit
                .audit_scope
                .iter()
                .all(|item| item.iteration == 1 && item.max_iterations == 2)
        );
        store
            .activate_worker_run(first_audit.worker_run_id)
            .await
            .unwrap();
        store
            .complete_worker_run(first_audit.worker_run_id, "Found one issue")
            .await
            .unwrap();

        let remediation = store
            .create_worker_run(CreateWorkerRun {
                parent_instance_id: connected.instance.instance_id,
                role: WorkerRole::Executor,
                task_summary: "Address the complete audit report".to_owned(),
                context_refs: json!([]),
                audit_work_items: Vec::new(),
                addresses_audit_run_id: Some(first_audit.worker_run_id),
            })
            .await
            .unwrap();
        assert_eq!(
            remediation.addresses_audit_run_id,
            Some(first_audit.worker_run_id)
        );
        assert!(matches!(
            store
                .create_worker_run(CreateWorkerRun {
                    parent_instance_id: connected.instance.instance_id,
                    role: WorkerRole::Executor,
                    task_summary: "Second remediation must be rejected".to_owned(),
                    context_refs: json!([]),
                    audit_work_items: Vec::new(),
                    addresses_audit_run_id: Some(first_audit.worker_run_id),
                })
                .await,
            Err(StoreError::Conflict(_))
        ));

        let second_audit = store
            .create_worker_run(CreateWorkerRun {
                parent_instance_id: connected.instance.instance_id,
                role: WorkerRole::Auditor,
                task_summary: "Audit remediation".to_owned(),
                context_refs: json!([]),
                audit_work_items: audit_items(),
                addresses_audit_run_id: None,
            })
            .await
            .unwrap();
        assert!(
            second_audit
                .audit_scope
                .iter()
                .all(|item| item.iteration == 2)
        );
        assert!(matches!(
            store
                .create_worker_run(CreateWorkerRun {
                    parent_instance_id: connected.instance.instance_id,
                    role: WorkerRole::Auditor,
                    task_summary: "Depth is exhausted".to_owned(),
                    context_refs: json!([]),
                    audit_work_items: audit_items(),
                    addresses_audit_run_id: None,
                })
                .await,
            Err(StoreError::Conflict(_))
        ));

        store.pool.close().await;
        remove_database(&path);
    }

    #[tokio::test]
    async fn audit_scope_is_only_valid_for_auditor_role() {
        let (store, path) = test_store(90).await;
        let connected = store
            .connect_orchestrator(connect_command("project", "backend", "C:/project/backend"))
            .await
            .unwrap();
        let item = AuditWorkItemInput {
            work_item_id: WorkItemId::new(),
            max_iterations: 1,
        };

        assert!(matches!(
            store
                .create_worker_run(CreateWorkerRun {
                    parent_instance_id: connected.instance.instance_id,
                    role: WorkerRole::Executor,
                    task_summary: "Executor cannot declare audit scope".to_owned(),
                    context_refs: json!([]),
                    audit_work_items: vec![item],
                    addresses_audit_run_id: None,
                })
                .await,
            Err(StoreError::Conflict(_))
        ));
        assert!(matches!(
            store
                .create_worker_run(CreateWorkerRun {
                    parent_instance_id: connected.instance.instance_id,
                    role: WorkerRole::Auditor,
                    task_summary: "Auditor requires at least one item".to_owned(),
                    context_refs: json!([]),
                    audit_work_items: Vec::new(),
                    addresses_audit_run_id: None,
                })
                .await,
            Err(StoreError::Conflict(_))
        ));

        store.pool.close().await;
        remove_database(&path);
    }

    #[tokio::test]
    async fn queued_message_is_returned_as_repository_event() {
        let (store, path) = test_store(90).await;
        let backend = store
            .connect_orchestrator(connect_command("project", "backend", "C:/project/backend"))
            .await
            .unwrap();
        let web = store
            .connect_orchestrator(connect_command("project", "web", "C:/project/web"))
            .await
            .unwrap();
        let published = store
            .publish_message(publish_command(
                ActorRef::Orchestrator(backend.instance.instance_id),
                MessageTarget::Repositories(vec![web.repository.repository_id]),
                "Contract ready",
                None,
            ))
            .await
            .unwrap();

        let filtered = store
            .wait_for_events(
                ActorRef::Orchestrator(web.instance.instance_id),
                0,
                &[EventKind::WorkerRunCompleted],
                Some(Duration::ZERO),
                20,
            )
            .await
            .unwrap();
        assert_eq!(filtered.outcome, WaitOutcome::Timeout);
        assert_eq!(filtered.next_cursor, 0);

        let batch = store
            .wait_for_events(
                ActorRef::Orchestrator(web.instance.instance_id),
                0,
                &[EventKind::MessageAvailable],
                Some(Duration::ZERO),
                20,
            )
            .await
            .unwrap();

        assert_eq!(batch.outcome, WaitOutcome::Events);
        assert_eq!(batch.events.len(), 1);
        assert_eq!(batch.events[0].kind, EventKind::MessageAvailable);
        assert_eq!(
            batch.events[0].entity_id,
            published.message.message_id.to_string()
        );
        assert_eq!(batch.next_cursor, batch.events[0].event_id);

        store.pool.close().await;
        remove_database(&path);
    }

    #[tokio::test]
    async fn waiter_is_notified_after_message_commit() {
        let (store, path) = test_store(90).await;
        let backend = store
            .connect_orchestrator(connect_command("project", "backend", "C:/project/backend"))
            .await
            .unwrap();
        let web = store
            .connect_orchestrator(connect_command("project", "web", "C:/project/web"))
            .await
            .unwrap();
        let web_instance_id = web.instance.instance_id;
        let web_repository_id = web.repository.repository_id;
        let waiting_store = store.clone();
        let waiter = tokio::spawn(async move {
            waiting_store
                .wait_for_events(
                    ActorRef::Orchestrator(web_instance_id),
                    0,
                    &[],
                    Some(Duration::from_secs(2)),
                    20,
                )
                .await
        });
        tokio::task::yield_now().await;

        store
            .publish_message(publish_command(
                ActorRef::Orchestrator(backend.instance.instance_id),
                MessageTarget::Repositories(vec![web_repository_id]),
                "Wake waiter",
                None,
            ))
            .await
            .unwrap();
        let batch = waiter.await.unwrap().unwrap();

        assert_eq!(batch.outcome, WaitOutcome::Events);
        assert_eq!(batch.events.len(), 1);

        store.pool.close().await;
        remove_database(&path);
    }

    #[tokio::test]
    async fn wait_timeout_preserves_cursor_and_renews_lease() {
        let (store, path) = test_store(1).await;
        let connected = store
            .connect_orchestrator(connect_command("project", "backend", "C:/project/backend"))
            .await
            .unwrap();
        let initial_heartbeat = connected.instance.last_heartbeat_at;

        let batch = store
            .wait_for_events(
                ActorRef::Orchestrator(connected.instance.instance_id),
                42,
                &[],
                Some(Duration::from_millis(1_500)),
                20,
            )
            .await
            .unwrap();
        let renewed = store
            .get_instance(connected.instance.instance_id)
            .await
            .unwrap();

        assert_eq!(batch.outcome, WaitOutcome::Timeout);
        assert_eq!(batch.next_cursor, 42);
        assert!(batch.events.is_empty());
        assert!(renewed.last_heartbeat_at > initial_heartbeat);
        assert!(renewed.lease_expires_at > utc_now());

        store.pool.close().await;
        remove_database(&path);
    }
}
