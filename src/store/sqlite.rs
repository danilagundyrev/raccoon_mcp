use std::{path::Path, str::FromStr, time::Duration};

use chrono::{SecondsFormat, Utc};
use sqlx::{
    Row, Sqlite, SqlitePool, Transaction,
    sqlite::{
        SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteRow, SqliteSynchronous,
    },
};
use thiserror::Error;

use crate::domain::{
    AgentInstance, ConnectOrchestrator, ConnectResult, CreateWorkerRun, InstanceId, PresenceStatus,
    Project, ProjectId, ProjectOverview, Repository, RepositoryId, RepositoryPresence, WorkerRun,
    WorkerRunId, WorkerRunStatus,
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
                "Mailbox and message tools will be introduced in the next implementation step."
                    .to_owned(),
            ],
        })
    }

    pub async fn heartbeat(&self, instance_id: InstanceId) -> Result<AgentInstance, StoreError> {
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
                 task_summary, context_refs_json, result_summary, error_summary,
                 created_at, activated_at, last_heartbeat_at, lease_expires_at, finished_at
             ) VALUES (?, ?, ?, ?, 'created', ?, ?, NULL, NULL, ?, NULL, ?, ?, NULL)",
        )
        .bind(worker_run_id.to_string())
        .bind(repository_id.to_string())
        .bind(command.parent_instance_id.to_string())
        .bind(command.role.as_str())
        .bind(&command.task_summary)
        .bind(&context_refs_json)
        .bind(&now)
        .bind(&now)
        .bind(&worker_lease_expires_at)
        .execute(&mut *transaction)
        .await?;

        transaction.commit().await?;
        self.get_worker_run(worker_run_id).await
    }

    pub async fn activate_worker_run(
        &self,
        worker_run_id: WorkerRunId,
    ) -> Result<WorkerRun, StoreError> {
        let worker = self.get_worker_run(worker_run_id).await?;
        match worker.status {
            WorkerRunStatus::Created => {
                let now = utc_now();
                let lease_expires_at = lease_deadline(self.worker_lease_duration);
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
                .execute(&self.pool)
                .await?;
                self.get_worker_run(worker_run_id).await
            }
            WorkerRunStatus::Active => self.heartbeat_worker_run(worker_run_id).await,
            status => Err(invalid_worker_transition(worker_run_id, status, "active")),
        }
    }

    pub async fn heartbeat_worker_run(
        &self,
        worker_run_id: WorkerRunId,
    ) -> Result<WorkerRun, StoreError> {
        let now = utc_now();
        let lease_expires_at = lease_deadline(self.worker_lease_duration);
        let result = sqlx::query(
            "UPDATE worker_runs
             SET last_heartbeat_at = ?, lease_expires_at = ?
             WHERE worker_run_id = ? AND status = 'active'",
        )
        .bind(&now)
        .bind(lease_expires_at)
        .bind(worker_run_id.to_string())
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
        worker_run_from_row(&row)
    }

    async fn finish_worker_run(
        &self,
        worker_run_id: WorkerRunId,
        target_status: WorkerRunStatus,
        result_summary: Option<&str>,
        error_summary: Option<&str>,
    ) -> Result<WorkerRun, StoreError> {
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
        .execute(&self.pool)
        .await?;
        let current = self.get_worker_run(worker_run_id).await?;
        if result.rows_affected() == 1 || current.status == target_status {
            Ok(current)
        } else {
            Err(invalid_worker_transition(
                worker_run_id,
                current.status,
                target_status.as_str(),
            ))
        }
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
        created_at: row.try_get("created_at")?,
        activated_at: row.try_get("activated_at")?,
        last_heartbeat_at: row.try_get("last_heartbeat_at")?,
        lease_expires_at: row.try_get("lease_expires_at")?,
        finished_at: row.try_get("finished_at")?,
    })
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
    use crate::domain::WorkerRole;

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
}
