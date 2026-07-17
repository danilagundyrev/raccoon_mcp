use super::*;

impl SqliteStore {
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
        renew_request_claims_for_owner(
            &mut transaction,
            "claimed_by_instance_id",
            &instance.instance_id.to_string(),
            self.request_claim_lease_duration,
            &now,
        )
        .await?;
        renew_work_item_claims_for_owner(
            &mut transaction,
            instance.instance_id,
            self.work_item_claim_lease_duration,
            &now,
        )
        .await?;

        let repositories = list_repositories_tx(&mut transaction, project.project_id, &now).await?;
        let bootstrap = sqlx::query(
            "SELECT
                (SELECT COUNT(*) FROM features f
                 WHERE f.project_id = ? AND f.status = 'active') AS active_feature_count,
                (SELECT COUNT(*) FROM deliveries
                 WHERE recipient_repository_id = ? AND status = 'pending') AS pending_inbox_count,
                (SELECT COUNT(*) FROM requests
                 WHERE target_repository_id = ? AND status IN ('pending', 'claimed')) AS open_request_count,
                COALESCE((SELECT seq FROM sqlite_sequence WHERE name = 'events'), 0)
                    AS snapshot_event_cursor",
        )
        .bind(project.project_id.to_string())
        .bind(repository.repository_id.to_string())
        .bind(repository.repository_id.to_string())
        .fetch_one(&mut *transaction)
        .await?;
        transaction.commit().await?;

        Ok(ConnectResult {
            project,
            repository,
            instance,
            repositories,
            active_feature_count: bootstrap.try_get::<i64, _>("active_feature_count")? as u64,
            pending_inbox_count: bootstrap.try_get::<i64, _>("pending_inbox_count")? as u64,
            open_request_count: bootstrap.try_get::<i64, _>("open_request_count")? as u64,
            snapshot_event_cursor: bootstrap.try_get("snapshot_event_cursor")?,
            warnings: warning.into_iter().collect(),
            next_actions: vec![
                "Call heartbeat before the returned lease expires.".to_owned(),
                "Inspect the project overview to discover peer repositories.".to_owned(),
                "Use snapshot_event_cursor as the recovery watermark before starting event waits."
                    .to_owned(),
                "Before spawning a child, call create_worker_run and pass its worker_run_id to the child."
                    .to_owned(),
                "Use list_inbox for compact pending headers and read_artifact only when full content is needed."
                    .to_owned(),
            ],
        })
    }

    pub async fn heartbeat(&self, instance_id: InstanceId) -> Result<AgentInstance, StoreError> {
        let _write_guard = self.write_lock.lock().await;
        let mut transaction = self.pool.begin().await?;
        let row = sqlx::query("SELECT status FROM agent_instances WHERE instance_id = ?")
            .bind(instance_id.to_string())
            .fetch_optional(&mut *transaction)
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
        .execute(&mut *transaction)
        .await?;
        renew_request_claims_for_owner(
            &mut transaction,
            "claimed_by_instance_id",
            &instance_id.to_string(),
            self.request_claim_lease_duration,
            &now,
        )
        .await?;
        renew_work_item_claims_for_owner(
            &mut transaction,
            instance_id,
            self.work_item_claim_lease_duration,
            &now,
        )
        .await?;
        transaction.commit().await?;

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

    pub async fn list_repositories(
        &self,
        project_id: ProjectId,
        presence_filter: RepositoryPresenceFilter,
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

        let mut repositories = rows
            .iter()
            .map(|row| {
                Ok(RepositoryPresence {
                    repository: repository_from_row(row)?,
                    online_instances: row.try_get::<i64, _>("online_instances")? as u64,
                })
            })
            .collect::<Result<Vec<_>, StoreError>>()?;
        repositories.retain(|presence| presence_filter.matches(presence.online_instances));
        Ok(repositories)
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
        let repositories = self
            .list_repositories(project_id, RepositoryPresenceFilter::All)
            .await?;
        Ok(ProjectOverview {
            project,
            repositories,
        })
    }
}

async fn list_repositories_tx(
    transaction: &mut Transaction<'_, Sqlite>,
    project_id: ProjectId,
    now: &str,
) -> Result<Vec<RepositoryPresence>, StoreError> {
    let rows = sqlx::query(
        "SELECT r.*, COUNT(i.instance_id) AS online_instances
         FROM repositories r
         LEFT JOIN agent_instances i
           ON i.repository_id = r.repository_id
          AND i.status = 'online'
          AND i.lease_expires_at > ?
         WHERE r.project_id = ?
         GROUP BY r.repository_id
         ORDER BY r.repository_key",
    )
    .bind(now)
    .bind(project_id.to_string())
    .fetch_all(&mut **transaction)
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
