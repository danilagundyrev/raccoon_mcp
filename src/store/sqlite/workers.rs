use super::*;

impl SqliteStore {
    #[cfg(test)]
    pub async fn create_worker_run(
        &self,
        command: CreateWorkerRun,
    ) -> Result<WorkerRun, StoreError> {
        self.create_worker_run_idempotent(command, None).await
    }

    pub async fn create_worker_run_idempotent(
        &self,
        command: CreateWorkerRun,
        idempotency_key: Option<&str>,
    ) -> Result<WorkerRun, StoreError> {
        let _write_guard = self.write_lock.lock().await;
        validate_worker_run_command(&command)?;
        let request_fingerprint = worker_run_fingerprint(&command)?;
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
        let actor_key = format!("orchestrator:{}", command.parent_instance_id);
        sqlx::query(
            "UPDATE agent_instances
             SET status = 'online', last_heartbeat_at = ?, lease_expires_at = ?
             WHERE instance_id = ?",
        )
        .bind(&now)
        .bind(&orchestrator_lease_expires_at)
        .bind(command.parent_instance_id.to_string())
        .execute(&mut *transaction)
        .await?;
        renew_request_claims_for_owner(
            &mut transaction,
            "claimed_by_instance_id",
            &command.parent_instance_id.to_string(),
            self.request_claim_lease_duration,
            &now,
        )
        .await?;
        renew_work_item_claims_for_owner(
            &mut transaction,
            command.parent_instance_id,
            self.work_item_claim_lease_duration,
            &now,
        )
        .await?;
        if let Some(existing_id) = find_idempotent_result(
            &mut transaction,
            &actor_key,
            "create_worker_run",
            idempotency_key,
            &request_fingerprint,
        )
        .await?
        {
            transaction.commit().await?;
            return self.get_worker_run(WorkerRunId::parse(&existing_id)?).await;
        }
        let project_id = ProjectId::parse(
            sqlx::query("SELECT project_id FROM repositories WHERE repository_id = ?")
                .bind(repository_id.to_string())
                .fetch_one(&mut *transaction)
                .await?
                .try_get("project_id")?,
        )?;
        let audit_scope = prepare_audit_scope(&mut transaction, &command, project_id).await?;
        if let Some(audit_run_id) = command.addresses_audit_run_id {
            validate_remediation_audit(&mut transaction, repository_id, audit_run_id).await?;
        }

        sqlx::query(
            "INSERT INTO worker_runs(
                 worker_run_id, repository_id, parent_instance_id, role, status,
                 task_summary, context_refs_json, result_summary, error_summary, addresses_audit_run_id,
                 work_item_id,
                 created_at, activated_at, last_heartbeat_at, lease_expires_at, finished_at
             ) VALUES (?, ?, ?, ?, 'created', ?, ?, NULL, NULL, ?, ?, ?, NULL, ?, ?, NULL)",
        )
        .bind(worker_run_id.to_string())
        .bind(repository_id.to_string())
        .bind(command.parent_instance_id.to_string())
        .bind(command.role.as_str())
        .bind(&command.task_summary)
        .bind(&context_refs_json)
        .bind(command.addresses_audit_run_id.map(|id| id.to_string()))
        .bind(command.work_item_id.map(|id| id.to_string()))
        .bind(&now)
        .bind(&now)
        .bind(&worker_lease_expires_at)
        .execute(&mut *transaction)
        .await?;

        let work_item_recipients = super::work::attach_worker_to_work_item(
            &mut transaction,
            &command,
            repository_id,
            project_id,
            &now,
        )
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
        record_idempotent_result(
            &mut transaction,
            &actor_key,
            "create_worker_run",
            idempotency_key,
            &request_fingerprint,
            &worker_run_id.to_string(),
            &now,
        )
        .await?;

        transaction.commit().await?;
        self.notifications.notify(repository_id);
        for recipient_id in work_item_recipients {
            self.notifications.notify(recipient_id);
        }
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
        let mut transaction = self.pool.begin().await?;
        let result = sqlx::query(
            "UPDATE worker_runs
             SET last_heartbeat_at = ?, lease_expires_at = ?
             WHERE worker_run_id = ? AND status = 'active' AND lease_expires_at > ?",
        )
        .bind(&now)
        .bind(lease_expires_at)
        .bind(worker_run_id.to_string())
        .bind(&now)
        .execute(&mut *transaction)
        .await?;
        if result.rows_affected() == 0 {
            transaction.rollback().await?;
            let worker = self.get_worker_run(worker_run_id).await?;
            return Err(invalid_worker_transition(
                worker_run_id,
                worker.status,
                "heartbeat",
            ));
        }
        renew_request_claims_for_owner(
            &mut transaction,
            "claimed_by_worker_run_id",
            &worker_run_id.to_string(),
            self.request_claim_lease_duration,
            &now,
        )
        .await?;
        transaction.commit().await?;
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
        let now = utc_now();
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
        if worker.lease_expires_at.as_str() <= now.as_str() {
            return Err(expired_worker_lease(worker_run_id));
        }

        if target_status == WorkerRunStatus::Completed {
            let required_artifact_kind = match (worker.role, worker.work_item_id) {
                (WorkerRole::Auditor, _) => Some("audit_report"),
                (WorkerRole::Executor, Some(_)) => Some("implementation_report"),
                _ => None,
            };
            if let Some(required_artifact_kind) = required_artifact_kind {
                let artifact_exists: i64 = sqlx::query_scalar(
                    "SELECT EXISTS(
                         SELECT 1 FROM artifacts
                         WHERE producer_worker_run_id = ? AND kind = ?
                     )",
                )
                .bind(worker_run_id.to_string())
                .bind(required_artifact_kind)
                .fetch_one(&self.pool)
                .await?;
                if artifact_exists == 0 {
                    return Err(StoreError::Conflict(format!(
                        "completing this worker run requires a {required_artifact_kind} artifact"
                    )));
                }
            }
        }

        let mut transaction = self.pool.begin().await?;
        let result = sqlx::query(
            "UPDATE worker_runs
             SET status = ?, result_summary = ?, error_summary = ?,
                 last_heartbeat_at = ?, lease_expires_at = ?, finished_at = ?
             WHERE worker_run_id = ?
               AND lease_expires_at > ?
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
        .bind(&now)
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
}
