use super::*;

impl SqliteStore {
    pub async fn readiness_check(&self) -> Result<(), StoreError> {
        self.health_check().await
    }

    pub async fn operational_metrics(&self) -> Result<OperationalMetrics, StoreError> {
        let now = utc_now();
        let row = sqlx::query(
            "SELECT
                (SELECT COUNT(*) FROM agent_instances
                 WHERE status = 'online' AND lease_expires_at > ?) AS online_orchestrators,
                (SELECT COUNT(DISTINCT repository_id) FROM agent_instances
                 WHERE status = 'online' AND lease_expires_at > ?) AS online_repositories,
                (SELECT COUNT(*) FROM worker_runs
                 WHERE status = 'active' AND lease_expires_at > ?) AS active_workers,
                (SELECT COUNT(*) FROM deliveries WHERE status = 'pending') AS pending_deliveries,
                (SELECT COUNT(*) FROM requests WHERE status IN ('pending', 'claimed')) AS open_requests,
                (SELECT COUNT(*) FROM requests
                 WHERE status = 'claimed' AND claim_expires_at <= ?) AS expired_request_claims,
                (SELECT COUNT(*) FROM work_items
                 WHERE status IN ('claimed', 'in_progress', 'blocked')
                   AND claim_expires_at > ?) AS actively_claimed_work_items,
                (SELECT MIN(created_at) FROM deliveries WHERE status = 'pending')
                    AS oldest_pending_delivery_at",
        )
        .bind(&now)
        .bind(&now)
        .bind(&now)
        .bind(&now)
        .bind(&now)
        .fetch_one(&self.pool)
        .await?;
        let oldest_pending_delivery_at: Option<String> =
            row.try_get("oldest_pending_delivery_at")?;
        let oldest_pending_delivery_age_seconds = oldest_pending_delivery_at
            .as_deref()
            .and_then(|created_at| chrono::DateTime::parse_from_rfc3339(created_at).ok())
            .map(|created_at| {
                Utc::now()
                    .signed_duration_since(created_at)
                    .num_seconds()
                    .max(0) as u64
            });
        Ok(OperationalMetrics {
            online_orchestrators: row.try_get::<i64, _>("online_orchestrators")? as u64,
            online_repositories: row.try_get::<i64, _>("online_repositories")? as u64,
            active_workers: row.try_get::<i64, _>("active_workers")? as u64,
            pending_deliveries: row.try_get::<i64, _>("pending_deliveries")? as u64,
            open_requests: row.try_get::<i64, _>("open_requests")? as u64,
            expired_request_claims: row.try_get::<i64, _>("expired_request_claims")? as u64,
            actively_claimed_work_items: row.try_get::<i64, _>("actively_claimed_work_items")?
                as u64,
            oldest_pending_delivery_at,
            oldest_pending_delivery_age_seconds,
        })
    }

    pub async fn run_lease_maintenance(&self) -> Result<LeaseMaintenanceReport, StoreError> {
        let _write_guard = self.write_lock.lock().await;
        let now = utc_now();
        let mut transaction = self.pool.begin().await?;

        let expired_workers = sqlx::query(
            "UPDATE worker_runs
             SET status = 'abandoned', finished_at = COALESCE(finished_at, ?)
             WHERE status IN ('created', 'active') AND lease_expires_at <= ?
             RETURNING worker_run_id, repository_id,
                 (SELECT project_id FROM repositories
                  WHERE repository_id = worker_runs.repository_id) AS project_id",
        )
        .bind(&now)
        .bind(&now)
        .fetch_all(&mut *transaction)
        .await?;
        let expired_requests = sqlx::query(
            "UPDATE requests
             SET status = 'pending', claimed_by_instance_id = NULL,
                 claimed_by_worker_run_id = NULL, claim_expires_at = NULL, updated_at = ?
             WHERE status = 'claimed' AND claim_expires_at <= ?
             RETURNING request_id, project_id, target_repository_id",
        )
        .bind(&now)
        .bind(&now)
        .fetch_all(&mut *transaction)
        .await?;
        let abandoned_workers = expired_workers.len() as u64;
        let requeued_requests = expired_requests.len() as u64;
        let disconnected_orchestrators = sqlx::query(
            "UPDATE agent_instances
             SET status = 'disconnected', disconnected_at = COALESCE(disconnected_at, ?)
             WHERE status = 'online' AND lease_expires_at <= ?",
        )
        .bind(&now)
        .bind(&now)
        .execute(&mut *transaction)
        .await?
        .rows_affected();

        let mut recipients = Vec::new();
        for row in expired_workers {
            let worker_run_id: String = row.try_get("worker_run_id")?;
            let repository_id = RepositoryId::parse(row.try_get("repository_id")?)?;
            let project_id = ProjectId::parse(row.try_get("project_id")?)?;
            append_event(
                &mut transaction,
                project_id,
                repository_id,
                EventKind::WorkerRunFailed,
                EventEntityType::WorkerRun,
                &worker_run_id,
                &now,
            )
            .await?;
            recipients.push(repository_id);
        }
        for row in expired_requests {
            let request_id: String = row.try_get("request_id")?;
            let project_id = ProjectId::parse(row.try_get("project_id")?)?;
            let repository_id = RepositoryId::parse(row.try_get("target_repository_id")?)?;
            append_event(
                &mut transaction,
                project_id,
                repository_id,
                EventKind::RequestAvailable,
                EventEntityType::Request,
                &request_id,
                &now,
            )
            .await?;
            recipients.push(repository_id);
        }
        transaction.commit().await?;
        self.notify_repositories(&recipients);
        Ok(LeaseMaintenanceReport {
            disconnected_orchestrators,
            abandoned_workers,
            requeued_requests,
        })
    }

    pub async fn run_retention(
        &self,
        retention: Duration,
        batch_size: u16,
    ) -> Result<RetentionReport, StoreError> {
        if batch_size == 0 {
            return Err(StoreError::Conflict(
                "retention batch size must be positive".to_owned(),
            ));
        }
        let _write_guard = self.write_lock.lock().await;
        let cutoff = (Utc::now()
            - chrono::Duration::from_std(retention)
                .unwrap_or_else(|_| chrono::Duration::seconds(i64::MAX)))
        .to_rfc3339_opts(SecondsFormat::Micros, true);
        let mut transaction = self.pool.begin().await?;
        let deleted_events = sqlx::query(
            "DELETE FROM events WHERE event_id IN (
                 SELECT event_id FROM events
                 WHERE created_at < ? ORDER BY created_at, event_id LIMIT ?
             )",
        )
        .bind(&cutoff)
        .bind(i64::from(batch_size))
        .execute(&mut *transaction)
        .await?
        .rows_affected();
        let deleted_idempotency_records = sqlx::query(
            "DELETE FROM idempotency_records WHERE rowid IN (
                 SELECT rowid FROM idempotency_records
                 WHERE created_at < ? ORDER BY created_at, rowid LIMIT ?
             )",
        )
        .bind(&cutoff)
        .bind(i64::from(batch_size))
        .execute(&mut *transaction)
        .await?
        .rows_affected();
        transaction.commit().await?;
        Ok(RetentionReport {
            deleted_events,
            deleted_idempotency_records,
        })
    }

    pub async fn expire_local_leases(&self) -> Result<(), StoreError> {
        let _write_guard = self.write_lock.lock().await;
        let now = utc_now();
        let mut transaction = self.pool.begin().await?;
        sqlx::query(
            "UPDATE agent_instances SET lease_expires_at = ?
             WHERE status = 'online' AND lease_expires_at > ?",
        )
        .bind(&now)
        .bind(&now)
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            "UPDATE worker_runs SET lease_expires_at = ?
             WHERE status IN ('created', 'active') AND lease_expires_at > ?",
        )
        .bind(&now)
        .bind(&now)
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            "UPDATE requests SET claim_expires_at = ?
             WHERE status = 'claimed' AND claim_expires_at > ?",
        )
        .bind(&now)
        .bind(&now)
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            "UPDATE work_items SET claim_expires_at = ?
             WHERE status IN ('claimed', 'in_progress', 'blocked') AND claim_expires_at > ?",
        )
        .bind(&now)
        .bind(&now)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(())
    }
}
