use std::collections::BTreeSet;

use super::*;

impl SqliteStore {
    #[cfg(test)]
    pub async fn create_feature(&self, command: CreateFeature) -> Result<Feature, StoreError> {
        self.create_feature_idempotent(command, None).await
    }

    pub async fn create_feature_idempotent(
        &self,
        command: CreateFeature,
        idempotency_key: Option<&str>,
    ) -> Result<Feature, StoreError> {
        let _write_guard = self.write_lock.lock().await;
        let now = utc_now();
        let feature_id = FeatureId::new();
        let fingerprint = payload_digest(&serde_json::json!({
            "title": command.title,
            "summary": command.summary,
        }))?;
        let mut transaction = self.pool.begin().await?;
        let actor = self
            .resolve_actor(
                &mut transaction,
                ActorRef::Orchestrator(command.parent_instance_id),
                &now,
            )
            .await?;
        let actor_key = resolved_actor_key(&actor);
        if let Some(existing_id) = find_idempotent_result(
            &mut transaction,
            &actor_key,
            "create_feature",
            idempotency_key,
            &fingerprint,
        )
        .await?
        {
            transaction.commit().await?;
            return self.get_feature(FeatureId::parse(&existing_id)?).await;
        }
        sqlx::query(
            "INSERT INTO features(
                 feature_id, project_id, created_by_instance_id, coordinator_repository_id,
                 title, summary, status, revision, created_at, updated_at,
                 completed_at, cancelled_at
             ) VALUES (?, ?, ?, ?, ?, ?, 'active', 1, ?, ?, NULL, NULL)",
        )
        .bind(feature_id.to_string())
        .bind(actor.project_id.to_string())
        .bind(command.parent_instance_id.to_string())
        .bind(actor.repository_id.to_string())
        .bind(command.title)
        .bind(command.summary)
        .bind(&now)
        .bind(&now)
        .execute(&mut *transaction)
        .await?;
        let recipients = append_project_event(
            &mut transaction,
            actor.project_id,
            EventKind::FeatureCreated,
            EventEntityType::Feature,
            &feature_id.to_string(),
            &now,
        )
        .await?;
        record_idempotent_result(
            &mut transaction,
            &actor_key,
            "create_feature",
            idempotency_key,
            &fingerprint,
            &feature_id.to_string(),
            &now,
        )
        .await?;
        transaction.commit().await?;
        self.notify_repositories(&recipients);
        self.get_feature(feature_id).await
    }

    pub async fn get_feature(&self, feature_id: FeatureId) -> Result<Feature, StoreError> {
        let row = sqlx::query("SELECT * FROM features WHERE feature_id = ?")
            .bind(feature_id.to_string())
            .fetch_optional(&self.pool)
            .await?
            .ok_or_else(|| StoreError::NotFound(format!("feature {feature_id}")))?;
        feature_from_row(&row)
    }

    pub async fn list_features(
        &self,
        project_id: ProjectId,
        status: Option<FeatureStatus>,
        after_feature_id: Option<FeatureId>,
        limit: u8,
    ) -> Result<FeaturePage, StoreError> {
        validate_history_limit(limit)?;
        let mut transaction = self.pool.begin().await?;
        let cursor = if let Some(after_feature_id) = after_feature_id {
            Some(
                sqlx::query(
                    "SELECT created_at, feature_id FROM features
                     WHERE project_id = ? AND feature_id = ?",
                )
                .bind(project_id.to_string())
                .bind(after_feature_id.to_string())
                .fetch_optional(&mut *transaction)
                .await?
                .ok_or_else(|| {
                    StoreError::NotFound(format!(
                        "feature cursor {after_feature_id} in project {project_id}"
                    ))
                })?,
            )
        } else {
            None
        };
        let mut query = QueryBuilder::<Sqlite>::new("SELECT * FROM features WHERE project_id = ");
        query.push_bind(project_id.to_string());
        if let Some(status) = status {
            query.push(" AND status = ").push_bind(status.as_str());
        }
        if let Some(cursor) = cursor.as_ref() {
            let created_at: String = cursor.try_get("created_at")?;
            let feature_id: String = cursor.try_get("feature_id")?;
            query
                .push(" AND (created_at > ")
                .push_bind(created_at.clone())
                .push(" OR (created_at = ")
                .push_bind(created_at)
                .push(" AND feature_id > ")
                .push_bind(feature_id)
                .push("))");
        }
        query
            .push(" ORDER BY created_at, feature_id LIMIT ")
            .push_bind(i64::from(limit) + 1);
        let rows = query.build().fetch_all(&mut *transaction).await?;
        let has_more = rows.len() > usize::from(limit);
        let items = rows
            .iter()
            .take(usize::from(limit))
            .map(feature_from_row)
            .collect::<Result<Vec<_>, _>>()?;
        transaction.commit().await?;
        Ok(FeaturePage {
            next_cursor: items.last().map(|feature| feature.feature_id),
            items,
            has_more,
        })
    }

    pub async fn complete_feature(&self, command: CompleteFeature) -> Result<Feature, StoreError> {
        let _write_guard = self.write_lock.lock().await;
        let now = utc_now();
        let mut transaction = self.pool.begin().await?;
        let actor = self
            .resolve_actor(
                &mut transaction,
                ActorRef::Orchestrator(command.parent_instance_id),
                &now,
            )
            .await?;
        let feature = get_feature_tx(&mut transaction, command.feature_id).await?;
        ensure_feature_actor(&feature, &actor)?;
        ensure_feature_revision(&feature, command.expected_revision)?;
        if feature.status != FeatureStatus::Active {
            return Err(StoreError::Conflict(format!(
                "feature {} is already {}",
                feature.feature_id,
                feature.status.as_str()
            )));
        }
        ensure_feature_has_no_open_work(&mut transaction, feature.feature_id).await?;
        ensure_feature_has_no_open_requests(&mut transaction, feature.feature_id).await?;
        ensure_feature_has_no_active_workers(&mut transaction, feature.feature_id).await?;
        ensure_feature_implementation_is_verified(&mut transaction, feature.feature_id).await?;

        let updated = sqlx::query(
            "UPDATE features
             SET status = 'completed', revision = revision + 1,
                 updated_at = ?, completed_at = ?, cancelled_at = NULL
             WHERE feature_id = ? AND status = 'active' AND revision = ?",
        )
        .bind(&now)
        .bind(&now)
        .bind(feature.feature_id.to_string())
        .bind(feature.revision as i64)
        .execute(&mut *transaction)
        .await?;
        if updated.rows_affected() != 1 {
            return Err(StoreError::Conflict(format!(
                "feature {} changed while completing",
                feature.feature_id
            )));
        }
        append_project_event(
            &mut transaction,
            feature.project_id,
            EventKind::FeatureUpdated,
            EventEntityType::Feature,
            &feature.feature_id.to_string(),
            &now,
        )
        .await?;
        let recipients = append_project_event(
            &mut transaction,
            feature.project_id,
            EventKind::FeatureCompleted,
            EventEntityType::Feature,
            &feature.feature_id.to_string(),
            &now,
        )
        .await?;
        transaction.commit().await?;
        self.notify_repositories(&recipients);
        self.get_feature(feature.feature_id).await
    }

    pub async fn cancel_feature(&self, command: CancelFeature) -> Result<Feature, StoreError> {
        let _write_guard = self.write_lock.lock().await;
        let now = utc_now();
        let mut transaction = self.pool.begin().await?;
        let actor = self
            .resolve_actor(
                &mut transaction,
                ActorRef::Orchestrator(command.parent_instance_id),
                &now,
            )
            .await?;
        let feature = get_feature_tx(&mut transaction, command.feature_id).await?;
        ensure_feature_actor(&feature, &actor)?;
        ensure_feature_revision(&feature, command.expected_revision)?;
        if feature.status != FeatureStatus::Active {
            return Err(StoreError::Conflict(format!(
                "feature {} is already {}",
                feature.feature_id,
                feature.status.as_str()
            )));
        }
        ensure_feature_has_no_open_requests(&mut transaction, feature.feature_id).await?;
        ensure_feature_has_no_active_workers(&mut transaction, feature.feature_id).await?;
        let active_items: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM work_items
             WHERE feature_id = ? AND status IN ('claimed', 'in_progress', 'blocked')",
        )
        .bind(feature.feature_id.to_string())
        .fetch_one(&mut *transaction)
        .await?;
        if active_items != 0 {
            return Err(StoreError::Conflict(format!(
                "feature {} has {active_items} actively claimed work item(s)",
                feature.feature_id
            )));
        }

        let rows = sqlx::query(
            "SELECT work_item_id FROM work_items
             WHERE feature_id = ? AND status IN ('planned', 'ready', 'failed')",
        )
        .bind(feature.feature_id.to_string())
        .fetch_all(&mut *transaction)
        .await?;
        let cancelled_items = rows
            .iter()
            .map(|row| {
                let work_item_id: String = row.try_get("work_item_id")?;
                Ok::<WorkItemId, StoreError>(WorkItemId::parse(&work_item_id)?)
            })
            .collect::<Result<Vec<_>, _>>()?;
        sqlx::query(
            "UPDATE work_items
             SET status = 'cancelled', revision = revision + 1,
                 claim_expires_at = NULL, assignee_instance_id = NULL,
                 updated_at = ?, finished_at = ?
             WHERE feature_id = ? AND status IN ('planned', 'ready', 'failed')",
        )
        .bind(&now)
        .bind(&now)
        .bind(feature.feature_id.to_string())
        .execute(&mut *transaction)
        .await?;
        append_work_item_events(&mut transaction, feature.project_id, &cancelled_items, &now)
            .await?;
        let updated = sqlx::query(
            "UPDATE features
             SET status = 'cancelled', revision = revision + 1,
                 updated_at = ?, cancelled_at = ?, completed_at = NULL
             WHERE feature_id = ? AND status = 'active' AND revision = ?",
        )
        .bind(&now)
        .bind(&now)
        .bind(feature.feature_id.to_string())
        .bind(feature.revision as i64)
        .execute(&mut *transaction)
        .await?;
        if updated.rows_affected() != 1 {
            return Err(StoreError::Conflict(format!(
                "feature {} changed while cancelling",
                feature.feature_id
            )));
        }
        append_project_event(
            &mut transaction,
            feature.project_id,
            EventKind::FeatureUpdated,
            EventEntityType::Feature,
            &feature.feature_id.to_string(),
            &now,
        )
        .await?;
        let recipients = append_project_event(
            &mut transaction,
            feature.project_id,
            EventKind::FeatureCancelled,
            EventEntityType::Feature,
            &feature.feature_id.to_string(),
            &now,
        )
        .await?;
        transaction.commit().await?;
        self.notify_repositories(&recipients);
        self.get_feature(feature.feature_id).await
    }

    #[cfg(test)]
    pub async fn create_work_item(&self, command: CreateWorkItem) -> Result<WorkItem, StoreError> {
        self.create_work_item_idempotent(command, None).await
    }

    pub async fn create_work_item_idempotent(
        &self,
        command: CreateWorkItem,
        idempotency_key: Option<&str>,
    ) -> Result<WorkItem, StoreError> {
        validate_audit_depth(command.max_audit_iterations)?;
        let _write_guard = self.write_lock.lock().await;
        let now = utc_now();
        let work_item_id = WorkItemId::new();
        let fingerprint = payload_digest(&serde_json::json!({
            "feature_id": command.feature_id,
            "parent_work_item_id": command.parent_work_item_id,
            "owner_repository_id": command.owner_repository_id,
            "title": command.title,
            "summary": command.summary,
            "kind": command.kind,
            "audit_scope_work_item_ids": command.audit_scope_work_item_ids,
            "addresses_audit_run_id": command.addresses_audit_run_id,
            "dependency_ids": command.dependency_ids,
            "priority": command.priority,
            "max_audit_iterations": command.max_audit_iterations,
        }))?;
        let mut transaction = self.pool.begin().await?;
        let actor = self
            .resolve_actor(
                &mut transaction,
                ActorRef::Orchestrator(command.parent_instance_id),
                &now,
            )
            .await?;
        let actor_key = resolved_actor_key(&actor);
        if let Some(existing_id) = find_idempotent_result(
            &mut transaction,
            &actor_key,
            "create_work_item",
            idempotency_key,
            &fingerprint,
        )
        .await?
        {
            transaction.commit().await?;
            return self.get_work_item(WorkItemId::parse(&existing_id)?).await;
        }
        ensure_active_feature_project(&mut transaction, command.feature_id, actor.project_id)
            .await?;
        ensure_repository_project(
            &mut transaction,
            command.owner_repository_id,
            actor.project_id,
        )
        .await?;
        validate_work_item_kind_command(&command)?;
        validate_work_item_relations(
            &mut transaction,
            command.feature_id,
            None,
            command.parent_work_item_id,
            &command.dependency_ids,
        )
        .await?;
        let audit_scope = match command.kind {
            WorkItemKind::Implementation => Vec::new(),
            WorkItemKind::Audit => {
                prepare_audit_work_item_scope(
                    &mut transaction,
                    command.feature_id,
                    command.owner_repository_id,
                    &command.audit_scope_work_item_ids,
                )
                .await?
            }
            WorkItemKind::Remediation => {
                validate_remediation_audit(
                    &mut transaction,
                    command.owner_repository_id,
                    command.feature_id,
                    command
                        .addresses_audit_run_id
                        .expect("validated remediation link"),
                )
                .await?;
                Vec::new()
            }
        };
        let status = if dependencies_completed(&mut transaction, &command.dependency_ids).await? {
            WorkItemStatus::Ready
        } else {
            WorkItemStatus::Planned
        };
        sqlx::query(
            "INSERT INTO work_items(
                 work_item_id, feature_id, parent_work_item_id, owner_repository_id,
                 assignee_instance_id, title, summary, kind, addresses_audit_run_id,
                 priority, status, revision,
                 max_audit_iterations, claim_expires_at, implementation_artifact_id,
                 latest_audit_artifact_id, verification_status, created_at, updated_at, finished_at
             ) VALUES (?, ?, ?, ?, NULL, ?, ?, ?, ?, ?, ?, 1, ?, NULL, NULL, NULL, NULL, ?, ?, NULL)",
        )
        .bind(work_item_id.to_string())
        .bind(command.feature_id.to_string())
        .bind(command.parent_work_item_id.map(|id| id.to_string()))
        .bind(command.owner_repository_id.to_string())
        .bind(command.title)
        .bind(command.summary)
        .bind(command.kind.as_str())
        .bind(command.addresses_audit_run_id.map(|id| id.to_string()))
        .bind(i64::from(command.priority))
        .bind(status.as_str())
        .bind(i64::from(command.max_audit_iterations))
        .bind(&now)
        .bind(&now)
        .execute(&mut *transaction)
        .await?;
        replace_dependencies(&mut transaction, work_item_id, &command.dependency_ids).await?;
        for scope_item in audit_scope {
            sqlx::query(
                "INSERT INTO work_item_audit_scope(
                     audit_work_item_id, scoped_work_item_id, audit_iteration, max_iterations
                 ) VALUES (?, ?, ?, ?)",
            )
            .bind(work_item_id.to_string())
            .bind(scope_item.work_item_id.to_string())
            .bind(i64::from(scope_item.iteration))
            .bind(i64::from(scope_item.max_iterations))
            .execute(&mut *transaction)
            .await?;
        }
        let recipients = append_project_event(
            &mut transaction,
            actor.project_id,
            EventKind::WorkItemCreated,
            EventEntityType::WorkItem,
            &work_item_id.to_string(),
            &now,
        )
        .await?;
        record_idempotent_result(
            &mut transaction,
            &actor_key,
            "create_work_item",
            idempotency_key,
            &fingerprint,
            &work_item_id.to_string(),
            &now,
        )
        .await?;
        transaction.commit().await?;
        self.notify_repositories(&recipients);
        self.get_work_item(work_item_id).await
    }

    pub async fn get_work_item(&self, work_item_id: WorkItemId) -> Result<WorkItem, StoreError> {
        let mut transaction = self.pool.begin().await?;
        let item = get_work_item_tx(&mut transaction, work_item_id).await?;
        transaction.commit().await?;
        Ok(item)
    }

    pub async fn list_work_items(
        &self,
        feature_id: FeatureId,
        after_work_item_id: Option<WorkItemId>,
        limit: u8,
    ) -> Result<WorkItemPage, StoreError> {
        if !(1..=100).contains(&limit) {
            return Err(StoreError::Conflict(
                "work-item page limit must be between 1 and 100".to_owned(),
            ));
        }
        let mut transaction = self.pool.begin().await?;
        let cursor = if let Some(after_work_item_id) = after_work_item_id {
            Some(
                sqlx::query(
                    "SELECT priority, created_at, work_item_id FROM work_items
                     WHERE feature_id = ? AND work_item_id = ?",
                )
                .bind(feature_id.to_string())
                .bind(after_work_item_id.to_string())
                .fetch_optional(&mut *transaction)
                .await?
                .ok_or_else(|| {
                    StoreError::NotFound(format!(
                        "work-item cursor {after_work_item_id} in feature {feature_id}"
                    ))
                })?,
            )
        } else {
            None
        };
        let mut query = QueryBuilder::<Sqlite>::new("SELECT * FROM work_items WHERE feature_id = ");
        query.push_bind(feature_id.to_string());
        if let Some(cursor) = cursor {
            let priority: i64 = cursor.try_get("priority")?;
            let created_at: String = cursor.try_get("created_at")?;
            let work_item_id: String = cursor.try_get("work_item_id")?;
            query
                .push(" AND (priority < ")
                .push_bind(priority)
                .push(" OR (priority = ")
                .push_bind(priority)
                .push(" AND created_at > ")
                .push_bind(created_at.clone())
                .push(") OR (priority = ")
                .push_bind(priority)
                .push(" AND created_at = ")
                .push_bind(created_at)
                .push(" AND work_item_id > ")
                .push_bind(work_item_id)
                .push("))");
        }
        query
            .push(" ORDER BY priority DESC, created_at, work_item_id LIMIT ")
            .push_bind(i64::from(limit) + 1);
        let rows = query.build().fetch_all(&mut *transaction).await?;
        let has_more = rows.len() > usize::from(limit);
        let page_rows = rows.iter().take(usize::from(limit)).collect::<Vec<_>>();
        let mut dependencies = HashMap::<String, Vec<WorkItemId>>::new();
        let mut audit_scopes = HashMap::<String, Vec<WorkItemId>>::new();
        if !page_rows.is_empty() {
            let mut dependency_query = QueryBuilder::<Sqlite>::new(
                "SELECT work_item_id, dependency_work_item_id FROM work_item_dependencies WHERE work_item_id IN (",
            );
            let mut separated = dependency_query.separated(", ");
            for row in &page_rows {
                separated.push_bind(row.try_get::<String, _>("work_item_id")?);
            }
            separated.push_unseparated(") ORDER BY work_item_id, dependency_work_item_id");
            for dependency_row in dependency_query
                .build()
                .fetch_all(&mut *transaction)
                .await?
            {
                dependencies
                    .entry(dependency_row.try_get("work_item_id")?)
                    .or_default()
                    .push(WorkItemId::parse(
                        dependency_row.try_get("dependency_work_item_id")?,
                    )?);
            }
            let mut scope_query = QueryBuilder::<Sqlite>::new(
                "SELECT audit_work_item_id, scoped_work_item_id FROM work_item_audit_scope WHERE audit_work_item_id IN (",
            );
            let mut separated = scope_query.separated(", ");
            for row in &page_rows {
                separated.push_bind(row.try_get::<String, _>("work_item_id")?);
            }
            separated.push_unseparated(") ORDER BY audit_work_item_id, scoped_work_item_id");
            for scope_row in scope_query.build().fetch_all(&mut *transaction).await? {
                audit_scopes
                    .entry(scope_row.try_get("audit_work_item_id")?)
                    .or_default()
                    .push(WorkItemId::parse(
                        scope_row.try_get("scoped_work_item_id")?,
                    )?);
            }
        }
        let page_work_item_ids = page_rows
            .iter()
            .map(|row| row.try_get::<String, _>("work_item_id"))
            .collect::<Result<Vec<_>, _>>()?;
        let mut worker_projections =
            load_work_item_worker_projections(&mut transaction, &page_work_item_ids).await?;
        let mut items = Vec::with_capacity(page_rows.len());
        for row in page_rows {
            let work_item_id: String = row.try_get("work_item_id")?;
            items.push(work_item_from_row(
                row,
                dependencies.remove(&work_item_id).unwrap_or_default(),
                audit_scopes.remove(&work_item_id).unwrap_or_default(),
                worker_projections.remove(&work_item_id),
            )?);
        }
        transaction.commit().await?;
        Ok(WorkItemPage {
            next_cursor: items.last().map(|item| item.work_item_id),
            items,
            has_more,
        })
    }

    pub async fn update_work_item(&self, command: UpdateWorkItem) -> Result<WorkItem, StoreError> {
        let _write_guard = self.write_lock.lock().await;
        let now = utc_now();
        let mut transaction = self.pool.begin().await?;
        let actor = self
            .resolve_actor(
                &mut transaction,
                ActorRef::Orchestrator(command.parent_instance_id),
                &now,
            )
            .await?;
        let current = get_work_item_tx(&mut transaction, command.work_item_id).await?;
        ensure_work_item_project(&mut transaction, &current, actor.project_id).await?;
        ensure_revision(&current, command.expected_revision)?;
        if matches!(
            current.status,
            WorkItemStatus::Completed | WorkItemStatus::Cancelled
        ) {
            return Err(StoreError::Conflict(
                "completed and cancelled work items are immutable".to_owned(),
            ));
        }
        if matches!(
            current.status,
            WorkItemStatus::Claimed | WorkItemStatus::InProgress | WorkItemStatus::Blocked
        ) && (current.owner_repository_id != actor.repository_id
            || current.assignee_instance_id != Some(command.parent_instance_id)
            || current
                .claim_expires_at
                .as_deref()
                .is_none_or(|expires| expires <= now.as_str()))
        {
            return Err(StoreError::Conflict(
                "active work item updates require its owner repository, assigned orchestrator and an unexpired claim"
                    .to_owned(),
            ));
        }

        let next_owner = command
            .owner_repository_id
            .unwrap_or(current.owner_repository_id);
        if next_owner != current.owner_repository_id {
            if current.kind != WorkItemKind::Implementation {
                return Err(StoreError::Conflict(
                    "audit and remediation owner_repository_id is immutable".to_owned(),
                ));
            }
            ensure_editable_shape(&current)?;
            ensure_repository_project(&mut transaction, next_owner, actor.project_id).await?;
        }
        let next_parent = command
            .parent_work_item_id
            .unwrap_or(current.parent_work_item_id);
        let next_dependencies = command
            .dependency_ids
            .clone()
            .unwrap_or_else(|| current.dependency_ids.clone());
        if command.parent_work_item_id.is_some() || command.dependency_ids.is_some() {
            ensure_editable_shape(&current)?;
            validate_work_item_relations(
                &mut transaction,
                current.feature_id,
                Some(current.work_item_id),
                next_parent,
                &next_dependencies,
            )
            .await?;
        }
        if let Some(status) = command.status {
            validate_status_transition(current.status, status)?;
        }
        let mut next_status = command.status.unwrap_or(current.status);
        if matches!(next_status, WorkItemStatus::Planned | WorkItemStatus::Ready) {
            next_status = if dependencies_completed(&mut transaction, &next_dependencies).await? {
                WorkItemStatus::Ready
            } else {
                WorkItemStatus::Planned
            };
        }
        let implementation_artifact_id = command
            .implementation_artifact_id
            .or(current.implementation_artifact_id);
        if current.kind == WorkItemKind::Implementation && next_status == WorkItemStatus::Cancelled
        {
            let published_report: i64 = sqlx::query_scalar(
                "SELECT EXISTS(
                     SELECT 1
                     FROM worker_runs worker
                     JOIN artifacts artifact
                       ON artifact.producer_worker_run_id = worker.worker_run_id
                      AND artifact.kind = 'implementation_report'
                     WHERE worker.work_item_id = ? AND worker.role = 'executor'
                 )",
            )
            .bind(current.work_item_id.to_string())
            .fetch_one(&mut *transaction)
            .await?;
            if published_report != 0 {
                return Err(StoreError::Conflict(
                    "an implementation work item with a published implementation_report cannot be cancelled; complete and audit it instead"
                        .to_owned(),
                ));
            }
        }
        let verification_status = if current.kind == WorkItemKind::Implementation
            && next_status == WorkItemStatus::Completed
        {
            Some(VerificationStatus::PendingAudit)
        } else {
            current.verification_status
        };
        if next_status == WorkItemStatus::Completed {
            match current.kind {
                WorkItemKind::Implementation | WorkItemKind::Remediation => {
                    let artifact_id = implementation_artifact_id.ok_or_else(|| {
                        StoreError::Conflict(
                            "completing implementation or remediation work requires implementation_artifact_id"
                                .to_owned(),
                        )
                    })?;
                    validate_implementation_artifact(
                        &mut transaction,
                        artifact_id,
                        current.work_item_id,
                        actor.project_id,
                        current.owner_repository_id,
                    )
                    .await?;
                }
                WorkItemKind::Audit => {
                    if command.implementation_artifact_id.is_some() {
                        return Err(StoreError::Conflict(
                            "audit work items are completed from their linked audit_report"
                                .to_owned(),
                        ));
                    }
                    validate_audit_work_item_completion(
                        &mut transaction,
                        current.work_item_id,
                        current.owner_repository_id,
                    )
                    .await?;
                }
            }
        } else if command.implementation_artifact_id.is_some() {
            return Err(StoreError::Conflict(
                "implementation_artifact_id is only accepted when completing a work item"
                    .to_owned(),
            ));
        }
        let terminal = matches!(
            next_status,
            WorkItemStatus::Completed | WorkItemStatus::Failed | WorkItemStatus::Cancelled
        );
        let clear_claim =
            terminal || matches!(next_status, WorkItemStatus::Ready | WorkItemStatus::Planned);
        let result = sqlx::query(
            "UPDATE work_items
             SET parent_work_item_id = ?, owner_repository_id = ?, title = ?, summary = ?,
                 priority = ?, status = ?, revision = revision + 1,
                 assignee_instance_id = CASE WHEN ? THEN NULL ELSE assignee_instance_id END,
                 claim_expires_at = CASE WHEN ? THEN NULL ELSE claim_expires_at END,
                 implementation_artifact_id = ?, verification_status = ?,
                 updated_at = ?, finished_at = ?
             WHERE work_item_id = ? AND revision = ?",
        )
        .bind(next_parent.map(|id| id.to_string()))
        .bind(next_owner.to_string())
        .bind(command.title.unwrap_or(current.title))
        .bind(command.summary.unwrap_or(current.summary))
        .bind(i64::from(command.priority.unwrap_or(current.priority)))
        .bind(next_status.as_str())
        .bind(clear_claim)
        .bind(clear_claim)
        .bind(implementation_artifact_id.map(|id| id.to_string()))
        .bind(verification_status.map(VerificationStatus::as_str))
        .bind(&now)
        .bind(terminal.then_some(now.as_str()))
        .bind(command.work_item_id.to_string())
        .bind(command.expected_revision as i64)
        .execute(&mut *transaction)
        .await?;
        if result.rows_affected() != 1 {
            let latest = get_work_item_tx(&mut transaction, command.work_item_id).await?;
            return Err(revision_conflict(latest, command.expected_revision));
        }
        if command.dependency_ids.is_some() {
            replace_dependencies(&mut transaction, command.work_item_id, &next_dependencies)
                .await?;
        }
        let mut changed_items = vec![command.work_item_id];
        match current.kind {
            WorkItemKind::Audit => {
                if next_status == WorkItemStatus::Completed {
                    changed_items.extend(
                        apply_completed_audit_outcomes(
                            &mut transaction,
                            current.work_item_id,
                            &now,
                        )
                        .await?,
                    );
                } else if matches!(
                    next_status,
                    WorkItemStatus::Failed | WorkItemStatus::Cancelled
                ) {
                    changed_items.extend(
                        set_audit_scope_retry_status(&mut transaction, current.work_item_id, &now)
                            .await?,
                    );
                }
            }
            WorkItemKind::Remediation => {
                let linked_status = match next_status {
                    WorkItemStatus::Completed => Some(VerificationStatus::PendingReaudit),
                    WorkItemStatus::Failed | WorkItemStatus::Cancelled => {
                        Some(VerificationStatus::RemediationRequired)
                    }
                    _ => None,
                };
                if let (Some(audit_run_id), Some(status)) =
                    (current.addresses_audit_run_id, linked_status)
                {
                    changed_items.extend(
                        set_remediation_scope_status(&mut transaction, audit_run_id, status, &now)
                            .await?,
                    );
                }
            }
            WorkItemKind::Implementation => {}
        }
        if next_status == WorkItemStatus::Completed {
            changed_items
                .extend(unlock_dependents(&mut transaction, command.work_item_id, &now).await?);
        }
        let recipients =
            append_work_item_events(&mut transaction, actor.project_id, &changed_items, &now)
                .await?;
        transaction.commit().await?;
        self.notify_repositories(&recipients);
        self.get_work_item(command.work_item_id).await
    }

    pub async fn increase_audit_depth(
        &self,
        command: IncreaseAuditDepth,
    ) -> Result<WorkItem, StoreError> {
        validate_audit_depth(command.max_audit_iterations)?;
        let reason = command.reason.trim();
        if reason.is_empty() {
            return Err(StoreError::Conflict("reason must not be empty".to_owned()));
        }
        let _write_guard = self.write_lock.lock().await;
        let now = utc_now();
        let mut transaction = self.pool.begin().await?;
        let actor = self
            .resolve_actor(
                &mut transaction,
                ActorRef::Orchestrator(command.parent_instance_id),
                &now,
            )
            .await?;
        let current = get_work_item_tx(&mut transaction, command.work_item_id).await?;
        ensure_work_item_project(&mut transaction, &current, actor.project_id).await?;
        ensure_revision(&current, command.expected_revision)?;
        ensure_active_feature_project(&mut transaction, current.feature_id, actor.project_id)
            .await?;
        if current.kind != WorkItemKind::Implementation
            || current.owner_repository_id != actor.repository_id
            || current.verification_status != Some(VerificationStatus::VerificationBlocked)
        {
            return Err(StoreError::Conflict(
                "audit depth can only be increased by the owner repository for a verification_blocked implementation item"
                    .to_owned(),
            ));
        }
        if command.max_audit_iterations <= current.max_audit_iterations {
            return Err(StoreError::Conflict(format!(
                "new audit depth must exceed current depth {}",
                current.max_audit_iterations
            )));
        }
        let latest_verdict: String = sqlx::query_scalar(
            "SELECT outcome.verdict
             FROM audit_outcomes outcome
             JOIN worker_run_audit_items scope
               ON scope.worker_run_id = outcome.audit_worker_run_id
              AND scope.work_item_id = outcome.scoped_work_item_id
             WHERE outcome.scoped_work_item_id = ?
             ORDER BY scope.audit_iteration DESC LIMIT 1",
        )
        .bind(command.work_item_id.to_string())
        .fetch_one(&mut *transaction)
        .await?;
        let reopened_status = if latest_verdict == AuditVerdict::ChangesRequired.as_str() {
            VerificationStatus::RemediationRequired
        } else {
            VerificationStatus::PendingReaudit
        };
        sqlx::query(
            "UPDATE work_items
             SET max_audit_iterations = ?, verification_status = ?,
                 revision = revision + 1, updated_at = ?
             WHERE work_item_id = ? AND revision = ?",
        )
        .bind(i64::from(command.max_audit_iterations))
        .bind(reopened_status.as_str())
        .bind(&now)
        .bind(command.work_item_id.to_string())
        .bind(command.expected_revision as i64)
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            "INSERT INTO audit_depth_changes(
                 work_item_id, changed_by_instance_id, old_max_iterations,
                 new_max_iterations, reason, created_at
             ) VALUES (?, ?, ?, ?, ?, ?)",
        )
        .bind(command.work_item_id.to_string())
        .bind(command.parent_instance_id.to_string())
        .bind(i64::from(current.max_audit_iterations))
        .bind(i64::from(command.max_audit_iterations))
        .bind(reason)
        .bind(&now)
        .execute(&mut *transaction)
        .await?;
        append_project_event(
            &mut transaction,
            actor.project_id,
            EventKind::WorkItemUpdated,
            EventEntityType::WorkItem,
            &command.work_item_id.to_string(),
            &now,
        )
        .await?;
        let recipients = append_project_event(
            &mut transaction,
            actor.project_id,
            EventKind::AuditDepthIncreased,
            EventEntityType::WorkItem,
            &command.work_item_id.to_string(),
            &now,
        )
        .await?;
        transaction.commit().await?;
        self.notify_repositories(&recipients);
        self.get_work_item(command.work_item_id).await
    }

    pub async fn claim_work_item(
        &self,
        instance_id: InstanceId,
        work_item_id: WorkItemId,
        expected_revision: u64,
    ) -> Result<WorkItem, StoreError> {
        let _write_guard = self.write_lock.lock().await;
        let now = utc_now();
        let mut transaction = self.pool.begin().await?;
        let actor = self
            .resolve_actor(&mut transaction, ActorRef::Orchestrator(instance_id), &now)
            .await?;
        let current = get_work_item_tx(&mut transaction, work_item_id).await?;
        ensure_work_item_project(&mut transaction, &current, actor.project_id).await?;
        ensure_revision(&current, expected_revision)?;
        if current.owner_repository_id != actor.repository_id {
            return Err(StoreError::Conflict(
                "only an orchestrator of the owner repository can claim this work item".to_owned(),
            ));
        }
        if !dependencies_completed(&mut transaction, &current.dependency_ids).await? {
            return Err(StoreError::Conflict(
                "work item dependencies are not completed".to_owned(),
            ));
        }
        let claimable = current.status == WorkItemStatus::Ready
            || (matches!(
                current.status,
                WorkItemStatus::Claimed | WorkItemStatus::InProgress | WorkItemStatus::Blocked
            ) && current
                .claim_expires_at
                .as_deref()
                .is_some_and(|value| value <= now.as_str()));
        if !claimable {
            if matches!(
                current.status,
                WorkItemStatus::Claimed | WorkItemStatus::InProgress | WorkItemStatus::Blocked
            ) && current
                .claim_expires_at
                .as_deref()
                .is_some_and(|value| value > now.as_str())
            {
                return Err(claim_conflict(
                    "work_item",
                    work_item_id.to_string(),
                    "work item is held by an active claim",
                    current.claim_expires_at.clone(),
                    &now,
                ));
            }
            return Err(StoreError::Conflict(format!(
                "work item {} is not claimable from '{}'",
                work_item_id,
                current.status.as_str()
            )));
        }
        let claim_expires_at = lease_deadline(self.work_item_claim_lease_duration);
        sqlx::query(
            "UPDATE work_items
             SET status = 'claimed', assignee_instance_id = ?, claim_expires_at = ?,
                 revision = revision + 1, updated_at = ?
             WHERE work_item_id = ? AND revision = ?",
        )
        .bind(instance_id.to_string())
        .bind(claim_expires_at)
        .bind(&now)
        .bind(work_item_id.to_string())
        .bind(expected_revision as i64)
        .execute(&mut *transaction)
        .await?;
        let recipients =
            append_work_item_events(&mut transaction, actor.project_id, &[work_item_id], &now)
                .await?;
        transaction.commit().await?;
        self.notify_repositories(&recipients);
        self.get_work_item(work_item_id).await
    }

    pub async fn renew_work_item_claim(
        &self,
        instance_id: InstanceId,
        work_item_id: WorkItemId,
        expected_revision: u64,
    ) -> Result<WorkItem, StoreError> {
        self.mutate_work_item_claim(instance_id, work_item_id, expected_revision, false)
            .await
    }

    pub async fn release_work_item_claim(
        &self,
        instance_id: InstanceId,
        work_item_id: WorkItemId,
        expected_revision: u64,
    ) -> Result<WorkItem, StoreError> {
        self.mutate_work_item_claim(instance_id, work_item_id, expected_revision, true)
            .await
    }

    async fn mutate_work_item_claim(
        &self,
        instance_id: InstanceId,
        work_item_id: WorkItemId,
        expected_revision: u64,
        release: bool,
    ) -> Result<WorkItem, StoreError> {
        let _write_guard = self.write_lock.lock().await;
        let now = utc_now();
        let mut transaction = self.pool.begin().await?;
        let actor = self
            .resolve_actor(&mut transaction, ActorRef::Orchestrator(instance_id), &now)
            .await?;
        let current = get_work_item_tx(&mut transaction, work_item_id).await?;
        ensure_work_item_project(&mut transaction, &current, actor.project_id).await?;
        ensure_revision(&current, expected_revision)?;
        if current.assignee_instance_id != Some(instance_id)
            || !matches!(
                current.status,
                WorkItemStatus::Claimed | WorkItemStatus::InProgress | WorkItemStatus::Blocked
            )
            || current
                .claim_expires_at
                .as_deref()
                .is_none_or(|value| value <= now.as_str())
        {
            return Err(StoreError::Conflict(
                "work-item claim is not active or is owned by another instance".to_owned(),
            ));
        }
        let (status, assignee, expires) = if release {
            (WorkItemStatus::Ready, None, None)
        } else {
            (
                current.status,
                Some(instance_id.to_string()),
                Some(lease_deadline(self.work_item_claim_lease_duration)),
            )
        };
        sqlx::query(
            "UPDATE work_items
             SET status = ?, assignee_instance_id = ?, claim_expires_at = ?,
                 revision = revision + 1, updated_at = ?
             WHERE work_item_id = ? AND revision = ?",
        )
        .bind(status.as_str())
        .bind(assignee)
        .bind(expires)
        .bind(&now)
        .bind(work_item_id.to_string())
        .bind(expected_revision as i64)
        .execute(&mut *transaction)
        .await?;
        let recipients =
            append_work_item_events(&mut transaction, actor.project_id, &[work_item_id], &now)
                .await?;
        transaction.commit().await?;
        self.notify_repositories(&recipients);
        self.get_work_item(work_item_id).await
    }

    pub(super) fn notify_repositories(&self, repositories: &[RepositoryId]) {
        for repository_id in repositories {
            self.notifications.notify(*repository_id);
        }
    }
}

async fn get_work_item_tx(
    transaction: &mut Transaction<'_, Sqlite>,
    work_item_id: WorkItemId,
) -> Result<WorkItem, StoreError> {
    let row = sqlx::query("SELECT * FROM work_items WHERE work_item_id = ?")
        .bind(work_item_id.to_string())
        .fetch_optional(&mut **transaction)
        .await?
        .ok_or_else(|| StoreError::NotFound(format!("work item {work_item_id}")))?;
    let dependency_rows = sqlx::query(
        "SELECT dependency_work_item_id FROM work_item_dependencies
         WHERE work_item_id = ? ORDER BY dependency_work_item_id",
    )
    .bind(work_item_id.to_string())
    .fetch_all(&mut **transaction)
    .await?;
    let dependency_ids = dependency_rows
        .iter()
        .map(|row| Ok(WorkItemId::parse(row.try_get("dependency_work_item_id")?)?))
        .collect::<Result<Vec<_>, StoreError>>()?;
    let audit_scope_rows = sqlx::query(
        "SELECT scoped_work_item_id FROM work_item_audit_scope
         WHERE audit_work_item_id = ? ORDER BY scoped_work_item_id",
    )
    .bind(work_item_id.to_string())
    .fetch_all(&mut **transaction)
    .await?;
    let audit_scope_work_item_ids = audit_scope_rows
        .iter()
        .map(|row| Ok(WorkItemId::parse(row.try_get("scoped_work_item_id")?)?))
        .collect::<Result<Vec<_>, StoreError>>()?;
    let worker_projection = load_work_item_worker_projection(transaction, work_item_id).await?;
    work_item_from_row(
        &row,
        dependency_ids,
        audit_scope_work_item_ids,
        worker_projection,
    )
}

async fn load_work_item_worker_projection(
    transaction: &mut Transaction<'_, Sqlite>,
    work_item_id: WorkItemId,
) -> Result<Option<WorkItemWorkerProjection>, StoreError> {
    let row = sqlx::query(
        "SELECT worker_run_id, role, status, last_heartbeat_at, lease_expires_at
         FROM worker_runs WHERE work_item_id = ?
         ORDER BY CASE status WHEN 'active' THEN 0 WHEN 'created' THEN 1 ELSE 2 END,
                  created_at DESC, worker_run_id DESC
         LIMIT 1",
    )
    .bind(work_item_id.to_string())
    .fetch_optional(&mut **transaction)
    .await?;
    row.map(|row| {
        Ok(WorkItemWorkerProjection {
            worker_run_id: WorkerRunId::parse(row.try_get("worker_run_id")?)?,
            role: row
                .try_get::<String, _>("role")?
                .parse()
                .map_err(StoreError::Conflict)?,
            status: row
                .try_get::<String, _>("status")?
                .parse()
                .map_err(StoreError::Conflict)?,
            last_heartbeat_at: row.try_get("last_heartbeat_at")?,
            lease_expires_at: row.try_get("lease_expires_at")?,
        })
    })
    .transpose()
}

async fn load_work_item_worker_projections(
    transaction: &mut Transaction<'_, Sqlite>,
    work_item_ids: &[String],
) -> Result<HashMap<String, WorkItemWorkerProjection>, StoreError> {
    if work_item_ids.is_empty() {
        return Ok(HashMap::new());
    }
    let mut query = QueryBuilder::<Sqlite>::new(
        "SELECT work_item_id, worker_run_id, role, status, last_heartbeat_at, lease_expires_at
         FROM (SELECT work_item_id, worker_run_id, role, status, last_heartbeat_at,
                      lease_expires_at,
                      ROW_NUMBER() OVER (
                          PARTITION BY work_item_id
                          ORDER BY CASE status WHEN 'active' THEN 0 WHEN 'created' THEN 1 ELSE 2 END,
                                   created_at DESC, worker_run_id DESC
                      ) AS worker_rank
               FROM worker_runs WHERE work_item_id IN (",
    );
    let mut separated = query.separated(", ");
    for work_item_id in work_item_ids {
        separated.push_bind(work_item_id);
    }
    separated.push_unseparated(") ) WHERE worker_rank = 1");
    let rows = query.build().fetch_all(&mut **transaction).await?;
    let mut projections = HashMap::with_capacity(rows.len());
    for row in rows {
        projections.insert(
            row.try_get("work_item_id")?,
            WorkItemWorkerProjection {
                worker_run_id: WorkerRunId::parse(row.try_get("worker_run_id")?)?,
                role: row
                    .try_get::<String, _>("role")?
                    .parse()
                    .map_err(StoreError::Conflict)?,
                status: row
                    .try_get::<String, _>("status")?
                    .parse()
                    .map_err(StoreError::Conflict)?,
                last_heartbeat_at: row.try_get("last_heartbeat_at")?,
                lease_expires_at: row.try_get("lease_expires_at")?,
            },
        );
    }
    Ok(projections)
}

fn work_item_from_row(
    row: &SqliteRow,
    dependency_ids: Vec<WorkItemId>,
    audit_scope_work_item_ids: Vec<WorkItemId>,
    worker_projection: Option<WorkItemWorkerProjection>,
) -> Result<WorkItem, StoreError> {
    let status: String = row.try_get("status")?;
    let kind: String = row.try_get("kind")?;
    Ok(WorkItem {
        work_item_id: WorkItemId::parse(row.try_get("work_item_id")?)?,
        feature_id: FeatureId::parse(row.try_get("feature_id")?)?,
        parent_work_item_id: parse_optional_id(
            row.try_get("parent_work_item_id")?,
            WorkItemId::parse,
        )?,
        owner_repository_id: RepositoryId::parse(row.try_get("owner_repository_id")?)?,
        assignee_instance_id: parse_optional_id(
            row.try_get("assignee_instance_id")?,
            InstanceId::parse,
        )?,
        title: row.try_get("title")?,
        summary: row.try_get("summary")?,
        kind: kind.parse().map_err(StoreError::Conflict)?,
        audit_scope_work_item_ids,
        addresses_audit_run_id: parse_optional_id(
            row.try_get("addresses_audit_run_id")?,
            WorkerRunId::parse,
        )?,
        dependency_ids,
        priority: row.try_get::<i64, _>("priority")? as i16,
        status: status.parse().map_err(StoreError::Conflict)?,
        revision: row.try_get::<i64, _>("revision")? as u64,
        max_audit_iterations: row.try_get::<i64, _>("max_audit_iterations")? as u8,
        claim_expires_at: row.try_get("claim_expires_at")?,
        implementation_artifact_id: parse_optional_id(
            row.try_get("implementation_artifact_id")?,
            ArtifactId::parse,
        )?,
        latest_audit_artifact_id: parse_optional_id(
            row.try_get("latest_audit_artifact_id")?,
            ArtifactId::parse,
        )?,
        verification_status: row
            .try_get::<Option<String>, _>("verification_status")?
            .map(|value| value.parse().map_err(StoreError::Conflict))
            .transpose()?,
        worker_projection,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
        finished_at: row.try_get("finished_at")?,
    })
}

fn feature_from_row(row: &SqliteRow) -> Result<Feature, StoreError> {
    let status: String = row.try_get("status")?;
    Ok(Feature {
        feature_id: FeatureId::parse(row.try_get("feature_id")?)?,
        project_id: ProjectId::parse(row.try_get("project_id")?)?,
        created_by_instance_id: InstanceId::parse(row.try_get("created_by_instance_id")?)?,
        coordinator_repository_id: RepositoryId::parse(row.try_get("coordinator_repository_id")?)?,
        title: row.try_get("title")?,
        summary: row.try_get("summary")?,
        status: status.parse().map_err(StoreError::Conflict)?,
        revision: row.try_get::<i64, _>("revision")? as u64,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
        completed_at: row.try_get("completed_at")?,
        cancelled_at: row.try_get("cancelled_at")?,
    })
}

async fn get_feature_tx(
    transaction: &mut Transaction<'_, Sqlite>,
    feature_id: FeatureId,
) -> Result<Feature, StoreError> {
    let row = sqlx::query("SELECT * FROM features WHERE feature_id = ?")
        .bind(feature_id.to_string())
        .fetch_optional(&mut **transaction)
        .await?
        .ok_or_else(|| StoreError::NotFound(format!("feature {feature_id}")))?;
    feature_from_row(&row)
}

fn ensure_feature_actor(feature: &Feature, actor: &ResolvedActor) -> Result<(), StoreError> {
    if feature.project_id != actor.project_id {
        return Err(StoreError::NotFound(format!(
            "feature {} in actor project",
            feature.feature_id
        )));
    }
    if feature.coordinator_repository_id != actor.repository_id {
        return Err(StoreError::Conflict(format!(
            "feature {} can only be closed by coordinator repository {}",
            feature.feature_id, feature.coordinator_repository_id
        )));
    }
    Ok(())
}

fn ensure_feature_revision(feature: &Feature, expected: u64) -> Result<(), StoreError> {
    if feature.revision != expected {
        return Err(StoreError::FeatureRevisionConflict {
            feature_id: feature.feature_id,
            expected,
            actual: feature.revision,
            current: Box::new(feature.clone()),
        });
    }
    Ok(())
}

async fn ensure_feature_has_no_open_work(
    transaction: &mut Transaction<'_, Sqlite>,
    feature_id: FeatureId,
) -> Result<(), StoreError> {
    let open_items: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM work_items
         WHERE feature_id = ? AND status NOT IN ('completed', 'cancelled')",
    )
    .bind(feature_id.to_string())
    .fetch_one(&mut **transaction)
    .await?;
    if open_items != 0 {
        return Err(StoreError::Conflict(format!(
            "feature {feature_id} has {open_items} unfinished work item(s)"
        )));
    }
    Ok(())
}

async fn ensure_feature_has_no_open_requests(
    transaction: &mut Transaction<'_, Sqlite>,
    feature_id: FeatureId,
) -> Result<(), StoreError> {
    let open_requests: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM requests
         WHERE feature_id = ? AND status IN ('pending', 'claimed')",
    )
    .bind(feature_id.to_string())
    .fetch_one(&mut **transaction)
    .await?;
    if open_requests != 0 {
        return Err(StoreError::Conflict(format!(
            "feature {feature_id} has {open_requests} open repository request(s)"
        )));
    }
    Ok(())
}

async fn ensure_feature_has_no_active_workers(
    transaction: &mut Transaction<'_, Sqlite>,
    feature_id: FeatureId,
) -> Result<(), StoreError> {
    let active_workers: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM worker_runs wr
         WHERE wr.status IN ('created', 'active') AND (
             EXISTS (
                 SELECT 1 FROM work_items wi
                 WHERE wi.work_item_id = wr.work_item_id AND wi.feature_id = ?
             ) OR EXISTS (
                 SELECT 1 FROM worker_run_audit_items ai
                 JOIN work_items wi ON wi.work_item_id = ai.work_item_id
                 WHERE ai.worker_run_id = wr.worker_run_id AND wi.feature_id = ?
             ) OR EXISTS (
                 SELECT 1 FROM worker_run_audit_items ai
                 JOIN work_items wi ON wi.work_item_id = ai.work_item_id
                 WHERE ai.worker_run_id = wr.addresses_audit_run_id AND wi.feature_id = ?
             ) OR EXISTS (
                 SELECT 1 FROM requests linked_request
                 WHERE linked_request.request_id = wr.request_id
                   AND linked_request.feature_id = ?
             )
         )",
    )
    .bind(feature_id.to_string())
    .bind(feature_id.to_string())
    .bind(feature_id.to_string())
    .bind(feature_id.to_string())
    .fetch_one(&mut **transaction)
    .await?;
    if active_workers != 0 {
        return Err(StoreError::Conflict(format!(
            "feature {feature_id} has {active_workers} active worker run(s)"
        )));
    }
    Ok(())
}

async fn ensure_feature_implementation_is_verified(
    transaction: &mut Transaction<'_, Sqlite>,
    feature_id: FeatureId,
) -> Result<(), StoreError> {
    let unverified: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM work_items item
         WHERE item.feature_id = ? AND item.kind = 'implementation'
           AND COALESCE(item.verification_status, '') <> 'verified'
           AND (
               item.implementation_artifact_id IS NOT NULL
               OR EXISTS (
                   SELECT 1
                   FROM worker_runs worker
                   JOIN artifacts artifact
                     ON artifact.producer_worker_run_id = worker.worker_run_id
                    AND artifact.kind = 'implementation_report'
                   WHERE worker.work_item_id = item.work_item_id
                     AND worker.role = 'executor'
               )
           )",
    )
    .bind(feature_id.to_string())
    .fetch_one(&mut **transaction)
    .await?;
    if unverified != 0 {
        return Err(StoreError::Conflict(format!(
            "feature {feature_id} has {unverified} implementation work item(s) without passing independent audit"
        )));
    }
    Ok(())
}

fn ensure_revision(current: &WorkItem, expected: u64) -> Result<(), StoreError> {
    if current.revision != expected {
        return Err(revision_conflict(current.clone(), expected));
    }
    Ok(())
}

fn revision_conflict(current: WorkItem, expected: u64) -> StoreError {
    StoreError::RevisionConflict {
        work_item_id: current.work_item_id,
        expected,
        actual: current.revision,
        current: Box::new(current),
    }
}

fn validate_audit_depth(value: u8) -> Result<(), StoreError> {
    if !(1..=10).contains(&value) {
        return Err(StoreError::Conflict(
            "max_audit_iterations must be between 1 and 10".to_owned(),
        ));
    }
    Ok(())
}

fn validate_work_item_kind_command(command: &CreateWorkItem) -> Result<(), StoreError> {
    match command.kind {
        WorkItemKind::Implementation => {
            if !command.audit_scope_work_item_ids.is_empty()
                || command.addresses_audit_run_id.is_some()
            {
                return Err(StoreError::Conflict(
                    "implementation work items cannot declare audit scope or remediation lineage"
                        .to_owned(),
                ));
            }
        }
        WorkItemKind::Audit => {
            if command.audit_scope_work_item_ids.is_empty() {
                return Err(StoreError::Conflict(
                    "audit work items require at least one audit_scope_work_item_id".to_owned(),
                ));
            }
            if command.audit_scope_work_item_ids.len() > MAX_AUDIT_WORK_ITEMS {
                return Err(StoreError::Conflict(format!(
                    "audit scope must contain at most {MAX_AUDIT_WORK_ITEMS} work items"
                )));
            }
            if command.addresses_audit_run_id.is_some() {
                return Err(StoreError::Conflict(
                    "audit work items cannot address another audit run".to_owned(),
                ));
            }
        }
        WorkItemKind::Remediation => {
            if !command.audit_scope_work_item_ids.is_empty() {
                return Err(StoreError::Conflict(
                    "remediation work items derive scope from their audit run".to_owned(),
                ));
            }
            if command.addresses_audit_run_id.is_none() {
                return Err(StoreError::Conflict(
                    "remediation work items require addresses_audit_run_id".to_owned(),
                ));
            }
        }
    }
    Ok(())
}

async fn prepare_audit_work_item_scope(
    transaction: &mut Transaction<'_, Sqlite>,
    feature_id: FeatureId,
    owner_repository_id: RepositoryId,
    work_item_ids: &[WorkItemId],
) -> Result<Vec<AuditScopeItem>, StoreError> {
    let mut unique = BTreeSet::new();
    let mut scope = Vec::with_capacity(work_item_ids.len());
    for work_item_id in work_item_ids {
        if !unique.insert(work_item_id.to_string()) {
            return Err(StoreError::Conflict(format!(
                "work item {work_item_id} appears more than once in audit scope"
            )));
        }
        let item = sqlx::query(
            "SELECT kind, status, feature_id, owner_repository_id, max_audit_iterations,
                    verification_status
             FROM work_items WHERE work_item_id = ?",
        )
        .bind(work_item_id.to_string())
        .fetch_optional(&mut **transaction)
        .await?
        .ok_or_else(|| StoreError::NotFound(format!("work item {work_item_id}")))?;
        let item_feature_id = FeatureId::parse(item.try_get("feature_id")?)?;
        let item_owner_id = RepositoryId::parse(item.try_get("owner_repository_id")?)?;
        let kind: String = item.try_get("kind")?;
        let status: String = item.try_get("status")?;
        let verification_status: Option<String> = item.try_get("verification_status")?;
        if item_feature_id != feature_id
            || item_owner_id != owner_repository_id
            || kind != WorkItemKind::Implementation.as_str()
            || status != WorkItemStatus::Completed.as_str()
        {
            return Err(StoreError::Conflict(format!(
                "audit scope item {work_item_id} must be a completed implementation item in the same feature and owner repository"
            )));
        }
        if !matches!(
            verification_status.as_deref(),
            None | Some("pending_audit") | Some("pending_reaudit")
        ) {
            return Err(StoreError::Conflict(format!(
                "audit scope item {work_item_id} is not awaiting an audit"
            )));
        }
        let max_iterations = item.try_get::<i64, _>("max_audit_iterations")? as u8;
        let previous = sqlx::query(
            "SELECT scope.audit_iteration, scope.max_iterations,
                    audit_item.status AS audit_item_status,
                    audit_run.status AS audit_run_status,
                    (SELECT COUNT(*) FROM artifacts
                     WHERE producer_worker_run_id = audit_run.worker_run_id
                       AND kind = 'audit_report') AS report_count,
                    remediation.status AS remediation_status
             FROM work_item_audit_scope scope
             JOIN work_items audit_item
               ON audit_item.work_item_id = scope.audit_work_item_id
             LEFT JOIN worker_runs audit_run
               ON audit_run.work_item_id = audit_item.work_item_id
              AND audit_run.role = 'auditor'
              AND audit_run.status = 'completed'
             LEFT JOIN work_items remediation
               ON remediation.addresses_audit_run_id = audit_run.worker_run_id
              AND remediation.kind = 'remediation'
             WHERE scope.scoped_work_item_id = ?
             ORDER BY scope.audit_iteration DESC
             LIMIT 1",
        )
        .bind(work_item_id.to_string())
        .fetch_optional(&mut **transaction)
        .await?;
        let previous_iteration = if let Some(previous) = previous {
            let stored_max = previous.try_get::<i64, _>("max_iterations")?;
            let audit_item_status: String = previous.try_get("audit_item_status")?;
            let audit_run_status: Option<String> = previous.try_get("audit_run_status")?;
            let report_count: i64 = previous.try_get("report_count")?;
            let remediation_status: Option<String> = previous.try_get("remediation_status")?;
            if stored_max > i64::from(max_iterations) {
                return Err(StoreError::Conflict(format!(
                    "work item {work_item_id} audit depth is below its recorded depth {stored_max}"
                )));
            }
            if audit_item_status != WorkItemStatus::Completed.as_str()
                || audit_run_status.as_deref() != Some(WorkerRunStatus::Completed.as_str())
                || report_count != 1
                || remediation_status
                    .as_deref()
                    .is_some_and(|value| value != WorkItemStatus::Completed.as_str())
            {
                return Err(StoreError::Conflict(format!(
                    "work item {work_item_id} cannot start its next audit until the previous audit work item and any remediation work item are completed"
                )));
            }
            previous.try_get::<i64, _>("audit_iteration")?
        } else {
            0
        };
        let iteration = previous_iteration + 1;
        if iteration > i64::from(max_iterations) {
            return Err(StoreError::Conflict(format!(
                "work item {work_item_id} exhausted audit depth {max_iterations}"
            )));
        }
        scope.push(AuditScopeItem {
            work_item_id: *work_item_id,
            iteration: iteration as u8,
            max_iterations,
        });
    }
    Ok(scope)
}

fn ensure_editable_shape(item: &WorkItem) -> Result<(), StoreError> {
    if !matches!(item.status, WorkItemStatus::Planned | WorkItemStatus::Ready) {
        return Err(StoreError::Conflict(
            "owner, parent and dependencies can only change while planned or ready".to_owned(),
        ));
    }
    Ok(())
}

pub(super) fn validate_status_transition(
    from: WorkItemStatus,
    to: WorkItemStatus,
) -> Result<(), StoreError> {
    if from == to {
        return Ok(());
    }
    let allowed = matches!(
        (from, to),
        (
            WorkItemStatus::Planned,
            WorkItemStatus::Ready | WorkItemStatus::Cancelled
        ) | (WorkItemStatus::Ready, WorkItemStatus::Cancelled)
            | (
                WorkItemStatus::Claimed,
                WorkItemStatus::InProgress | WorkItemStatus::Completed | WorkItemStatus::Cancelled
            )
            | (
                WorkItemStatus::InProgress,
                WorkItemStatus::Blocked
                    | WorkItemStatus::Completed
                    | WorkItemStatus::Failed
                    | WorkItemStatus::Cancelled
            )
            | (
                WorkItemStatus::Blocked,
                WorkItemStatus::InProgress | WorkItemStatus::Failed | WorkItemStatus::Cancelled
            )
            | (
                WorkItemStatus::Failed,
                WorkItemStatus::Ready | WorkItemStatus::Cancelled
            )
    );
    if !allowed {
        return Err(StoreError::Conflict(format!(
            "invalid work-item transition '{}' -> '{}'",
            from.as_str(),
            to.as_str()
        )));
    }
    Ok(())
}

async fn ensure_feature_project(
    transaction: &mut Transaction<'_, Sqlite>,
    feature_id: FeatureId,
    project_id: ProjectId,
) -> Result<(), StoreError> {
    let exists: i64 = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM features WHERE feature_id = ? AND project_id = ?)",
    )
    .bind(feature_id.to_string())
    .bind(project_id.to_string())
    .fetch_one(&mut **transaction)
    .await?;
    if exists == 0 {
        return Err(StoreError::NotFound(format!(
            "feature {feature_id} in actor project"
        )));
    }
    Ok(())
}

async fn ensure_active_feature_project(
    transaction: &mut Transaction<'_, Sqlite>,
    feature_id: FeatureId,
    project_id: ProjectId,
) -> Result<(), StoreError> {
    ensure_feature_project(transaction, feature_id, project_id).await?;
    let status: String = sqlx::query_scalar("SELECT status FROM features WHERE feature_id = ?")
        .bind(feature_id.to_string())
        .fetch_one(&mut **transaction)
        .await?;
    if status != FeatureStatus::Active.as_str() {
        return Err(StoreError::Conflict(format!(
            "feature {feature_id} is '{status}', expected 'active'"
        )));
    }
    Ok(())
}

async fn ensure_work_item_project(
    transaction: &mut Transaction<'_, Sqlite>,
    item: &WorkItem,
    project_id: ProjectId,
) -> Result<(), StoreError> {
    ensure_feature_project(transaction, item.feature_id, project_id).await
}

async fn ensure_repository_project(
    transaction: &mut Transaction<'_, Sqlite>,
    repository_id: RepositoryId,
    project_id: ProjectId,
) -> Result<(), StoreError> {
    if repository_project_id(transaction, repository_id).await? != project_id {
        return Err(StoreError::Conflict(
            "work-item owner repository must belong to the feature project".to_owned(),
        ));
    }
    Ok(())
}

async fn validate_work_item_relations(
    transaction: &mut Transaction<'_, Sqlite>,
    feature_id: FeatureId,
    work_item_id: Option<WorkItemId>,
    parent_id: Option<WorkItemId>,
    dependency_ids: &[WorkItemId],
) -> Result<(), StoreError> {
    if dependency_ids.len() > MAX_WORK_ITEM_DEPENDENCIES {
        return Err(StoreError::Conflict(format!(
            "a work item may have at most {MAX_WORK_ITEM_DEPENDENCIES} dependencies"
        )));
    }
    let mut unique = BTreeSet::new();
    for id in dependency_ids {
        if !unique.insert(id.to_string()) {
            return Err(StoreError::Conflict(format!("duplicate dependency {id}")));
        }
        if Some(*id) == work_item_id {
            return Err(StoreError::Conflict(
                "work item cannot depend on itself".to_owned(),
            ));
        }
    }
    if !unique.is_empty() {
        let mut query =
            QueryBuilder::<Sqlite>::new("SELECT COUNT(*) FROM work_items WHERE feature_id = ");
        query
            .push_bind(feature_id.to_string())
            .push(" AND work_item_id IN (");
        let mut separated = query.separated(", ");
        for id in &unique {
            separated.push_bind(id);
        }
        separated.push_unseparated(")");
        let count: i64 = query
            .build_query_scalar()
            .fetch_one(&mut **transaction)
            .await?;
        if count != unique.len() as i64 {
            return Err(StoreError::NotFound(format!(
                "one or more dependencies do not belong to feature {feature_id}"
            )));
        }
    }
    for id in dependency_ids {
        if let Some(item_id) = work_item_id {
            if dependency_reaches(transaction, *id, item_id).await? {
                return Err(StoreError::Conflict(
                    "work-item dependency would create a cycle".to_owned(),
                ));
            }
        }
    }
    if let Some(parent_id) = parent_id {
        if Some(parent_id) == work_item_id {
            return Err(StoreError::Conflict(
                "work item cannot parent itself".to_owned(),
            ));
        }
        ensure_item_feature(transaction, parent_id, feature_id).await?;
        if let Some(item_id) = work_item_id {
            if parent_reaches(transaction, parent_id, item_id).await? {
                return Err(StoreError::Conflict(
                    "work-item parent would create a cycle".to_owned(),
                ));
            }
        }
    }
    Ok(())
}

async fn ensure_item_feature(
    transaction: &mut Transaction<'_, Sqlite>,
    item_id: WorkItemId,
    feature_id: FeatureId,
) -> Result<(), StoreError> {
    let exists: i64 = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM work_items WHERE work_item_id = ? AND feature_id = ?)",
    )
    .bind(item_id.to_string())
    .bind(feature_id.to_string())
    .fetch_one(&mut **transaction)
    .await?;
    if exists == 0 {
        return Err(StoreError::NotFound(format!(
            "work item {item_id} in feature {feature_id}"
        )));
    }
    Ok(())
}

async fn dependency_reaches(
    transaction: &mut Transaction<'_, Sqlite>,
    start: WorkItemId,
    target: WorkItemId,
) -> Result<bool, StoreError> {
    let reaches: i64 = sqlx::query_scalar(
        "WITH RECURSIVE dependency_path(work_item_id) AS (
             SELECT dependency_work_item_id FROM work_item_dependencies WHERE work_item_id = ?
             UNION
             SELECT d.dependency_work_item_id
             FROM work_item_dependencies d
             JOIN dependency_path p ON d.work_item_id = p.work_item_id
         )
         SELECT EXISTS(SELECT 1 FROM dependency_path WHERE work_item_id = ?)",
    )
    .bind(start.to_string())
    .bind(target.to_string())
    .fetch_one(&mut **transaction)
    .await?;
    Ok(reaches != 0)
}

async fn parent_reaches(
    transaction: &mut Transaction<'_, Sqlite>,
    start: WorkItemId,
    target: WorkItemId,
) -> Result<bool, StoreError> {
    let reaches: i64 = sqlx::query_scalar(
        "WITH RECURSIVE parent_path(work_item_id) AS (
             SELECT parent_work_item_id FROM work_items WHERE work_item_id = ?
             UNION
             SELECT w.parent_work_item_id
             FROM work_items w
             JOIN parent_path p ON w.work_item_id = p.work_item_id
             WHERE w.parent_work_item_id IS NOT NULL
         )
         SELECT EXISTS(SELECT 1 FROM parent_path WHERE work_item_id = ?)",
    )
    .bind(start.to_string())
    .bind(target.to_string())
    .fetch_one(&mut **transaction)
    .await?;
    Ok(reaches != 0)
}

async fn dependencies_completed(
    transaction: &mut Transaction<'_, Sqlite>,
    dependency_ids: &[WorkItemId],
) -> Result<bool, StoreError> {
    for dependency_id in dependency_ids {
        let status: String =
            sqlx::query_scalar("SELECT status FROM work_items WHERE work_item_id = ?")
                .bind(dependency_id.to_string())
                .fetch_one(&mut **transaction)
                .await?;
        if status != "completed" {
            return Ok(false);
        }
    }
    Ok(true)
}

async fn replace_dependencies(
    transaction: &mut Transaction<'_, Sqlite>,
    work_item_id: WorkItemId,
    dependency_ids: &[WorkItemId],
) -> Result<(), StoreError> {
    sqlx::query("DELETE FROM work_item_dependencies WHERE work_item_id = ?")
        .bind(work_item_id.to_string())
        .execute(&mut **transaction)
        .await?;
    if !dependency_ids.is_empty() {
        let mut insert = QueryBuilder::<Sqlite>::new(
            "INSERT INTO work_item_dependencies(work_item_id, dependency_work_item_id) ",
        );
        insert.push_values(dependency_ids, |mut row, dependency_id| {
            row.push_bind(work_item_id.to_string())
                .push_bind(dependency_id.to_string());
        });
        insert.build().execute(&mut **transaction).await?;
    }
    Ok(())
}

async fn validate_implementation_artifact(
    transaction: &mut Transaction<'_, Sqlite>,
    artifact_id: ArtifactId,
    work_item_id: WorkItemId,
    project_id: ProjectId,
    owner_repository_id: RepositoryId,
) -> Result<(), StoreError> {
    let exists: i64 = sqlx::query_scalar(
        "SELECT EXISTS(
             SELECT 1
             FROM artifacts artifact
             JOIN worker_runs worker
               ON worker.worker_run_id = artifact.producer_worker_run_id
             WHERE artifact.artifact_id = ?
               AND artifact.project_id = ?
               AND artifact.producer_repository_id = ?
               AND artifact.kind = 'implementation_report'
               AND worker.role = 'executor'
               AND worker.status = 'completed'
               AND worker.work_item_id = ?
               AND worker.worker_run_id = (
                   SELECT latest.worker_run_id
                   FROM worker_runs latest
                   WHERE latest.work_item_id = ? AND latest.role = 'executor'
                   ORDER BY latest.rowid DESC
                   LIMIT 1
               )
         )",
    )
    .bind(artifact_id.to_string())
    .bind(project_id.to_string())
    .bind(owner_repository_id.to_string())
    .bind(work_item_id.to_string())
    .bind(work_item_id.to_string())
    .fetch_one(&mut **transaction)
    .await?;
    if exists == 0 {
        return Err(StoreError::Conflict(
            "implementation artifact must be produced by the completed current executor for this work item"
                .to_owned(),
        ));
    }
    Ok(())
}

async fn validate_audit_work_item_completion(
    transaction: &mut Transaction<'_, Sqlite>,
    work_item_id: WorkItemId,
    owner_repository_id: RepositoryId,
) -> Result<(), StoreError> {
    let valid: i64 = sqlx::query_scalar(
        "SELECT EXISTS(
             SELECT 1
             FROM worker_runs worker
             JOIN artifacts report
               ON report.producer_worker_run_id = worker.worker_run_id
              AND report.kind = 'audit_report'
             WHERE worker.work_item_id = ?
               AND worker.repository_id = ?
               AND worker.role = 'auditor'
               AND worker.status = 'completed'
         )",
    )
    .bind(work_item_id.to_string())
    .bind(owner_repository_id.to_string())
    .fetch_one(&mut **transaction)
    .await?;
    if valid == 0 {
        return Err(StoreError::Conflict(
            "completing an audit work item requires its completed repository-local auditor and audit_report"
                .to_owned(),
        ));
    }
    Ok(())
}

async fn apply_completed_audit_outcomes(
    transaction: &mut Transaction<'_, Sqlite>,
    audit_work_item_id: WorkItemId,
    now: &str,
) -> Result<Vec<WorkItemId>, StoreError> {
    let rows = sqlx::query(
        "SELECT outcome.scoped_work_item_id, outcome.verdict,
                scope.audit_iteration, item.max_audit_iterations
         FROM worker_runs worker
         JOIN audit_outcomes outcome
           ON outcome.audit_worker_run_id = worker.worker_run_id
         JOIN work_item_audit_scope scope
           ON scope.audit_work_item_id = worker.work_item_id
          AND scope.scoped_work_item_id = outcome.scoped_work_item_id
         JOIN work_items item ON item.work_item_id = outcome.scoped_work_item_id
         WHERE worker.work_item_id = ? AND worker.role = 'auditor'
           AND worker.status = 'completed'",
    )
    .bind(audit_work_item_id.to_string())
    .fetch_all(&mut **transaction)
    .await?;
    let mut changed = Vec::with_capacity(rows.len());
    for row in rows {
        let work_item_id = WorkItemId::parse(row.try_get("scoped_work_item_id")?)?;
        let verdict: AuditVerdict = row
            .try_get::<String, _>("verdict")?
            .parse()
            .map_err(StoreError::Conflict)?;
        let iteration: i64 = row.try_get("audit_iteration")?;
        let max_iterations: i64 = row.try_get("max_audit_iterations")?;
        let status = match verdict {
            AuditVerdict::Passed => VerificationStatus::Verified,
            AuditVerdict::ChangesRequired if iteration < max_iterations => {
                VerificationStatus::RemediationRequired
            }
            AuditVerdict::Blocked if iteration < max_iterations => {
                VerificationStatus::PendingReaudit
            }
            AuditVerdict::ChangesRequired | AuditVerdict::Blocked => {
                VerificationStatus::VerificationBlocked
            }
        };
        sqlx::query(
            "UPDATE work_items
             SET verification_status = ?, revision = revision + 1, updated_at = ?
             WHERE work_item_id = ? AND kind = 'implementation'",
        )
        .bind(status.as_str())
        .bind(now)
        .bind(work_item_id.to_string())
        .execute(&mut **transaction)
        .await?;
        changed.push(work_item_id);
    }
    Ok(changed)
}

async fn set_audit_scope_retry_status(
    transaction: &mut Transaction<'_, Sqlite>,
    audit_work_item_id: WorkItemId,
    now: &str,
) -> Result<Vec<WorkItemId>, StoreError> {
    let rows = sqlx::query(
        "SELECT scoped_work_item_id, audit_iteration
         FROM work_item_audit_scope WHERE audit_work_item_id = ?",
    )
    .bind(audit_work_item_id.to_string())
    .fetch_all(&mut **transaction)
    .await?;
    let mut changed = Vec::with_capacity(rows.len());
    for row in rows {
        let work_item_id = WorkItemId::parse(row.try_get("scoped_work_item_id")?)?;
        let status = if row.try_get::<i64, _>("audit_iteration")? == 1 {
            VerificationStatus::PendingAudit
        } else {
            VerificationStatus::PendingReaudit
        };
        sqlx::query(
            "UPDATE work_items SET verification_status = ?, revision = revision + 1, updated_at = ?
             WHERE work_item_id = ?",
        )
        .bind(status.as_str())
        .bind(now)
        .bind(work_item_id.to_string())
        .execute(&mut **transaction)
        .await?;
        changed.push(work_item_id);
    }
    Ok(changed)
}

async fn set_remediation_scope_status(
    transaction: &mut Transaction<'_, Sqlite>,
    audit_run_id: WorkerRunId,
    status: VerificationStatus,
    now: &str,
) -> Result<Vec<WorkItemId>, StoreError> {
    let rows = sqlx::query(
        "SELECT scoped_work_item_id FROM audit_outcomes
         WHERE audit_worker_run_id = ? AND verdict = 'changes_required'",
    )
    .bind(audit_run_id.to_string())
    .fetch_all(&mut **transaction)
    .await?;
    let mut changed = Vec::with_capacity(rows.len());
    for row in rows {
        let work_item_id = WorkItemId::parse(row.try_get("scoped_work_item_id")?)?;
        sqlx::query(
            "UPDATE work_items SET verification_status = ?, revision = revision + 1, updated_at = ?
             WHERE work_item_id = ?",
        )
        .bind(status.as_str())
        .bind(now)
        .bind(work_item_id.to_string())
        .execute(&mut **transaction)
        .await?;
        changed.push(work_item_id);
    }
    Ok(changed)
}

async fn unlock_dependents(
    transaction: &mut Transaction<'_, Sqlite>,
    completed_id: WorkItemId,
    now: &str,
) -> Result<Vec<WorkItemId>, StoreError> {
    let rows = sqlx::query(
        "SELECT work_item_id FROM work_item_dependencies WHERE dependency_work_item_id = ?",
    )
    .bind(completed_id.to_string())
    .fetch_all(&mut **transaction)
    .await?;
    let mut unlocked = Vec::new();
    for row in rows {
        let item_id = WorkItemId::parse(row.try_get("work_item_id")?)?;
        let remaining: i64 = sqlx::query_scalar(
            "SELECT COUNT(*)
             FROM work_item_dependencies d
             JOIN work_items dependency ON dependency.work_item_id = d.dependency_work_item_id
             WHERE d.work_item_id = ? AND dependency.status <> 'completed'",
        )
        .bind(item_id.to_string())
        .fetch_one(&mut **transaction)
        .await?;
        if remaining == 0 {
            let result = sqlx::query(
                "UPDATE work_items
                 SET status = 'ready', revision = revision + 1, updated_at = ?
                 WHERE work_item_id = ? AND status = 'planned'",
            )
            .bind(now)
            .bind(item_id.to_string())
            .execute(&mut **transaction)
            .await?;
            if result.rows_affected() == 1 {
                unlocked.push(item_id);
            }
        }
    }
    Ok(unlocked)
}

async fn append_project_event(
    transaction: &mut Transaction<'_, Sqlite>,
    project_id: ProjectId,
    kind: EventKind,
    entity_type: EventEntityType,
    entity_id: &str,
    now: &str,
) -> Result<Vec<RepositoryId>, StoreError> {
    let rows = sqlx::query("SELECT repository_id FROM repositories WHERE project_id = ?")
        .bind(project_id.to_string())
        .fetch_all(&mut **transaction)
        .await?;
    let mut recipients = Vec::with_capacity(rows.len());
    for row in rows {
        let repository_id = RepositoryId::parse(row.try_get("repository_id")?)?;
        append_event(
            transaction,
            project_id,
            repository_id,
            kind,
            entity_type,
            entity_id,
            now,
        )
        .await?;
        recipients.push(repository_id);
    }
    Ok(recipients)
}

async fn append_work_item_events(
    transaction: &mut Transaction<'_, Sqlite>,
    project_id: ProjectId,
    work_item_ids: &[WorkItemId],
    now: &str,
) -> Result<Vec<RepositoryId>, StoreError> {
    let mut recipients = Vec::new();
    for work_item_id in work_item_ids {
        recipients = append_project_event(
            transaction,
            project_id,
            EventKind::WorkItemUpdated,
            EventEntityType::WorkItem,
            &work_item_id.to_string(),
            now,
        )
        .await?;
    }
    Ok(recipients)
}

pub(super) async fn attach_worker_to_work_item(
    transaction: &mut Transaction<'_, Sqlite>,
    worker_run_id: WorkerRunId,
    command: &CreateWorkerRun,
    repository_id: RepositoryId,
    project_id: ProjectId,
    now: &str,
) -> Result<(Vec<RepositoryId>, Vec<AuditScopeItem>), StoreError> {
    let Some(work_item_id) = command.work_item_id else {
        return Ok((Vec::new(), Vec::new()));
    };
    let item = get_work_item_tx(transaction, work_item_id).await?;
    ensure_work_item_project(transaction, &item, project_id).await?;
    match (command.role, item.kind) {
        (WorkerRole::Executor, WorkItemKind::Implementation) => {
            if command.addresses_audit_run_id.is_some() {
                return Err(StoreError::Conflict(
                    "implementation executor cannot address an audit run".to_owned(),
                ));
            }
        }
        (WorkerRole::Executor, WorkItemKind::Remediation) => {
            if command.addresses_audit_run_id != item.addresses_audit_run_id {
                return Err(StoreError::Conflict(
                    "remediation executor must use the audit run linked by its work item"
                        .to_owned(),
                ));
            }
        }
        (WorkerRole::Auditor, WorkItemKind::Audit) => {}
        _ => {
            return Err(StoreError::Conflict(format!(
                "{} workers cannot attach to {} work items",
                command.role,
                item.kind.as_str()
            )));
        }
    }
    let blocking_attempts: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM worker_runs
         WHERE work_item_id = ? AND worker_run_id <> ?
           AND status IN ('created', 'active', 'completed')",
    )
    .bind(work_item_id.to_string())
    .bind(worker_run_id.to_string())
    .fetch_one(&mut **transaction)
    .await?;
    if blocking_attempts != 0 {
        return Err(StoreError::Conflict(format!(
            "{} work item {} already has an active or completed worker attempt",
            item.kind.as_str(),
            item.work_item_id
        )));
    }
    if item.owner_repository_id != repository_id
        || item.assignee_instance_id != Some(command.parent_instance_id)
        || !matches!(
            item.status,
            WorkItemStatus::Claimed | WorkItemStatus::InProgress
        )
        || item
            .claim_expires_at
            .as_deref()
            .is_none_or(|value| value <= now)
    {
        return Err(StoreError::Conflict(
            "worker work item must have an active claim owned by its parent orchestrator"
                .to_owned(),
        ));
    }
    let audit_scope = load_audit_scope(transaction, work_item_id).await?;
    let mut changed_items = Vec::new();
    match item.kind {
        WorkItemKind::Audit => {
            for scope_item in &audit_scope {
                sqlx::query(
                    "UPDATE work_items
                     SET verification_status = 'auditing', revision = revision + 1, updated_at = ?
                     WHERE work_item_id = ?",
                )
                .bind(now)
                .bind(scope_item.work_item_id.to_string())
                .execute(&mut **transaction)
                .await?;
                changed_items.push(scope_item.work_item_id);
            }
        }
        WorkItemKind::Remediation => {
            changed_items.extend(
                set_remediation_scope_status(
                    transaction,
                    item.addresses_audit_run_id
                        .expect("validated remediation link"),
                    VerificationStatus::Remediating,
                    now,
                )
                .await?,
            );
        }
        WorkItemKind::Implementation => {}
    }
    if item.status == WorkItemStatus::Claimed {
        sqlx::query(
            "UPDATE work_items
             SET status = 'in_progress', revision = revision + 1, updated_at = ?
             WHERE work_item_id = ? AND revision = ? AND status = 'claimed'",
        )
        .bind(now)
        .bind(work_item_id.to_string())
        .bind(item.revision as i64)
        .execute(&mut **transaction)
        .await?;
        changed_items.push(work_item_id);
    }
    let recipients = append_work_item_events(transaction, project_id, &changed_items, now).await?;
    Ok((recipients, audit_scope))
}

async fn load_audit_scope(
    transaction: &mut Transaction<'_, Sqlite>,
    work_item_id: WorkItemId,
) -> Result<Vec<AuditScopeItem>, StoreError> {
    let rows = sqlx::query(
        "SELECT scoped_work_item_id, audit_iteration, max_iterations
         FROM work_item_audit_scope
         WHERE audit_work_item_id = ?
         ORDER BY scoped_work_item_id",
    )
    .bind(work_item_id.to_string())
    .fetch_all(&mut **transaction)
    .await?;
    rows.iter()
        .map(|row| {
            Ok(AuditScopeItem {
                work_item_id: WorkItemId::parse(row.try_get("scoped_work_item_id")?)?,
                iteration: row.try_get::<i64, _>("audit_iteration")? as u8,
                max_iterations: row.try_get::<i64, _>("max_iterations")? as u8,
            })
        })
        .collect()
}

pub(super) async fn link_audit_report(
    transaction: &mut Transaction<'_, Sqlite>,
    actor: &ResolvedActor,
    artifact: &Artifact,
    audit_outcomes: &[AuditOutcomeInput],
    now: &str,
) -> Result<Vec<RepositoryId>, StoreError> {
    if artifact.kind != ArtifactKind::AuditReport {
        return Ok(Vec::new());
    }
    let worker_run_id = actor.worker_run_id.ok_or_else(|| {
        StoreError::Conflict("audit_report must be published by an active auditor run".to_owned())
    })?;
    let role: String = sqlx::query_scalar("SELECT role FROM worker_runs WHERE worker_run_id = ?")
        .bind(worker_run_id.to_string())
        .fetch_one(&mut **transaction)
        .await?;
    if role != "auditor" {
        return Err(StoreError::Conflict(
            "audit_report must be published by an auditor run".to_owned(),
        ));
    }
    let audit_work_item_id: String =
        sqlx::query_scalar("SELECT work_item_id FROM worker_runs WHERE worker_run_id = ?")
            .bind(worker_run_id.to_string())
            .fetch_one(&mut **transaction)
            .await?;
    for outcome in audit_outcomes {
        sqlx::query(
            "INSERT INTO audit_outcomes(
                 audit_artifact_id, audit_worker_run_id, audit_work_item_id,
                 scoped_work_item_id, verdict, summary
             ) VALUES (?, ?, ?, ?, ?, ?)",
        )
        .bind(artifact.artifact_id.to_string())
        .bind(worker_run_id.to_string())
        .bind(&audit_work_item_id)
        .bind(outcome.work_item_id.to_string())
        .bind(outcome.verdict.as_str())
        .bind(outcome.summary.trim())
        .execute(&mut **transaction)
        .await?;
    }
    let rows = sqlx::query(
        "SELECT audit.work_item_id
         FROM worker_run_audit_items audit
         JOIN work_items item ON item.work_item_id = audit.work_item_id
         JOIN features feature ON feature.feature_id = item.feature_id
         WHERE audit.worker_run_id = ? AND feature.project_id = ?",
    )
    .bind(worker_run_id.to_string())
    .bind(actor.project_id.to_string())
    .fetch_all(&mut **transaction)
    .await?;
    let mut work_item_ids = Vec::with_capacity(rows.len());
    for row in rows {
        let work_item_id = WorkItemId::parse(row.try_get("work_item_id")?)?;
        sqlx::query(
            "UPDATE work_items
             SET latest_audit_artifact_id = ?, revision = revision + 1, updated_at = ?
             WHERE work_item_id = ?",
        )
        .bind(artifact.artifact_id.to_string())
        .bind(now)
        .bind(work_item_id.to_string())
        .execute(&mut **transaction)
        .await?;
        work_item_ids.push(work_item_id);
    }
    sqlx::query(
        "UPDATE work_items
         SET latest_audit_artifact_id = ?, revision = revision + 1, updated_at = ?
         WHERE work_item_id = ? AND kind = 'audit'",
    )
    .bind(artifact.artifact_id.to_string())
    .bind(now)
    .bind(&audit_work_item_id)
    .execute(&mut **transaction)
    .await?;
    work_item_ids.push(WorkItemId::parse(&audit_work_item_id)?);
    append_work_item_events(transaction, actor.project_id, &work_item_ids, now).await
}

pub(super) async fn validate_audit_report_publication(
    transaction: &mut Transaction<'_, Sqlite>,
    actor: &ResolvedActor,
    audit_outcomes: &[AuditOutcomeInput],
) -> Result<(), StoreError> {
    let worker_run_id = actor.worker_run_id.ok_or_else(|| {
        StoreError::Conflict("audit_report must be published by an active auditor run".to_owned())
    })?;
    let row = sqlx::query(
        "SELECT role,
                (SELECT COUNT(*) FROM artifacts
                 WHERE producer_worker_run_id = worker_runs.worker_run_id
                   AND kind = 'audit_report') AS report_count
         FROM worker_runs WHERE worker_run_id = ?",
    )
    .bind(worker_run_id.to_string())
    .fetch_one(&mut **transaction)
    .await?;
    let role: String = row.try_get("role")?;
    let report_count: i64 = row.try_get("report_count")?;
    if role != "auditor" || report_count != 0 {
        return Err(StoreError::Conflict(
            "an auditor run can publish exactly one audit_report".to_owned(),
        ));
    }
    let scope_rows = sqlx::query(
        "SELECT work_item_id FROM worker_run_audit_items
         WHERE worker_run_id = ? ORDER BY work_item_id",
    )
    .bind(worker_run_id.to_string())
    .fetch_all(&mut **transaction)
    .await?;
    let expected = scope_rows
        .iter()
        .map(|row| row.try_get::<String, _>("work_item_id"))
        .collect::<Result<BTreeSet<_>, _>>()?;
    let mut actual = BTreeSet::new();
    for outcome in audit_outcomes {
        if outcome.summary.trim().is_empty() {
            return Err(StoreError::Conflict(
                "each audit outcome requires a non-empty summary".to_owned(),
            ));
        }
        if !actual.insert(outcome.work_item_id.to_string()) {
            return Err(StoreError::Conflict(format!(
                "audit outcome for work item {} appears more than once",
                outcome.work_item_id
            )));
        }
    }
    if actual != expected {
        return Err(StoreError::Conflict(
            "audit_outcomes must cover every scoped work item exactly once".to_owned(),
        ));
    }
    Ok(())
}
