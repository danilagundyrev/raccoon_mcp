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
                 feature_id, project_id, created_by_instance_id, title, summary, created_at, updated_at
             ) VALUES (?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(feature_id.to_string())
        .bind(actor.project_id.to_string())
        .bind(command.parent_instance_id.to_string())
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

    pub async fn list_features(&self, project_id: ProjectId) -> Result<Vec<Feature>, StoreError> {
        let rows = sqlx::query("SELECT * FROM features WHERE project_id = ? ORDER BY created_at")
            .bind(project_id.to_string())
            .fetch_all(&self.pool)
            .await?;
        rows.iter().map(feature_from_row).collect()
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
        ensure_feature_project(&mut transaction, command.feature_id, actor.project_id).await?;
        ensure_repository_project(
            &mut transaction,
            command.owner_repository_id,
            actor.project_id,
        )
        .await?;
        validate_work_item_relations(
            &mut transaction,
            command.feature_id,
            None,
            command.parent_work_item_id,
            &command.dependency_ids,
        )
        .await?;
        let status = if dependencies_completed(&mut transaction, &command.dependency_ids).await? {
            WorkItemStatus::Ready
        } else {
            WorkItemStatus::Planned
        };
        sqlx::query(
            "INSERT INTO work_items(
                 work_item_id, feature_id, parent_work_item_id, owner_repository_id,
                 assignee_instance_id, title, summary, priority, status, revision,
                 max_audit_iterations, claim_expires_at, implementation_artifact_id,
                 latest_audit_artifact_id, created_at, updated_at, finished_at
             ) VALUES (?, ?, ?, ?, NULL, ?, ?, ?, ?, 1, ?, NULL, NULL, NULL, ?, ?, NULL)",
        )
        .bind(work_item_id.to_string())
        .bind(command.feature_id.to_string())
        .bind(command.parent_work_item_id.map(|id| id.to_string()))
        .bind(command.owner_repository_id.to_string())
        .bind(command.title)
        .bind(command.summary)
        .bind(i64::from(command.priority))
        .bind(status.as_str())
        .bind(i64::from(command.max_audit_iterations))
        .bind(&now)
        .bind(&now)
        .execute(&mut *transaction)
        .await?;
        replace_dependencies(&mut transaction, work_item_id, &command.dependency_ids).await?;
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
        get_work_item_pool(&self.pool, work_item_id).await
    }

    pub async fn list_work_items(
        &self,
        feature_id: FeatureId,
    ) -> Result<Vec<WorkItem>, StoreError> {
        let rows = sqlx::query(
            "SELECT work_item_id FROM work_items
             WHERE feature_id = ? ORDER BY priority DESC, created_at",
        )
        .bind(feature_id.to_string())
        .fetch_all(&self.pool)
        .await?;
        let mut items = Vec::with_capacity(rows.len());
        for row in rows {
            items.push(
                get_work_item_pool(&self.pool, WorkItemId::parse(row.try_get("work_item_id")?)?)
                    .await?,
            );
        }
        Ok(items)
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
        if next_status == WorkItemStatus::Completed {
            let artifact_id = implementation_artifact_id.ok_or_else(|| {
                StoreError::Conflict(
                    "completing a work item requires implementation_artifact_id".to_owned(),
                )
            })?;
            validate_implementation_artifact(
                &mut transaction,
                artifact_id,
                current.work_item_id,
                actor.project_id,
                current.owner_repository_id,
                current.assignee_instance_id,
            )
            .await?;
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
                 implementation_artifact_id = ?, updated_at = ?, finished_at = ?
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

async fn get_work_item_pool(
    pool: &SqlitePool,
    work_item_id: WorkItemId,
) -> Result<WorkItem, StoreError> {
    let row = sqlx::query("SELECT * FROM work_items WHERE work_item_id = ?")
        .bind(work_item_id.to_string())
        .fetch_optional(pool)
        .await?
        .ok_or_else(|| StoreError::NotFound(format!("work item {work_item_id}")))?;
    let dependency_rows = sqlx::query(
        "SELECT dependency_work_item_id FROM work_item_dependencies
         WHERE work_item_id = ? ORDER BY dependency_work_item_id",
    )
    .bind(work_item_id.to_string())
    .fetch_all(pool)
    .await?;
    work_item_from_row(&row, &dependency_rows)
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
    work_item_from_row(&row, &dependency_rows)
}

fn work_item_from_row(
    row: &SqliteRow,
    dependency_rows: &[SqliteRow],
) -> Result<WorkItem, StoreError> {
    let status: String = row.try_get("status")?;
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
        dependency_ids: dependency_rows
            .iter()
            .map(|row| Ok(WorkItemId::parse(row.try_get("dependency_work_item_id")?)?))
            .collect::<Result<Vec<_>, StoreError>>()?,
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
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
        finished_at: row.try_get("finished_at")?,
    })
}

fn feature_from_row(row: &SqliteRow) -> Result<Feature, StoreError> {
    Ok(Feature {
        feature_id: FeatureId::parse(row.try_get("feature_id")?)?,
        project_id: ProjectId::parse(row.try_get("project_id")?)?,
        created_by_instance_id: InstanceId::parse(row.try_get("created_by_instance_id")?)?,
        title: row.try_get("title")?,
        summary: row.try_get("summary")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
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

fn ensure_editable_shape(item: &WorkItem) -> Result<(), StoreError> {
    if !matches!(item.status, WorkItemStatus::Planned | WorkItemStatus::Ready) {
        return Err(StoreError::Conflict(
            "owner, parent and dependencies can only change while planned or ready".to_owned(),
        ));
    }
    Ok(())
}

fn validate_status_transition(from: WorkItemStatus, to: WorkItemStatus) -> Result<(), StoreError> {
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
                WorkItemStatus::InProgress | WorkItemStatus::Cancelled
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
        ensure_item_feature(transaction, *id, feature_id).await?;
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
    for dependency_id in dependency_ids {
        sqlx::query(
            "INSERT INTO work_item_dependencies(work_item_id, dependency_work_item_id) VALUES (?, ?)",
        )
        .bind(work_item_id.to_string())
        .bind(dependency_id.to_string())
        .execute(&mut **transaction)
        .await?;
    }
    Ok(())
}

async fn validate_implementation_artifact(
    transaction: &mut Transaction<'_, Sqlite>,
    artifact_id: ArtifactId,
    work_item_id: WorkItemId,
    project_id: ProjectId,
    owner_repository_id: RepositoryId,
    assignee_instance_id: Option<InstanceId>,
) -> Result<(), StoreError> {
    let assignee_instance_id = assignee_instance_id.ok_or_else(|| {
        StoreError::Conflict(
            "completed work item must still be owned by its claimed orchestrator".to_owned(),
        )
    })?;
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
               AND worker.parent_instance_id = ?
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
    .bind(assignee_instance_id.to_string())
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
    command: &CreateWorkerRun,
    repository_id: RepositoryId,
    project_id: ProjectId,
    now: &str,
) -> Result<Vec<RepositoryId>, StoreError> {
    let Some(work_item_id) = command.work_item_id else {
        return Ok(Vec::new());
    };
    if command.role != WorkerRole::Executor {
        return Err(StoreError::Conflict(
            "only executor runs can link a single work_item_id".to_owned(),
        ));
    }
    let item = get_work_item_tx(transaction, work_item_id).await?;
    ensure_work_item_project(transaction, &item, project_id).await?;
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
        append_work_item_events(transaction, project_id, &[work_item_id], now).await
    } else {
        Ok(Vec::new())
    }
}

pub(super) async fn link_audit_report(
    transaction: &mut Transaction<'_, Sqlite>,
    actor: &ResolvedActor,
    artifact: &Artifact,
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
    append_work_item_events(transaction, actor.project_id, &work_item_ids, now).await
}

pub(super) async fn validate_audit_report_publication(
    transaction: &mut Transaction<'_, Sqlite>,
    actor: &ResolvedActor,
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
    Ok(())
}
