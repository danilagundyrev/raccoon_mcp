use super::*;

impl SqliteStore {
    pub async fn list_worker_runs(
        &self,
        actor_ref: ActorRef,
        filter: WorkerRunListFilter,
        after_worker_run_id: Option<WorkerRunId>,
        limit: u8,
    ) -> Result<WorkerRunPage, StoreError> {
        validate_history_limit(limit)?;
        let actor = self.resolve_actor_readonly(actor_ref, &utc_now()).await?;
        let cursor = if let Some(cursor_id) = after_worker_run_id {
            Some(
                sqlx::query(
                    "SELECT w.created_at, w.worker_run_id FROM worker_runs w
                     JOIN repositories r ON r.repository_id = w.repository_id
                     WHERE w.worker_run_id = ? AND r.project_id = ?
                       AND (? IS NOT NULL OR w.repository_id = ?)",
                )
                .bind(cursor_id.to_string())
                .bind(actor.project_id.to_string())
                .bind(actor.instance_id.map(|id| id.to_string()))
                .bind(actor.repository_id.to_string())
                .fetch_optional(&self.pool)
                .await?
                .ok_or_else(|| StoreError::NotFound(format!("worker-run cursor {cursor_id}")))?,
            )
        } else {
            None
        };
        let mut query = QueryBuilder::<Sqlite>::new(
            "SELECT w.worker_run_id, w.repository_id, w.parent_instance_id, w.role, w.status,
                    w.task_summary, w.result_summary, w.error_summary, w.addresses_audit_run_id,
                    w.work_item_id, w.request_id, w.created_at, w.last_heartbeat_at,
                    w.lease_expires_at, w.finished_at
             FROM worker_runs w JOIN repositories r ON r.repository_id = w.repository_id
             WHERE r.project_id = ",
        );
        query.push_bind(actor.project_id.to_string());
        if actor.instance_id.is_none() {
            query
                .push(" AND w.repository_id = ")
                .push_bind(actor.repository_id.to_string());
        }
        if let Some(repository_id) = filter.repository_id {
            query
                .push(" AND w.repository_id = ")
                .push_bind(repository_id.to_string());
        }
        if let Some(feature_id) = filter.feature_id {
            query
                .push(" AND (EXISTS (SELECT 1 FROM work_items wi WHERE wi.work_item_id = w.work_item_id AND wi.feature_id = ")
                .push_bind(feature_id.to_string())
                .push(") OR EXISTS (SELECT 1 FROM requests rq WHERE rq.request_id = w.request_id AND rq.feature_id = ")
                .push_bind(feature_id.to_string())
                .push("))");
        }
        if let Some(work_item_id) = filter.work_item_id {
            query
                .push(" AND w.work_item_id = ")
                .push_bind(work_item_id.to_string());
        }
        if let Some(role) = filter.role {
            query.push(" AND w.role = ").push_bind(role.as_str());
        }
        if let Some(status) = filter.status {
            query.push(" AND w.status = ").push_bind(status.as_str());
        }
        if let Some(audit_run_id) = filter.addresses_audit_run_id {
            query
                .push(" AND w.addresses_audit_run_id = ")
                .push_bind(audit_run_id.to_string());
        }
        push_history_cursor(
            &mut query,
            cursor.as_ref(),
            "w.created_at",
            "w.worker_run_id",
        )?;
        query
            .push(" ORDER BY w.created_at, w.worker_run_id LIMIT ")
            .push_bind(i64::from(limit) + 1);
        let rows = query.build().fetch_all(&self.pool).await?;
        let has_more = rows.len() > usize::from(limit);
        let items = rows
            .iter()
            .take(usize::from(limit))
            .map(worker_run_summary_from_row)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(WorkerRunPage {
            next_cursor: items.last().map(|item| item.worker_run_id),
            items,
            has_more,
        })
    }

    pub async fn list_requests(
        &self,
        actor_ref: ActorRef,
        filter: RequestListFilter,
        after_request_id: Option<RequestId>,
        limit: u8,
    ) -> Result<RequestPage, StoreError> {
        validate_history_limit(limit)?;
        let actor = self.resolve_actor_readonly(actor_ref, &utc_now()).await?;
        let cursor = if let Some(cursor_id) = after_request_id {
            Some(
                sqlx::query(
                    "SELECT created_at, request_id FROM requests
                     WHERE request_id = ? AND project_id = ?
                       AND (requester_repository_id = ? OR target_repository_id = ?)",
                )
                .bind(cursor_id.to_string())
                .bind(actor.project_id.to_string())
                .bind(actor.repository_id.to_string())
                .bind(actor.repository_id.to_string())
                .fetch_optional(&self.pool)
                .await?
                .ok_or_else(|| StoreError::NotFound(format!("request cursor {cursor_id}")))?,
            )
        } else {
            None
        };
        let mut query = QueryBuilder::<Sqlite>::new(
            "SELECT request_id, project_id, requester_repository_id, target_repository_id,
                    feature_id, work_item_id, status, handler_requirement, answer_requirement,
                    question_summary, answer_message_id, created_at, updated_at, finished_at
             FROM requests WHERE project_id = ",
        );
        query
            .push_bind(actor.project_id.to_string())
            .push(" AND (requester_repository_id = ")
            .push_bind(actor.repository_id.to_string())
            .push(" OR target_repository_id = ")
            .push_bind(actor.repository_id.to_string())
            .push(")");
        if let Some(repository_id) = filter.requester_repository_id {
            query
                .push(" AND requester_repository_id = ")
                .push_bind(repository_id.to_string());
        }
        if let Some(repository_id) = filter.target_repository_id {
            query
                .push(" AND target_repository_id = ")
                .push_bind(repository_id.to_string());
        }
        if let Some(feature_id) = filter.feature_id {
            query
                .push(" AND feature_id = ")
                .push_bind(feature_id.to_string());
        }
        if let Some(work_item_id) = filter.work_item_id {
            query
                .push(" AND work_item_id = ")
                .push_bind(work_item_id.to_string());
        }
        if let Some(status) = filter.status {
            query.push(" AND status = ").push_bind(status.as_str());
        }
        push_history_cursor(&mut query, cursor.as_ref(), "created_at", "request_id")?;
        query
            .push(" ORDER BY created_at, request_id LIMIT ")
            .push_bind(i64::from(limit) + 1);
        let rows = query.build().fetch_all(&self.pool).await?;
        let has_more = rows.len() > usize::from(limit);
        let items = rows
            .iter()
            .take(usize::from(limit))
            .map(request_summary_from_row)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(RequestPage {
            next_cursor: items.last().map(|item| item.request_id),
            items,
            has_more,
        })
    }

    pub async fn list_deliveries(
        &self,
        actor_ref: ActorRef,
        status: DeliveryListStatus,
        after_delivery_id: Option<i64>,
        limit: u8,
    ) -> Result<DeliveryPage, StoreError> {
        validate_history_limit(limit)?;
        let actor = self.resolve_actor_readonly(actor_ref, &utc_now()).await?;
        let mut query = QueryBuilder::<Sqlite>::new(
            "SELECT d.delivery_id, d.status AS delivery_status, d.created_at AS delivered_at,
                    d.acked_at, d.acked_by_instance_id, d.acked_by_worker_run_id, m.*
             FROM deliveries d JOIN messages m ON m.message_id = d.message_id
             WHERE d.recipient_repository_id = ",
        );
        query.push_bind(actor.repository_id.to_string());
        match status {
            DeliveryListStatus::Pending => {
                query.push(" AND d.status = 'pending'");
            }
            DeliveryListStatus::Acknowledged => {
                query.push(" AND d.status = 'acked'");
            }
            DeliveryListStatus::All => {}
        }
        if let Some(cursor) = after_delivery_id {
            if cursor < 0 {
                return Err(StoreError::Conflict(
                    "delivery cursor must be non-negative".to_owned(),
                ));
            }
            query.push(" AND d.delivery_id > ").push_bind(cursor);
        }
        query
            .push(" ORDER BY d.delivery_id LIMIT ")
            .push_bind(i64::from(limit) + 1);
        let rows = query.build().fetch_all(&self.pool).await?;
        let has_more = rows.len() > usize::from(limit);
        let items = rows
            .iter()
            .take(usize::from(limit))
            .map(delivery_history_from_row)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(DeliveryPage {
            next_cursor: items.last().map(|item| item.delivery_id),
            items,
            has_more,
        })
    }

    pub async fn list_messages(
        &self,
        actor_ref: ActorRef,
        filter: MessageListFilter,
        after_message_id: Option<MessageId>,
        limit: u8,
    ) -> Result<MessagePage, StoreError> {
        validate_history_limit(limit)?;
        let actor = self.resolve_actor_readonly(actor_ref, &utc_now()).await?;
        let cursor = if let Some(cursor_id) = after_message_id {
            Some(
                sqlx::query(
                    "SELECT m.created_at, m.message_id FROM messages m
                     WHERE m.message_id = ? AND m.project_id = ? AND
                       (m.sender_repository_id = ? OR EXISTS (
                          SELECT 1 FROM deliveries d WHERE d.message_id = m.message_id
                            AND d.recipient_repository_id = ?))",
                )
                .bind(cursor_id.to_string())
                .bind(actor.project_id.to_string())
                .bind(actor.repository_id.to_string())
                .bind(actor.repository_id.to_string())
                .fetch_optional(&self.pool)
                .await?
                .ok_or_else(|| StoreError::NotFound(format!("message cursor {cursor_id}")))?,
            )
        } else {
            None
        };
        let mut query =
            QueryBuilder::<Sqlite>::new("SELECT m.* FROM messages m WHERE m.project_id = ");
        query
            .push_bind(actor.project_id.to_string())
            .push(" AND (m.sender_repository_id = ")
            .push_bind(actor.repository_id.to_string())
            .push(" OR EXISTS (SELECT 1 FROM deliveries access WHERE access.message_id = m.message_id AND access.recipient_repository_id = ")
            .push_bind(actor.repository_id.to_string())
            .push("))");
        if let Some(feature_id) = filter.feature_id {
            query
                .push(" AND m.feature_id = ")
                .push_bind(feature_id.to_string());
        }
        if let Some(work_item_id) = filter.work_item_id {
            query
                .push(" AND m.work_item_id = ")
                .push_bind(work_item_id.to_string());
        }
        if let Some(request_id) = filter.request_id {
            query
                .push(" AND m.request_id = ")
                .push_bind(request_id.to_string());
        }
        if let Some(thread_id) = filter.thread_id {
            query
                .push(" AND m.thread_id = ")
                .push_bind(thread_id.to_string());
        }
        if let Some(kind) = filter.kind {
            query.push(" AND m.kind = ").push_bind(kind.as_str());
        }
        if let Some(repository_id) = filter.producer_repository_id {
            query
                .push(" AND m.sender_repository_id = ")
                .push_bind(repository_id.to_string());
        }
        if let Some(repository_id) = filter.recipient_repository_id {
            query
                .push(" AND EXISTS (SELECT 1 FROM deliveries recipient WHERE recipient.message_id = m.message_id AND recipient.recipient_repository_id = ")
                .push_bind(repository_id.to_string())
                .push(")");
        }
        push_history_cursor(&mut query, cursor.as_ref(), "m.created_at", "m.message_id")?;
        query
            .push(" ORDER BY m.created_at, m.message_id LIMIT ")
            .push_bind(i64::from(limit) + 1);
        let rows = query.build().fetch_all(&self.pool).await?;
        let has_more = rows.len() > usize::from(limit);
        let items = rows
            .iter()
            .take(usize::from(limit))
            .map(message_from_row)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(MessagePage {
            next_cursor: items.last().map(|item| item.message_id),
            items,
            has_more,
        })
    }

    pub async fn list_artifacts(
        &self,
        actor_ref: ActorRef,
        filter: ArtifactListFilter,
        after_artifact_id: Option<ArtifactId>,
        limit: u8,
    ) -> Result<ArtifactPage, StoreError> {
        validate_history_limit(limit)?;
        let actor = self.resolve_actor_readonly(actor_ref, &utc_now()).await?;
        let cursor = if let Some(cursor_id) = after_artifact_id {
            let mut cursor_query = QueryBuilder::<Sqlite>::new(
                "SELECT a.created_at, a.artifact_id FROM artifacts a WHERE a.artifact_id = ",
            );
            cursor_query
                .push_bind(cursor_id.to_string())
                .push(" AND a.project_id = ")
                .push_bind(actor.project_id.to_string())
                .push(" AND ");
            push_artifact_access_predicate(
                &mut cursor_query,
                actor.repository_id,
                actor.instance_id.is_some(),
            );
            Some(
                cursor_query
                    .build()
                    .fetch_optional(&self.pool)
                    .await?
                    .ok_or_else(|| StoreError::NotFound(format!("artifact cursor {cursor_id}")))?,
            )
        } else {
            None
        };
        let mut query = QueryBuilder::<Sqlite>::new(
            "SELECT a.artifact_id, a.project_id, a.producer_repository_id,
                    a.producer_instance_id, a.producer_worker_run_id, a.kind, a.title,
                    a.content_type, a.created_at,
                    NULL AS feature_id, NULL AS work_item_id, NULL AS request_id,
                    NULL AS message_id
             FROM artifacts a WHERE a.project_id = ",
        );
        query.push_bind(actor.project_id.to_string()).push(" AND ");
        push_artifact_access_predicate(
            &mut query,
            actor.repository_id,
            actor.instance_id.is_some(),
        );
        if let Some(kind) = filter.kind {
            query.push(" AND a.kind = ").push_bind(kind.as_str());
        }
        if let Some(feature_id) = filter.feature_id {
            query
                .push(" AND (EXISTS (SELECT 1 FROM messages linked_message WHERE linked_message.artifact_id = a.artifact_id AND linked_message.feature_id = ")
                .push_bind(feature_id.to_string())
                .push(") OR EXISTS (SELECT 1 FROM work_items linked_item WHERE linked_item.implementation_artifact_id = a.artifact_id AND linked_item.feature_id = ")
                .push_bind(feature_id.to_string())
                .push(") OR EXISTS (SELECT 1 FROM audit_outcomes linked_audit JOIN work_items linked_audit_item ON linked_audit_item.work_item_id = linked_audit.audit_work_item_id WHERE linked_audit.audit_artifact_id = a.artifact_id AND linked_audit_item.feature_id = ")
                .push_bind(feature_id.to_string())
                .push(") OR EXISTS (SELECT 1 FROM requests linked_request WHERE linked_request.question_artifact_id = a.artifact_id AND linked_request.feature_id = ")
                .push_bind(feature_id.to_string())
                .push("))");
        }
        if let Some(work_item_id) = filter.work_item_id {
            query
                .push(" AND (EXISTS (SELECT 1 FROM messages linked_message WHERE linked_message.artifact_id = a.artifact_id AND linked_message.work_item_id = ")
                .push_bind(work_item_id.to_string())
                .push(") OR EXISTS (SELECT 1 FROM work_items linked_item WHERE linked_item.implementation_artifact_id = a.artifact_id AND linked_item.work_item_id = ")
                .push_bind(work_item_id.to_string())
                .push(") OR EXISTS (SELECT 1 FROM audit_outcomes linked_audit WHERE linked_audit.audit_artifact_id = a.artifact_id AND (linked_audit.audit_work_item_id = ")
                .push_bind(work_item_id.to_string())
                .push(" OR linked_audit.scoped_work_item_id = ")
                .push_bind(work_item_id.to_string())
                .push("))")
                .push(" OR EXISTS (SELECT 1 FROM requests linked_request WHERE linked_request.question_artifact_id = a.artifact_id AND linked_request.work_item_id = ")
                .push_bind(work_item_id.to_string())
                .push("))");
        }
        if let Some(request_id) = filter.request_id {
            query
                .push(" AND (EXISTS (SELECT 1 FROM messages linked_message WHERE linked_message.artifact_id = a.artifact_id AND linked_message.request_id = ")
                .push_bind(request_id.to_string())
                .push(") OR EXISTS (SELECT 1 FROM requests linked_request WHERE linked_request.question_artifact_id = a.artifact_id AND linked_request.request_id = ")
                .push_bind(request_id.to_string())
                .push("))");
        }
        if let Some(repository_id) = filter.producer_repository_id {
            query
                .push(" AND a.producer_repository_id = ")
                .push_bind(repository_id.to_string());
        }
        if let Some(worker_run_id) = filter.producer_worker_run_id {
            query
                .push(" AND a.producer_worker_run_id = ")
                .push_bind(worker_run_id.to_string());
        }
        push_history_cursor(&mut query, cursor.as_ref(), "a.created_at", "a.artifact_id")?;
        query
            .push(" ORDER BY a.created_at, a.artifact_id LIMIT ")
            .push_bind(i64::from(limit) + 1);
        let rows = query.build().fetch_all(&self.pool).await?;
        let has_more = rows.len() > usize::from(limit);
        let mut items = rows
            .iter()
            .take(usize::from(limit))
            .map(artifact_metadata_from_row)
            .collect::<Result<Vec<_>, _>>()?;
        populate_artifact_metadata_links(&self.pool, &mut items).await?;
        Ok(ArtifactPage {
            next_cursor: items.last().map(|item| item.artifact_id),
            items,
            has_more,
        })
    }
}

fn push_history_cursor<'a>(
    query: &mut QueryBuilder<'a, Sqlite>,
    cursor: Option<&SqliteRow>,
    created_at_column: &str,
    id_column: &str,
) -> Result<(), StoreError> {
    if let Some(cursor) = cursor {
        let created_at: String = cursor.try_get("created_at")?;
        let id: String = cursor.try_get(id_column.rsplit('.').next().unwrap_or(id_column))?;
        query
            .push(" AND (")
            .push(created_at_column)
            .push(" > ")
            .push_bind(created_at.clone())
            .push(" OR (")
            .push(created_at_column)
            .push(" = ")
            .push_bind(created_at)
            .push(" AND ")
            .push(id_column)
            .push(" > ")
            .push_bind(id)
            .push("))");
    }
    Ok(())
}

async fn populate_artifact_metadata_links(
    pool: &SqlitePool,
    items: &mut [ArtifactMetadata],
) -> Result<(), StoreError> {
    if items.is_empty() {
        return Ok(());
    }
    let positions = items
        .iter()
        .enumerate()
        .map(|(index, item)| (item.artifact_id, index))
        .collect::<HashMap<_, _>>();
    let artifact_ids = items
        .iter()
        .map(|item| item.artifact_id)
        .collect::<Vec<_>>();

    let mut links = QueryBuilder::<Sqlite>::new(
        "SELECT artifact_id, 1 AS relation_order, feature_id, work_item_id,
                NULL AS audit_work_item_id, NULL AS scoped_work_item_id,
                request_id, message_id
         FROM messages WHERE artifact_id IN (",
    );
    push_artifact_id_bindings(&mut links, &artifact_ids);
    links.push(
        ") UNION ALL
         SELECT question_artifact_id, 2, feature_id, work_item_id, NULL, NULL,
                request_id, NULL
         FROM requests WHERE question_artifact_id IN (",
    );
    push_artifact_id_bindings(&mut links, &artifact_ids);
    links.push(
        ") UNION ALL
         SELECT implementation_artifact_id, 3, feature_id, work_item_id,
                NULL, NULL, NULL, NULL
         FROM work_items WHERE implementation_artifact_id IN (",
    );
    push_artifact_id_bindings(&mut links, &artifact_ids);
    links.push(
        ") UNION ALL
         SELECT outcome.audit_artifact_id, 4, audit_item.feature_id, NULL,
                outcome.audit_work_item_id, outcome.scoped_work_item_id, NULL, NULL
         FROM audit_outcomes outcome
         JOIN work_items audit_item ON audit_item.work_item_id = outcome.audit_work_item_id
         WHERE outcome.audit_artifact_id IN (",
    );
    push_artifact_id_bindings(&mut links, &artifact_ids);
    links.push(
        ") ORDER BY artifact_id, relation_order, scoped_work_item_id,
                    work_item_id, request_id, message_id",
    );
    for row in links.build().fetch_all(pool).await? {
        let artifact_id = ArtifactId::parse(row.try_get("artifact_id")?)?;
        let metadata = &mut items[positions[&artifact_id]];
        set_optional_id(
            &mut metadata.feature_id,
            row.try_get("feature_id")?,
            FeatureId::parse,
        )?;
        set_optional_id(
            &mut metadata.work_item_id,
            row.try_get("work_item_id")?,
            WorkItemId::parse,
        )?;
        set_optional_id(
            &mut metadata.audit_work_item_id,
            row.try_get("audit_work_item_id")?,
            WorkItemId::parse,
        )?;
        set_optional_id(
            &mut metadata.request_id,
            row.try_get("request_id")?,
            RequestId::parse,
        )?;
        set_optional_id(
            &mut metadata.message_id,
            row.try_get("message_id")?,
            MessageId::parse,
        )?;
        if let Some(scoped_work_item_id) =
            parse_optional_id(row.try_get("scoped_work_item_id")?, WorkItemId::parse)?
        {
            metadata.scoped_work_item_ids.push(scoped_work_item_id);
        }
    }
    Ok(())
}

fn push_artifact_id_bindings(query: &mut QueryBuilder<'_, Sqlite>, artifact_ids: &[ArtifactId]) {
    let mut separated = query.separated(", ");
    for artifact_id in artifact_ids {
        separated.push_bind(artifact_id.to_string());
    }
}

fn set_optional_id<T: Copy>(
    target: &mut Option<T>,
    raw: Option<String>,
    parse: impl Fn(&str) -> Result<T, uuid::Error>,
) -> Result<(), StoreError> {
    if target.is_none() {
        *target = parse_optional_id(raw, parse)?;
    }
    Ok(())
}

fn worker_run_summary_from_row(row: &SqliteRow) -> Result<WorkerRunSummary, StoreError> {
    Ok(WorkerRunSummary {
        worker_run_id: WorkerRunId::parse(row.try_get("worker_run_id")?)?,
        repository_id: RepositoryId::parse(row.try_get("repository_id")?)?,
        parent_instance_id: InstanceId::parse(row.try_get("parent_instance_id")?)?,
        role: row
            .try_get::<String, _>("role")?
            .parse()
            .map_err(StoreError::Conflict)?,
        status: row
            .try_get::<String, _>("status")?
            .parse()
            .map_err(StoreError::Conflict)?,
        task_summary: row.try_get("task_summary")?,
        result_summary: row.try_get("result_summary")?,
        error_summary: row.try_get("error_summary")?,
        addresses_audit_run_id: parse_optional_id(
            row.try_get("addresses_audit_run_id")?,
            WorkerRunId::parse,
        )?,
        work_item_id: parse_optional_id(row.try_get("work_item_id")?, WorkItemId::parse)?,
        request_id: parse_optional_id(row.try_get("request_id")?, RequestId::parse)?,
        created_at: row.try_get("created_at")?,
        last_heartbeat_at: row.try_get("last_heartbeat_at")?,
        lease_expires_at: row.try_get("lease_expires_at")?,
        finished_at: row.try_get("finished_at")?,
    })
}

fn request_summary_from_row(row: &SqliteRow) -> Result<RequestSummary, StoreError> {
    Ok(RequestSummary {
        request_id: RequestId::parse(row.try_get("request_id")?)?,
        project_id: ProjectId::parse(row.try_get("project_id")?)?,
        requester_repository_id: RepositoryId::parse(row.try_get("requester_repository_id")?)?,
        target_repository_id: RepositoryId::parse(row.try_get("target_repository_id")?)?,
        feature_id: parse_optional_id(row.try_get("feature_id")?, FeatureId::parse)?,
        work_item_id: parse_optional_id(row.try_get("work_item_id")?, WorkItemId::parse)?,
        status: row
            .try_get::<String, _>("status")?
            .parse()
            .map_err(StoreError::Conflict)?,
        handler_requirement: row
            .try_get::<String, _>("handler_requirement")?
            .parse()
            .map_err(StoreError::Conflict)?,
        answer_requirement: row
            .try_get::<String, _>("answer_requirement")?
            .parse()
            .map_err(StoreError::Conflict)?,
        question_summary: row.try_get("question_summary")?,
        answer_message_id: parse_optional_id(row.try_get("answer_message_id")?, MessageId::parse)?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
        finished_at: row.try_get("finished_at")?,
    })
}

fn delivery_history_from_row(row: &SqliteRow) -> Result<DeliveryHistoryItem, StoreError> {
    let status: String = row.try_get("delivery_status")?;
    Ok(DeliveryHistoryItem {
        delivery_id: row.try_get("delivery_id")?,
        message: message_from_row(row)?,
        status: if status == "acked" {
            "acknowledged"
        } else {
            "pending"
        }
        .to_owned(),
        delivered_at: row.try_get("delivered_at")?,
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

fn artifact_metadata_from_row(row: &SqliteRow) -> Result<ArtifactMetadata, StoreError> {
    Ok(ArtifactMetadata {
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
        kind: row
            .try_get::<String, _>("kind")?
            .parse()
            .map_err(StoreError::Conflict)?,
        title: row.try_get("title")?,
        content_type: row.try_get("content_type")?,
        feature_id: parse_optional_id(row.try_get("feature_id")?, FeatureId::parse)?,
        work_item_id: parse_optional_id(row.try_get("work_item_id")?, WorkItemId::parse)?,
        audit_work_item_id: None,
        scoped_work_item_ids: Vec::new(),
        request_id: parse_optional_id(row.try_get("request_id")?, RequestId::parse)?,
        message_id: parse_optional_id(row.try_get("message_id")?, MessageId::parse)?,
        created_at: row.try_get("created_at")?,
    })
}
