use super::*;

impl SqliteStore {
    #[cfg(test)]
    pub async fn ask_repository(
        &self,
        command: AskRepository,
    ) -> Result<CoordinationRequest, StoreError> {
        self.ask_repository_idempotent(command, None).await
    }

    pub async fn ask_repository_idempotent(
        &self,
        command: AskRepository,
        idempotency_key: Option<&str>,
    ) -> Result<CoordinationRequest, StoreError> {
        validate_context_refs_value(&command.context_refs)?;
        let _write_guard = self.write_lock.lock().await;
        let request_fingerprint = ask_repository_fingerprint(&command)?;
        let now = utc_now();
        let mut transaction = self.pool.begin().await?;
        let actor = self
            .resolve_actor(&mut transaction, command.actor, &now)
            .await?;
        let actor_key = resolved_actor_key(&actor);
        if let Some(existing_id) = find_idempotent_result(
            &mut transaction,
            &actor_key,
            "ask_repository",
            idempotency_key,
            &request_fingerprint,
        )
        .await?
        {
            transaction.commit().await?;
            return self.get_request(RequestId::parse(&existing_id)?).await;
        }
        let target_project_id =
            repository_project_id(&mut transaction, command.target_repository_id).await?;
        if target_project_id != actor.project_id {
            return Err(StoreError::Conflict(
                "request target must belong to the actor project".to_owned(),
            ));
        }
        if command.target_repository_id == actor.repository_id {
            return Err(StoreError::Conflict(
                "request target must differ from the requester repository".to_owned(),
            ));
        }
        let (feature_id, work_item_id) = resolve_request_scope(
            &mut transaction,
            actor.project_id,
            command.feature_id,
            command.work_item_id,
        )
        .await?;
        let context_refs_json = serde_json::to_string(&command.context_refs)?;
        let request_id = RequestId::new();
        let question_artifact = command.body.map(|content| Artifact {
            artifact_id: ArtifactId::new(),
            project_id: actor.project_id,
            producer_repository_id: actor.repository_id,
            producer_instance_id: actor.instance_id,
            producer_worker_run_id: actor.worker_run_id,
            kind: ArtifactKind::Note,
            title: command.question_summary.clone(),
            content_type: "text/markdown".to_owned(),
            content,
            created_at: now.clone(),
        });
        if let Some(artifact) = &question_artifact {
            insert_artifact(&mut transaction, artifact).await?;
        }

        sqlx::query(
            "INSERT INTO requests(
                 request_id, project_id, requester_repository_id, target_repository_id,
                 created_by_instance_id, created_by_worker_run_id, feature_id, work_item_id,
                 status, handler_requirement, answer_requirement,
                 question_summary, question_artifact_id, context_refs_json, claimed_by_instance_id,
                 claimed_by_worker_run_id, claim_expires_at, failure_summary, failure_retryable,
                 answer_message_id, answer_provenance, answered_by_instance_id,
                 answered_by_worker_run_id, created_at, updated_at, finished_at
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?, ?, ?, ?, ?, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, ?, ?, NULL)",
        )
        .bind(request_id.to_string())
        .bind(actor.project_id.to_string())
        .bind(actor.repository_id.to_string())
        .bind(command.target_repository_id.to_string())
        .bind(actor.instance_id.map(|id| id.to_string()))
        .bind(actor.worker_run_id.map(|id| id.to_string()))
        .bind(feature_id.map(|id| id.to_string()))
        .bind(work_item_id.map(|id| id.to_string()))
        .bind(command.handler_requirement.as_str())
        .bind(command.answer_requirement.as_str())
        .bind(command.question_summary)
        .bind(question_artifact.as_ref().map(|artifact| artifact.artifact_id.to_string()))
        .bind(context_refs_json)
        .bind(&now)
        .bind(&now)
        .execute(&mut *transaction)
        .await?;
        append_event(
            &mut transaction,
            actor.project_id,
            command.target_repository_id,
            EventKind::RequestAvailable,
            EventEntityType::Request,
            &request_id.to_string(),
            &now,
        )
        .await?;
        record_idempotent_result(
            &mut transaction,
            &actor_key,
            "ask_repository",
            idempotency_key,
            &request_fingerprint,
            &request_id.to_string(),
            &now,
        )
        .await?;
        transaction.commit().await?;
        self.notifications.notify(command.target_repository_id);
        self.get_request(request_id).await
    }

    pub async fn get_request(
        &self,
        request_id: RequestId,
    ) -> Result<CoordinationRequest, StoreError> {
        let row = sqlx::query("SELECT * FROM requests WHERE request_id = ?")
            .bind(request_id.to_string())
            .fetch_optional(&self.pool)
            .await?
            .ok_or_else(|| StoreError::NotFound(format!("request {request_id}")))?;
        request_from_row(&row)
    }

    pub async fn read_request(
        &self,
        actor_ref: ActorRef,
        request_id: RequestId,
    ) -> Result<CoordinationRequest, StoreError> {
        let actor = self.resolve_actor_readonly(actor_ref, &utc_now()).await?;
        let row = sqlx::query(
            "SELECT * FROM requests
             WHERE request_id = ? AND project_id = ?
               AND (requester_repository_id = ? OR target_repository_id = ?)",
        )
        .bind(request_id.to_string())
        .bind(actor.project_id.to_string())
        .bind(actor.repository_id.to_string())
        .bind(actor.repository_id.to_string())
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| StoreError::NotFound(format!("accessible request {request_id}")))?;
        request_from_row(&row)
    }

    pub async fn claim_request(
        &self,
        actor_ref: ActorRef,
        request_id: RequestId,
    ) -> Result<CoordinationRequest, StoreError> {
        let _write_guard = self.write_lock.lock().await;
        let now = utc_now();
        let mut transaction = self.pool.begin().await?;
        let actor = self
            .resolve_actor(&mut transaction, actor_ref, &now)
            .await?;
        let request_row = sqlx::query("SELECT * FROM requests WHERE request_id = ?")
            .bind(request_id.to_string())
            .fetch_optional(&mut *transaction)
            .await?
            .ok_or_else(|| StoreError::NotFound(format!("request {request_id}")))?;
        let request = request_from_row(&request_row)?;
        if request.project_id != actor.project_id
            || request.target_repository_id != actor.repository_id
        {
            return Err(StoreError::NotFound(format!(
                "request {request_id} in actor repository"
            )));
        }
        ensure_request_handler(&mut transaction, &request, &actor).await?;
        let claim_expires_at = (Utc::now() + self.request_claim_lease_duration)
            .to_rfc3339_opts(SecondsFormat::Micros, true);
        let result = sqlx::query(
            "UPDATE requests
             SET status = 'claimed', claimed_by_instance_id = ?, claimed_by_worker_run_id = ?,
                 claim_expires_at = ?, updated_at = ?
             WHERE request_id = ? AND target_repository_id = ?
               AND (status = 'pending' OR (status = 'claimed' AND claim_expires_at <= ?))",
        )
        .bind(actor.instance_id.map(|id| id.to_string()))
        .bind(actor.worker_run_id.map(|id| id.to_string()))
        .bind(&claim_expires_at)
        .bind(&now)
        .bind(request_id.to_string())
        .bind(actor.repository_id.to_string())
        .bind(&now)
        .execute(&mut *transaction)
        .await?;
        if result.rows_affected() != 1 {
            let current_row = sqlx::query("SELECT * FROM requests WHERE request_id = ?")
                .bind(request_id.to_string())
                .fetch_optional(&mut *transaction)
                .await?
                .ok_or_else(|| StoreError::NotFound(format!("request {request_id}")))?;
            let current = request_from_row(&current_row)?;
            if current.target_repository_id == actor.repository_id
                && current.status == RequestStatus::Claimed
                && current
                    .claim_expires_at
                    .as_deref()
                    .is_some_and(|expires| expires > now.as_str())
            {
                return Err(claim_conflict(
                    "request",
                    request_id.to_string(),
                    "request is held by an active claim",
                    current.claim_expires_at,
                    &now,
                ));
            }
            return Err(request_transition_conflict(request_id, "claim"));
        }
        append_event(
            &mut transaction,
            actor.project_id,
            actor.repository_id,
            EventKind::RequestClaimed,
            EventEntityType::Request,
            &request_id.to_string(),
            &now,
        )
        .await?;
        transaction.commit().await?;
        self.notifications.notify(actor.repository_id);
        self.get_request(request_id).await
    }

    pub async fn renew_request_claim(
        &self,
        actor_ref: ActorRef,
        request_id: RequestId,
    ) -> Result<CoordinationRequest, StoreError> {
        let _write_guard = self.write_lock.lock().await;
        let now = utc_now();
        let mut transaction = self.pool.begin().await?;
        let actor = self
            .resolve_actor(&mut transaction, actor_ref, &now)
            .await?;
        let claim_expires_at = (Utc::now() + self.request_claim_lease_duration)
            .to_rfc3339_opts(SecondsFormat::Micros, true);
        let (owner_column, owner_id) = actor_owner(&actor);
        let query = format!(
            "UPDATE requests SET claim_expires_at = ?, updated_at = ?
             WHERE request_id = ? AND target_repository_id = ? AND status = 'claimed'
               AND {owner_column} = ? AND claim_expires_at > ?"
        );
        let result = sqlx::query(&query)
            .bind(claim_expires_at)
            .bind(&now)
            .bind(request_id.to_string())
            .bind(actor.repository_id.to_string())
            .bind(owner_id)
            .bind(&now)
            .execute(&mut *transaction)
            .await?;
        if result.rows_affected() != 1 {
            return Err(request_transition_conflict(request_id, "renew claim"));
        }
        transaction.commit().await?;
        self.get_request(request_id).await
    }

    pub async fn release_request_claim(
        &self,
        actor_ref: ActorRef,
        request_id: RequestId,
    ) -> Result<CoordinationRequest, StoreError> {
        let _write_guard = self.write_lock.lock().await;
        let now = utc_now();
        let mut transaction = self.pool.begin().await?;
        let actor = self
            .resolve_actor(&mut transaction, actor_ref, &now)
            .await?;
        let (owner_column, owner_id) = actor_owner(&actor);
        let query = format!(
            "UPDATE requests
             SET status = 'pending', claimed_by_instance_id = NULL,
                 claimed_by_worker_run_id = NULL, claim_expires_at = NULL, updated_at = ?
             WHERE request_id = ? AND target_repository_id = ? AND status = 'claimed'
               AND {owner_column} = ? AND claim_expires_at > ?"
        );
        let result = sqlx::query(&query)
            .bind(&now)
            .bind(request_id.to_string())
            .bind(actor.repository_id.to_string())
            .bind(owner_id)
            .bind(&now)
            .execute(&mut *transaction)
            .await?;
        if result.rows_affected() != 1 {
            return Err(request_transition_conflict(request_id, "release claim"));
        }
        append_event(
            &mut transaction,
            actor.project_id,
            actor.repository_id,
            EventKind::RequestAvailable,
            EventEntityType::Request,
            &request_id.to_string(),
            &now,
        )
        .await?;
        transaction.commit().await?;
        self.notifications.notify(actor.repository_id);
        self.get_request(request_id).await
    }

    pub async fn answer_request(
        &self,
        command: AnswerRequest,
    ) -> Result<AnswerRequestResult, StoreError> {
        let _write_guard = self.write_lock.lock().await;
        let now = utc_now();
        let mut transaction = self.pool.begin().await?;
        let actor = self
            .resolve_actor(&mut transaction, command.actor, &now)
            .await?;
        let request_row = sqlx::query("SELECT * FROM requests WHERE request_id = ?")
            .bind(command.request_id.to_string())
            .fetch_optional(&mut *transaction)
            .await?
            .ok_or_else(|| StoreError::NotFound(format!("request {}", command.request_id)))?;
        let request = request_from_row(&request_row)?;
        ensure_request_handler(&mut transaction, &request, &actor).await?;
        ensure_active_request_claim(&request, &actor, &now)?;
        if request.answer_requirement == AnswerRequirement::ArtifactRequired
            && command
                .body
                .as_deref()
                .is_none_or(|body| body.trim().is_empty())
        {
            return Err(StoreError::Conflict(
                "request requires a non-empty answer artifact body".to_owned(),
            ));
        }

        let artifact = command.body.map(|content| Artifact {
            artifact_id: ArtifactId::new(),
            project_id: actor.project_id,
            producer_repository_id: actor.repository_id,
            producer_instance_id: actor.instance_id,
            producer_worker_run_id: actor.worker_run_id,
            kind: ArtifactKind::Answer,
            title: command.answer_summary.clone(),
            content_type: command.content_type,
            content,
            created_at: now.clone(),
        });
        if let Some(artifact) = &artifact {
            insert_artifact(&mut transaction, artifact).await?;
        }
        let message = Message {
            message_id: MessageId::new(),
            project_id: actor.project_id,
            sender_repository_id: actor.repository_id,
            sender_instance_id: actor.instance_id,
            sender_worker_run_id: actor.worker_run_id,
            kind: MessageKind::Report,
            feature_id: request.feature_id,
            work_item_id: request.work_item_id,
            request_id: Some(command.request_id),
            thread_id: ThreadId::new(),
            correlation_id: Some(command.request_id.to_string()),
            in_reply_to: None,
            summary: command.answer_summary,
            artifact_id: artifact.as_ref().map(|value| value.artifact_id),
            created_at: now.clone(),
        };
        insert_message(&mut transaction, &message).await?;
        insert_delivery(
            &mut transaction,
            message.message_id,
            request.requester_repository_id,
            &now,
        )
        .await?;
        let (owner_column, owner_id) = actor_owner(&actor);
        let answer_provenance = if actor.worker_run_id.is_some() {
            "butler"
        } else {
            "orchestrator"
        };
        let query = format!(
            "UPDATE requests
             SET status = 'answered', answer_message_id = ?, claim_expires_at = NULL,
                 answer_provenance = ?, answered_by_instance_id = ?,
                 answered_by_worker_run_id = ?, updated_at = ?, finished_at = ?
             WHERE request_id = ? AND target_repository_id = ? AND status = 'claimed'
               AND {owner_column} = ? AND claim_expires_at > ?"
        );
        let result = sqlx::query(&query)
            .bind(message.message_id.to_string())
            .bind(answer_provenance)
            .bind(actor.instance_id.map(|id| id.to_string()))
            .bind(actor.worker_run_id.map(|id| id.to_string()))
            .bind(&now)
            .bind(&now)
            .bind(command.request_id.to_string())
            .bind(actor.repository_id.to_string())
            .bind(owner_id)
            .bind(&now)
            .execute(&mut *transaction)
            .await?;
        if result.rows_affected() != 1 {
            return Err(request_transition_conflict(command.request_id, "answer"));
        }
        append_event(
            &mut transaction,
            actor.project_id,
            request.requester_repository_id,
            EventKind::MessageAvailable,
            EventEntityType::Message,
            &message.message_id.to_string(),
            &now,
        )
        .await?;
        append_event(
            &mut transaction,
            actor.project_id,
            request.requester_repository_id,
            EventKind::RequestAnswered,
            EventEntityType::Request,
            &command.request_id.to_string(),
            &now,
        )
        .await?;
        transaction.commit().await?;
        self.notifications.notify(request.requester_repository_id);
        Ok(AnswerRequestResult {
            request: self.get_request(command.request_id).await?,
            message,
            artifact,
        })
    }

    pub async fn fail_request(
        &self,
        command: FailRequest,
    ) -> Result<CoordinationRequest, StoreError> {
        let _write_guard = self.write_lock.lock().await;
        let now = utc_now();
        let mut transaction = self.pool.begin().await?;
        let actor = self
            .resolve_actor(&mut transaction, command.actor, &now)
            .await?;
        let request_row = sqlx::query("SELECT * FROM requests WHERE request_id = ?")
            .bind(command.request_id.to_string())
            .fetch_optional(&mut *transaction)
            .await?
            .ok_or_else(|| StoreError::NotFound(format!("request {}", command.request_id)))?;
        let request = request_from_row(&request_row)?;
        ensure_active_request_claim(&request, &actor, &now)?;
        let next_status = if command.retryable {
            "pending"
        } else {
            "failed"
        };
        let finished_at = (!command.retryable).then_some(now.as_str());
        let (owner_column, owner_id) = actor_owner(&actor);
        let query = format!(
            "UPDATE requests
             SET status = ?, claimed_by_instance_id = NULL, claimed_by_worker_run_id = NULL,
                 claim_expires_at = NULL, failure_summary = ?, failure_retryable = ?,
                 updated_at = ?, finished_at = ?
             WHERE request_id = ? AND target_repository_id = ? AND status = 'claimed'
               AND {owner_column} = ? AND claim_expires_at > ?"
        );
        let result = sqlx::query(&query)
            .bind(next_status)
            .bind(command.summary)
            .bind(command.retryable)
            .bind(&now)
            .bind(finished_at)
            .bind(command.request_id.to_string())
            .bind(actor.repository_id.to_string())
            .bind(owner_id)
            .bind(&now)
            .execute(&mut *transaction)
            .await?;
        if result.rows_affected() != 1 {
            return Err(request_transition_conflict(command.request_id, "fail"));
        }
        let (recipient, event_kind) = if command.retryable {
            (actor.repository_id, EventKind::RequestAvailable)
        } else {
            (request.requester_repository_id, EventKind::RequestFailed)
        };
        append_event(
            &mut transaction,
            actor.project_id,
            recipient,
            event_kind,
            EventEntityType::Request,
            &command.request_id.to_string(),
            &now,
        )
        .await?;
        transaction.commit().await?;
        self.notifications.notify(recipient);
        self.get_request(command.request_id).await
    }
}

async fn ensure_request_handler(
    transaction: &mut Transaction<'_, Sqlite>,
    request: &CoordinationRequest,
    actor: &ResolvedActor,
) -> Result<(), StoreError> {
    match (actor.instance_id, actor.worker_run_id) {
        (Some(_), None)
            if request.handler_requirement == HandlerRequirement::OrchestratorAllowed =>
        {
            Ok(())
        }
        (Some(_), None) => Err(StoreError::Conflict(
            "request requires its linked butler worker".to_owned(),
        )),
        (None, Some(worker_run_id)) => {
            let valid: i64 = sqlx::query_scalar(
                "SELECT EXISTS(
                     SELECT 1 FROM worker_runs
                     WHERE worker_run_id = ? AND repository_id = ?
                       AND role = 'butler' AND request_id = ? AND status = 'active'
                 )",
            )
            .bind(worker_run_id.to_string())
            .bind(actor.repository_id.to_string())
            .bind(request.request_id.to_string())
            .fetch_one(&mut **transaction)
            .await?;
            if valid == 0 {
                return Err(StoreError::Conflict(
                    "request workers must be the active butler linked to this request".to_owned(),
                ));
            }
            Ok(())
        }
        _ => Err(StoreError::Conflict("invalid request actor".to_owned())),
    }
}

async fn resolve_request_scope(
    transaction: &mut Transaction<'_, Sqlite>,
    project_id: ProjectId,
    requested_feature_id: Option<FeatureId>,
    work_item_id: Option<WorkItemId>,
) -> Result<(Option<FeatureId>, Option<WorkItemId>), StoreError> {
    let feature_id = if let Some(work_item_id) = work_item_id {
        let row = sqlx::query(
            "SELECT wi.feature_id, f.project_id, f.status
             FROM work_items wi
             JOIN features f ON f.feature_id = wi.feature_id
             WHERE wi.work_item_id = ?",
        )
        .bind(work_item_id.to_string())
        .fetch_optional(&mut **transaction)
        .await?
        .ok_or_else(|| StoreError::NotFound(format!("work item {work_item_id}")))?;
        let actual_project_id = ProjectId::parse(row.try_get("project_id")?)?;
        if actual_project_id != project_id {
            return Err(StoreError::NotFound(format!(
                "work item {work_item_id} in actor project"
            )));
        }
        let work_feature_id = FeatureId::parse(row.try_get("feature_id")?)?;
        if requested_feature_id.is_some_and(|feature_id| feature_id != work_feature_id) {
            return Err(StoreError::Conflict(format!(
                "work item {work_item_id} does not belong to the requested feature"
            )));
        }
        let status: String = row.try_get("status")?;
        if status != FeatureStatus::Active.as_str() {
            return Err(StoreError::Conflict(format!(
                "feature {work_feature_id} is '{status}', expected 'active'"
            )));
        }
        Some(work_feature_id)
    } else if let Some(feature_id) = requested_feature_id {
        let row = sqlx::query("SELECT project_id, status FROM features WHERE feature_id = ?")
            .bind(feature_id.to_string())
            .fetch_optional(&mut **transaction)
            .await?
            .ok_or_else(|| StoreError::NotFound(format!("feature {feature_id}")))?;
        let actual_project_id = ProjectId::parse(row.try_get("project_id")?)?;
        if actual_project_id != project_id {
            return Err(StoreError::NotFound(format!(
                "feature {feature_id} in actor project"
            )));
        }
        let status: String = row.try_get("status")?;
        if status != FeatureStatus::Active.as_str() {
            return Err(StoreError::Conflict(format!(
                "feature {feature_id} is '{status}', expected 'active'"
            )));
        }
        Some(feature_id)
    } else {
        None
    };
    Ok((feature_id, work_item_id))
}
