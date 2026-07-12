use super::*;

impl SqliteStore {
    #[cfg(test)]
    pub async fn publish_message(
        &self,
        command: PublishMessage,
    ) -> Result<PublishResult, StoreError> {
        self.publish_message_idempotent(command, None).await
    }

    pub async fn publish_message_idempotent(
        &self,
        command: PublishMessage,
        idempotency_key: Option<&str>,
    ) -> Result<PublishResult, StoreError> {
        let _write_guard = self.write_lock.lock().await;
        let request_fingerprint = publish_message_fingerprint(&command)?;
        let now = utc_now();
        let mut transaction = self.pool.begin().await?;
        let actor = self
            .resolve_actor(&mut transaction, command.actor, &now)
            .await?;
        let actor_key = resolved_actor_key(&actor);
        if let Some(existing_id) = find_idempotent_result(
            &mut transaction,
            &actor_key,
            "publish_message",
            idempotency_key,
            &request_fingerprint,
        )
        .await?
        {
            transaction.commit().await?;
            return self
                .get_publish_result(MessageId::parse(&existing_id)?)
                .await;
        }

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
        let work_event_recipients = if let Some(artifact) = &artifact {
            super::work::link_audit_report(&mut transaction, &actor, artifact, &now).await?
        } else {
            Vec::new()
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
        record_idempotent_result(
            &mut transaction,
            &actor_key,
            "publish_message",
            idempotency_key,
            &request_fingerprint,
            &message.message_id.to_string(),
            &now,
        )
        .await?;

        transaction.commit().await?;
        for recipient_id in &recipient_ids {
            self.notifications.notify(*recipient_id);
        }
        for recipient_id in work_event_recipients {
            self.notifications.notify(recipient_id);
        }
        Ok(PublishResult {
            message,
            artifact,
            recipient_count: recipient_ids.len(),
        })
    }

    async fn get_publish_result(&self, message_id: MessageId) -> Result<PublishResult, StoreError> {
        let row = sqlx::query("SELECT * FROM messages WHERE message_id = ?")
            .bind(message_id.to_string())
            .fetch_optional(&self.pool)
            .await?
            .ok_or_else(|| StoreError::NotFound(format!("message {message_id}")))?;
        let message = message_from_row(&row)?;
        let artifact = if let Some(artifact_id) = message.artifact_id {
            let row = sqlx::query("SELECT * FROM artifacts WHERE artifact_id = ?")
                .bind(artifact_id.to_string())
                .fetch_one(&self.pool)
                .await?;
            Some(artifact_from_row(&row)?)
        } else {
            None
        };
        let recipient_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM deliveries WHERE message_id = ?")
                .bind(message_id.to_string())
                .fetch_one(&self.pool)
                .await?;
        Ok(PublishResult {
            message,
            artifact,
            recipient_count: recipient_count as usize,
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
                ) OR EXISTS (
                    SELECT 1 FROM requests q
                    WHERE q.question_artifact_id = a.artifact_id
                      AND (q.requester_repository_id = ? OR q.target_repository_id = ?)
                ))",
        )
        .bind(artifact_id.to_string())
        .bind(actor.repository_id.to_string())
        .bind(actor.repository_id.to_string())
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
}
