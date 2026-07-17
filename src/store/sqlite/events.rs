use super::*;

impl SqliteStore {
    #[cfg(test)]
    pub async fn wait_for_events(
        &self,
        actor_ref: ActorRef,
        cursor: i64,
        kinds: &[EventKind],
        timeout: Duration,
        limit: u8,
    ) -> Result<EventBatch, StoreError> {
        self.wait_for_events_from(actor_ref, Some(cursor), None, kinds, timeout, limit)
            .await
    }

    pub async fn wait_for_events_from(
        &self,
        actor_ref: ActorRef,
        cursor: Option<i64>,
        consumer_key: Option<&str>,
        kinds: &[EventKind],
        timeout: Duration,
        limit: u8,
    ) -> Result<EventBatch, StoreError> {
        let deadline = Instant::now() + timeout;
        if cursor.is_some_and(|cursor| cursor < 0) {
            return Err(StoreError::Conflict(
                "event cursor must be non-negative".to_owned(),
            ));
        }
        if !(1..=50).contains(&limit) {
            return Err(StoreError::Conflict(
                "event limit must be between 1 and 50".to_owned(),
            ));
        }
        let consumer_key = consumer_key
            .map(|key| crate::domain::validate_key("consumer_key", key))
            .transpose()
            .map_err(StoreError::Conflict)?;
        let fallback_cursor = cursor.unwrap_or(0);

        if timeout.is_zero() {
            let (actor, resolved_cursor) = self
                .prepare_event_wait(actor_ref, cursor, consumer_key.as_deref())
                .await?;
            return self
                .wait_for_events_until(actor_ref, actor, resolved_cursor, kinds, limit, deadline)
                .await;
        }
        let prepared = match timeout_at(
            deadline,
            self.prepare_event_wait(actor_ref, cursor, consumer_key.as_deref()),
        )
        .await
        {
            Ok(result) => result?,
            Err(_) => return Ok(deadline_event_batch(fallback_cursor)),
        };
        let (actor, resolved_cursor) = prepared;
        match timeout_at(
            deadline,
            self.wait_for_events_until(actor_ref, actor, resolved_cursor, kinds, limit, deadline),
        )
        .await
        {
            Ok(result) => result,
            Err(_) => Ok(deadline_event_batch(resolved_cursor)),
        }
    }

    pub async fn get_event_checkpoint(
        &self,
        actor_ref: ActorRef,
        consumer_key: &str,
    ) -> Result<EventCheckpoint, StoreError> {
        let consumer_key = crate::domain::validate_key("consumer_key", consumer_key)
            .map_err(StoreError::Conflict)?;
        let actor = self.resolve_actor_readonly(actor_ref, &utc_now()).await?;
        let row = sqlx::query(
            "SELECT cursor, created_at, updated_at FROM event_consumers
             WHERE repository_id = ? AND consumer_key = ?",
        )
        .bind(actor.repository_id.to_string())
        .bind(&consumer_key)
        .fetch_optional(&self.pool)
        .await?;
        Ok(EventCheckpoint {
            repository_id: actor.repository_id,
            consumer_key,
            cursor: row.as_ref().map_or(Ok(0), |row| row.try_get("cursor"))?,
            created_at: row
                .as_ref()
                .map(|row| row.try_get("created_at"))
                .transpose()?,
            updated_at: row
                .as_ref()
                .map(|row| row.try_get("updated_at"))
                .transpose()?,
        })
    }

    pub async fn commit_event_cursor(
        &self,
        actor_ref: ActorRef,
        consumer_key: &str,
        cursor: i64,
    ) -> Result<EventCheckpoint, StoreError> {
        if cursor < 0 {
            return Err(StoreError::Conflict(
                "event cursor must be non-negative".to_owned(),
            ));
        }
        let consumer_key = crate::domain::validate_key("consumer_key", consumer_key)
            .map_err(StoreError::Conflict)?;
        let _write_guard = self.write_lock.lock().await;
        let now = utc_now();
        let mut transaction = self.pool.begin().await?;
        let actor = self
            .resolve_actor(&mut transaction, actor_ref, &now)
            .await?;
        let high_water: i64 = sqlx::query_scalar(
            "SELECT COALESCE((SELECT seq FROM sqlite_sequence WHERE name = 'events'), 0)",
        )
        .fetch_one(&mut *transaction)
        .await?;
        if cursor > high_water {
            return Err(StoreError::Conflict(format!(
                "event cursor {cursor} is beyond current high-water mark {high_water}"
            )));
        }
        let existing = sqlx::query(
            "SELECT cursor, created_at, updated_at FROM event_consumers
             WHERE repository_id = ? AND consumer_key = ?",
        )
        .bind(actor.repository_id.to_string())
        .bind(&consumer_key)
        .fetch_optional(&mut *transaction)
        .await?;
        let (created_at, updated_at) = if let Some(existing) = existing {
            let current: i64 = existing.try_get("cursor")?;
            if cursor < current {
                return Err(StoreError::Conflict(format!(
                    "event checkpoint cannot move backwards from {current} to {cursor}"
                )));
            }
            let created_at = existing.try_get("created_at")?;
            if cursor == current {
                (created_at, existing.try_get("updated_at")?)
            } else {
                sqlx::query(
                    "UPDATE event_consumers SET cursor = ?, updated_at = ?
                     WHERE repository_id = ? AND consumer_key = ?",
                )
                .bind(cursor)
                .bind(&now)
                .bind(actor.repository_id.to_string())
                .bind(&consumer_key)
                .execute(&mut *transaction)
                .await?;
                (created_at, now.clone())
            }
        } else {
            sqlx::query(
                "INSERT INTO event_consumers(repository_id, consumer_key, cursor, created_at, updated_at)
                 VALUES (?, ?, ?, ?, ?)",
            )
            .bind(actor.repository_id.to_string())
            .bind(&consumer_key)
            .bind(cursor)
            .bind(&now)
            .bind(&now)
            .execute(&mut *transaction)
            .await?;
            (now.clone(), now.clone())
        };
        transaction.commit().await?;
        Ok(EventCheckpoint {
            repository_id: actor.repository_id,
            consumer_key,
            cursor,
            created_at: Some(created_at),
            updated_at: Some(updated_at),
        })
    }

    async fn prepare_event_wait(
        &self,
        actor_ref: ActorRef,
        cursor: Option<i64>,
        consumer_key: Option<&str>,
    ) -> Result<(ResolvedActor, i64), StoreError> {
        let _write_guard = self.write_lock.lock().await;
        let mut transaction = self.pool.begin().await?;
        let actor = self
            .resolve_actor(&mut transaction, actor_ref, &utc_now())
            .await?;
        let resolved_cursor = match (cursor, consumer_key) {
            (Some(cursor), _) => cursor,
            (None, Some(consumer_key)) => sqlx::query_scalar(
                "SELECT cursor FROM event_consumers
                     WHERE repository_id = ? AND consumer_key = ?",
            )
            .bind(actor.repository_id.to_string())
            .bind(consumer_key)
            .fetch_optional(&mut *transaction)
            .await?
            .unwrap_or(0),
            (None, None) => 0,
        };
        transaction.commit().await?;
        Ok((actor, resolved_cursor))
    }

    async fn wait_for_events_until(
        &self,
        actor_ref: ActorRef,
        actor: ResolvedActor,
        cursor: i64,
        kinds: &[EventKind],
        limit: u8,
        deadline: Instant,
    ) -> Result<EventBatch, StoreError> {
        let repository_id = actor.repository_id;
        let notifier = self.notifications.for_repository(repository_id);
        let lease_seconds = match actor_ref {
            ActorRef::Orchestrator(_) => self.lease_duration.num_seconds(),
            ActorRef::Worker(_) => self.worker_lease_duration.num_seconds(),
        };
        let claim_seconds = self.request_claim_lease_duration.num_seconds();
        let renewal_seconds = match actor_ref {
            ActorRef::Orchestrator(_) => {
                claim_seconds.min(self.work_item_claim_lease_duration.num_seconds())
            }
            ActorRef::Worker(_) => claim_seconds,
        };
        let heartbeat_every =
            Duration::from_secs((lease_seconds.min(renewal_seconds) / 3).max(1) as u64);
        let mut heartbeat = interval_at(Instant::now() + heartbeat_every, heartbeat_every);
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
                    return Ok(deadline_event_batch(cursor));
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
}

fn deadline_event_batch(cursor: i64) -> EventBatch {
    EventBatch {
        outcome: WaitOutcome::Deadline,
        events: Vec::new(),
        next_cursor: cursor,
        has_more: false,
    }
}
