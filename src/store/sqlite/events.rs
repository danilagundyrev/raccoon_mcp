use super::*;

impl SqliteStore {
    pub async fn wait_for_events(
        &self,
        actor_ref: ActorRef,
        cursor: i64,
        kinds: &[EventKind],
        timeout: Duration,
        limit: u8,
    ) -> Result<EventBatch, StoreError> {
        if cursor < 0 {
            return Err(StoreError::Conflict(
                "event cursor must be non-negative".to_owned(),
            ));
        }
        if !(1..=50).contains(&limit) {
            return Err(StoreError::Conflict(
                "event limit must be between 1 and 50".to_owned(),
            ));
        }

        let actor = self.touch_actor(actor_ref).await?;
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
        let deadline = Instant::now() + timeout;

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
                    return Ok(EventBatch {
                        outcome: WaitOutcome::Deadline,
                        events: Vec::new(),
                        next_cursor: cursor,
                        has_more: false,
                    });
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
