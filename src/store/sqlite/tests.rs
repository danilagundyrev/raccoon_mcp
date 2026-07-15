use std::{fs, path::PathBuf};

use serde_json::json;
use uuid::Uuid;

use super::*;
use crate::domain::{
    AnswerRequest, AskRepository, AuditWorkItemInput, FailRequest, RequestStatus, WorkItemId,
    WorkerRole,
};

fn temp_database_path() -> PathBuf {
    std::env::temp_dir()
        .join("agent-sync-mcp-tests")
        .join(format!("{}.sqlite3", Uuid::new_v4()))
}

async fn test_store(lease_seconds: u16) -> (SqliteStore, PathBuf) {
    let path = temp_database_path();
    let store = SqliteStore::open(&path, 2, lease_seconds, 3600, 600, 3600)
        .await
        .unwrap();
    (store, path)
}

fn connect_command(project: &str, repository: &str, path: &str) -> ConnectOrchestrator {
    ConnectOrchestrator {
        project_key: project.to_owned(),
        project_display_name: None,
        repository_key: repository.to_owned(),
        repository_display_name: None,
        canonical_path: path.to_owned(),
        instance_id: None,
        capabilities: json!({ "spawning": true }),
    }
}

fn publish_command(
    actor: ActorRef,
    target: MessageTarget,
    summary: &str,
    body: Option<&str>,
) -> PublishMessage {
    PublishMessage {
        actor,
        kind: crate::domain::MessageKind::Report,
        target,
        summary: summary.to_owned(),
        body: body.map(str::to_owned),
        artifact_kind: crate::domain::ArtifactKind::ImplementationReport,
        artifact_title: None,
        content_type: "text/markdown".to_owned(),
        feature_id: None,
        work_item_id: None,
        request_id: None,
        thread_id: None,
        correlation_id: None,
        in_reply_to: None,
    }
}

fn remove_database(path: &Path) {
    let _ = fs::remove_file(path);
    let _ = fs::remove_file(path.with_extension("sqlite3-shm"));
    let _ = fs::remove_file(path.with_extension("sqlite3-wal"));
    if let Some(parent) = path.parent() {
        let _ = fs::remove_dir(parent);
    }
}

#[test]
fn work_item_transition_matrix_is_exhaustive() {
    let statuses = [
        WorkItemStatus::Planned,
        WorkItemStatus::Ready,
        WorkItemStatus::Claimed,
        WorkItemStatus::InProgress,
        WorkItemStatus::Blocked,
        WorkItemStatus::Completed,
        WorkItemStatus::Failed,
        WorkItemStatus::Cancelled,
    ];
    for from in statuses {
        for to in statuses {
            let expected = from == to
                || matches!(
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
                            WorkItemStatus::InProgress
                                | WorkItemStatus::Failed
                                | WorkItemStatus::Cancelled
                        )
                        | (
                            WorkItemStatus::Failed,
                            WorkItemStatus::Ready | WorkItemStatus::Cancelled
                        )
                );
            assert_eq!(
                super::work::validate_status_transition(from, to).is_ok(),
                expected,
                "unexpected transition result for {} -> {}",
                from.as_str(),
                to.as_str()
            );
        }
    }
}

#[tokio::test]
async fn connects_multiple_repositories_to_one_project() {
    let (store, path) = test_store(90).await;
    let backend = store
        .connect_orchestrator(connect_command("project", "backend", "C:/project/backend"))
        .await
        .unwrap();
    let web = store
        .connect_orchestrator(connect_command("project", "web", "C:/project/web"))
        .await
        .unwrap();

    assert_eq!(backend.project.project_id, web.project.project_id);
    assert_ne!(
        backend.repository.repository_id,
        web.repository.repository_id
    );
    assert_eq!(web.active_feature_count, 0);
    assert_eq!(web.pending_inbox_count, 0);
    assert_eq!(web.open_request_count, 0);
    let overview = store
        .project_overview(backend.project.project_id)
        .await
        .unwrap();
    assert_eq!(overview.repositories.len(), 2);
    assert!(
        overview
            .repositories
            .iter()
            .all(|repo| repo.online_instances == 1)
    );

    store.pool.close().await;
    remove_database(&path);
}

#[tokio::test]
async fn disconnect_is_visible_in_presence() {
    let (store, path) = test_store(90).await;
    let connected = store
        .connect_orchestrator(connect_command("project", "backend", "C:/project/backend"))
        .await
        .unwrap();
    let disconnected = store
        .disconnect_orchestrator(connected.instance.instance_id)
        .await
        .unwrap();

    assert_eq!(disconnected.status, PresenceStatus::Disconnected);
    let repositories = store
        .list_repositories(connected.project.project_id, RepositoryPresenceFilter::All)
        .await
        .unwrap();
    assert_eq!(repositories[0].online_instances, 0);
    assert!(
        store
            .list_repositories(
                connected.project.project_id,
                RepositoryPresenceFilter::Online,
            )
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        store
            .list_repositories(
                connected.project.project_id,
                RepositoryPresenceFilter::Offline,
            )
            .await
            .unwrap()
            .len(),
        1
    );

    store.pool.close().await;
    remove_database(&path);
}

#[tokio::test]
async fn rejects_instance_reuse_across_repositories() {
    let (store, path) = test_store(90).await;
    let connected = store
        .connect_orchestrator(connect_command("project", "backend", "C:/project/backend"))
        .await
        .unwrap();
    let mut command = connect_command("project", "web", "C:/project/web");
    command.instance_id = Some(connected.instance.instance_id);

    let error = store.connect_orchestrator(command).await.unwrap_err();
    assert!(matches!(error, StoreError::Conflict(_)));

    store.pool.close().await;
    remove_database(&path);
}

#[tokio::test]
async fn worker_run_completes_full_lifecycle_in_parent_repository() {
    let (store, path) = test_store(90).await;
    let connected = store
        .connect_orchestrator(connect_command("project", "backend", "C:/project/backend"))
        .await
        .unwrap();
    let created = store
        .create_worker_run(CreateWorkerRun {
            parent_instance_id: connected.instance.instance_id,
            role: WorkerRole::Executor,
            task_summary: "Implement a fake endpoint".to_owned(),
            context_refs: json!([{ "kind": "message", "id": "message-1" }]),
            audit_work_items: Vec::new(),
            addresses_audit_run_id: None,
            work_item_id: None,
        })
        .await
        .unwrap();

    assert_eq!(created.repository_id, connected.repository.repository_id);
    assert_eq!(created.status, WorkerRunStatus::Created);
    let active = store
        .activate_worker_run(created.worker_run_id)
        .await
        .unwrap();
    assert_eq!(active.status, WorkerRunStatus::Active);
    assert!(active.activated_at.is_some());
    let completed = store
        .complete_worker_run(active.worker_run_id, "Fake endpoint implemented")
        .await
        .unwrap();
    assert_eq!(completed.status, WorkerRunStatus::Completed);
    assert_eq!(
        completed.result_summary.as_deref(),
        Some("Fake endpoint implemented")
    );
    assert!(completed.finished_at.is_some());
    let lifecycle_events = store
        .wait_for_events(
            ActorRef::Orchestrator(connected.instance.instance_id),
            0,
            &[
                EventKind::WorkerRunCreated,
                EventKind::WorkerRunActive,
                EventKind::WorkerRunCompleted,
            ],
            Some(Duration::ZERO),
            20,
        )
        .await
        .unwrap();
    assert_eq!(
        lifecycle_events
            .events
            .iter()
            .map(|event| event.kind)
            .collect::<Vec<_>>(),
        vec![
            EventKind::WorkerRunCreated,
            EventKind::WorkerRunActive,
            EventKind::WorkerRunCompleted,
        ]
    );

    store.pool.close().await;
    remove_database(&path);
}

#[tokio::test]
async fn completed_worker_run_rejects_reactivation() {
    let (store, path) = test_store(90).await;
    let connected = store
        .connect_orchestrator(connect_command("project", "backend", "C:/project/backend"))
        .await
        .unwrap();
    let created = store
        .create_worker_run(CreateWorkerRun {
            parent_instance_id: connected.instance.instance_id,
            role: WorkerRole::Butler,
            task_summary: "Inspect a fake contract".to_owned(),
            context_refs: json!([]),
            audit_work_items: Vec::new(),
            addresses_audit_run_id: None,
            work_item_id: None,
        })
        .await
        .unwrap();
    store
        .activate_worker_run(created.worker_run_id)
        .await
        .unwrap();
    store
        .complete_worker_run(created.worker_run_id, "Contract summarized")
        .await
        .unwrap();

    let error = store
        .activate_worker_run(created.worker_run_id)
        .await
        .unwrap_err();
    assert!(matches!(error, StoreError::Conflict(_)));

    store.pool.close().await;
    remove_database(&path);
}

#[tokio::test]
async fn concurrent_finish_transitions_cannot_overwrite_each_other() {
    let (store, path) = test_store(90).await;
    let connected = store
        .connect_orchestrator(connect_command("project", "backend", "C:/project/backend"))
        .await
        .unwrap();
    let created = store
        .create_worker_run(CreateWorkerRun {
            parent_instance_id: connected.instance.instance_id,
            role: WorkerRole::Executor,
            task_summary: "Race two terminal transitions".to_owned(),
            context_refs: json!([]),
            audit_work_items: Vec::new(),
            addresses_audit_run_id: None,
            work_item_id: None,
        })
        .await
        .unwrap();
    store
        .activate_worker_run(created.worker_run_id)
        .await
        .unwrap();

    let (completed, failed) = tokio::join!(
        store.complete_worker_run(created.worker_run_id, "completed"),
        store.fail_worker_run(created.worker_run_id, "failed")
    );
    assert_ne!(completed.is_ok(), failed.is_ok());
    let final_run = store.get_worker_run(created.worker_run_id).await.unwrap();
    assert!(matches!(
        final_run.status,
        WorkerRunStatus::Completed | WorkerRunStatus::Failed
    ));

    store.pool.close().await;
    remove_database(&path);
}

#[tokio::test]
async fn expired_worker_run_cannot_complete_or_fail() {
    let (store, path) = test_store(90).await;
    let connected = store
        .connect_orchestrator(connect_command(
            "expired-finish",
            "backend",
            "C:/expired/backend",
        ))
        .await
        .unwrap();
    let worker = store
        .create_worker_run(CreateWorkerRun {
            parent_instance_id: connected.instance.instance_id,
            role: WorkerRole::Butler,
            task_summary: "Expire before finishing".to_owned(),
            context_refs: json!([]),
            audit_work_items: Vec::new(),
            addresses_audit_run_id: None,
            work_item_id: None,
        })
        .await
        .unwrap();
    store
        .activate_worker_run(worker.worker_run_id)
        .await
        .unwrap();
    sqlx::query("UPDATE worker_runs SET lease_expires_at = ? WHERE worker_run_id = ?")
        .bind("2000-01-01T00:00:00.000000Z")
        .bind(worker.worker_run_id.to_string())
        .execute(&store.pool)
        .await
        .unwrap();

    assert!(matches!(
        store
            .complete_worker_run(worker.worker_run_id, "too late")
            .await,
        Err(StoreError::Conflict(_))
    ));
    assert!(matches!(
        store
            .fail_worker_run(worker.worker_run_id, "too late")
            .await,
        Err(StoreError::Conflict(_))
    ));
    store.run_lease_maintenance().await.unwrap();
    assert_eq!(
        store
            .get_worker_run(worker.worker_run_id)
            .await
            .unwrap()
            .status,
        WorkerRunStatus::Abandoned
    );

    store.pool.close().await;
    remove_database(&path);
}

#[tokio::test]
async fn direct_message_exposes_header_then_artifact_and_ack_is_idempotent() {
    let (store, path) = test_store(90).await;
    let backend = store
        .connect_orchestrator(connect_command("project", "backend", "C:/project/backend"))
        .await
        .unwrap();
    let web = store
        .connect_orchestrator(connect_command("project", "web", "C:/project/web"))
        .await
        .unwrap();
    let android = store
        .connect_orchestrator(connect_command("project", "android", "C:/project/android"))
        .await
        .unwrap();

    let published = store
        .publish_message(publish_command(
            ActorRef::Orchestrator(backend.instance.instance_id),
            MessageTarget::Repositories(vec![web.repository.repository_id]),
            "Backend contract is ready",
            Some("# Full contract\n\nGET /example"),
        ))
        .await
        .unwrap();
    assert_eq!(published.recipient_count, 1);
    let web_snapshot = store
        .connect_orchestrator(connect_command("project", "web", "C:/project/web"))
        .await
        .unwrap();
    assert_eq!(web_snapshot.pending_inbox_count, 1);
    let web_inbox = store
        .list_inbox(
            ActorRef::Orchestrator(web.instance.instance_id),
            None,
            &[],
            20,
        )
        .await
        .unwrap();
    assert_eq!(web_inbox.items.len(), 1);
    assert_eq!(
        web_inbox.items[0].message.summary,
        "Backend contract is ready"
    );
    assert!(
        store
            .list_inbox(
                ActorRef::Orchestrator(web.instance.instance_id),
                None,
                &[MessageKind::Announcement],
                20,
            )
            .await
            .unwrap()
            .items
            .is_empty()
    );
    assert_eq!(
        store
            .list_inbox(
                ActorRef::Orchestrator(web.instance.instance_id),
                None,
                &[MessageKind::Report],
                20,
            )
            .await
            .unwrap()
            .items
            .len(),
        1
    );
    assert!(
        store
            .list_inbox(
                ActorRef::Orchestrator(android.instance.instance_id),
                None,
                &[],
                20
            )
            .await
            .unwrap()
            .items
            .is_empty()
    );
    let artifact = store
        .read_artifact(
            ActorRef::Orchestrator(web.instance.instance_id),
            published.artifact.unwrap().artifact_id,
        )
        .await
        .unwrap();
    assert_eq!(artifact.content, "# Full contract\n\nGET /example");
    assert!(matches!(
        store
            .read_artifact(
                ActorRef::Orchestrator(android.instance.instance_id),
                artifact.artifact_id,
            )
            .await,
        Err(StoreError::NotFound(_))
    ));

    let first_ack = store
        .ack_message(
            ActorRef::Orchestrator(web.instance.instance_id),
            published.message.message_id,
        )
        .await
        .unwrap();
    let second_ack = store
        .ack_message(
            ActorRef::Orchestrator(web.instance.instance_id),
            published.message.message_id,
        )
        .await
        .unwrap();
    assert_eq!(first_ack.delivery_id, second_ack.delivery_id);
    assert!(
        store
            .list_inbox(
                ActorRef::Orchestrator(web.instance.instance_id),
                None,
                &[],
                20
            )
            .await
            .unwrap()
            .items
            .is_empty()
    );

    store.pool.close().await;
    remove_database(&path);
}

#[tokio::test]
async fn broadcast_deliveries_are_acknowledged_independently() {
    let (store, path) = test_store(90).await;
    let backend = store
        .connect_orchestrator(connect_command("project", "backend", "C:/project/backend"))
        .await
        .unwrap();
    let web = store
        .connect_orchestrator(connect_command("project", "web", "C:/project/web"))
        .await
        .unwrap();
    let published = store
        .publish_message(publish_command(
            ActorRef::Orchestrator(backend.instance.instance_id),
            MessageTarget::Broadcast,
            "Project announcement",
            None,
        ))
        .await
        .unwrap();
    assert_eq!(published.recipient_count, 2);

    store
        .ack_message(
            ActorRef::Orchestrator(web.instance.instance_id),
            published.message.message_id,
        )
        .await
        .unwrap();
    assert!(
        store
            .list_inbox(
                ActorRef::Orchestrator(web.instance.instance_id),
                None,
                &[],
                20
            )
            .await
            .unwrap()
            .items
            .is_empty()
    );
    assert_eq!(
        store
            .list_inbox(
                ActorRef::Orchestrator(backend.instance.instance_id),
                None,
                &[],
                20
            )
            .await
            .unwrap()
            .items
            .len(),
        1
    );

    store.pool.close().await;
    remove_database(&path);
}

#[tokio::test]
async fn active_worker_can_publish_without_creating_repository() {
    let (store, path) = test_store(90).await;
    let backend = store
        .connect_orchestrator(connect_command("project", "backend", "C:/project/backend"))
        .await
        .unwrap();
    let web = store
        .connect_orchestrator(connect_command("project", "web", "C:/project/web"))
        .await
        .unwrap();
    let worker = store
        .create_worker_run(CreateWorkerRun {
            parent_instance_id: backend.instance.instance_id,
            role: WorkerRole::Executor,
            task_summary: "Publish fake report".to_owned(),
            context_refs: json!([]),
            audit_work_items: Vec::new(),
            addresses_audit_run_id: None,
            work_item_id: None,
        })
        .await
        .unwrap();
    store
        .activate_worker_run(worker.worker_run_id)
        .await
        .unwrap();

    let published = store
        .publish_message(publish_command(
            ActorRef::Worker(worker.worker_run_id),
            MessageTarget::Repositories(vec![web.repository.repository_id]),
            "Worker report",
            Some("Done"),
        ))
        .await
        .unwrap();
    assert_eq!(
        published.message.sender_worker_run_id,
        Some(worker.worker_run_id)
    );
    assert_eq!(
        store
            .project_overview(backend.project.project_id)
            .await
            .unwrap()
            .repositories
            .len(),
        2
    );

    store.pool.close().await;
    remove_database(&path);
}

#[tokio::test]
async fn direct_message_cannot_cross_project_boundary() {
    let (store, path) = test_store(90).await;
    let first = store
        .connect_orchestrator(connect_command("first", "backend", "C:/first/backend"))
        .await
        .unwrap();
    let second = store
        .connect_orchestrator(connect_command("second", "web", "C:/second/web"))
        .await
        .unwrap();

    let error = store
        .publish_message(publish_command(
            ActorRef::Orchestrator(first.instance.instance_id),
            MessageTarget::Repositories(vec![second.repository.repository_id]),
            "Invalid cross-project message",
            None,
        ))
        .await
        .unwrap_err();
    assert!(matches!(error, StoreError::Conflict(_)));

    store.pool.close().await;
    remove_database(&path);
}

#[tokio::test]
async fn pending_inbox_survives_store_restart() {
    let (store, path) = test_store(90).await;
    let backend = store
        .connect_orchestrator(connect_command("project", "backend", "C:/project/backend"))
        .await
        .unwrap();
    let web = store
        .connect_orchestrator(connect_command("project", "web", "C:/project/web"))
        .await
        .unwrap();
    let published = store
        .publish_message(publish_command(
            ActorRef::Orchestrator(backend.instance.instance_id),
            MessageTarget::Repositories(vec![web.repository.repository_id]),
            "Persist across restart",
            Some("Persistent artifact"),
        ))
        .await
        .unwrap();
    store.pool.close().await;

    let reopened = SqliteStore::open(&path, 2, 90, 3600, 600, 3600)
        .await
        .unwrap();
    let inbox = reopened
        .list_inbox(
            ActorRef::Orchestrator(web.instance.instance_id),
            None,
            &[],
            20,
        )
        .await
        .unwrap();
    assert_eq!(
        inbox.items[0].message.message_id,
        published.message.message_id
    );
    assert_eq!(inbox.items[0].message.summary, "Persist across restart");

    reopened.pool.close().await;
    remove_database(&path);
}

#[tokio::test]
async fn concurrent_connects_to_new_project_do_not_lock_database() {
    let (store, path) = test_store(90).await;
    let mut tasks = tokio::task::JoinSet::new();
    for index in 0..20 {
        let store = store.clone();
        tasks.spawn(async move {
            store
                .connect_orchestrator(connect_command(
                    "parallel-project",
                    &format!("repository-{index}"),
                    &format!("C:/parallel/repository-{index}"),
                ))
                .await
        });
    }

    let mut connected = Vec::new();
    while let Some(result) = tasks.join_next().await {
        connected.push(result.unwrap().unwrap());
    }
    assert_eq!(connected.len(), 20);
    let project_id = connected[0].project.project_id;
    assert!(
        connected
            .iter()
            .all(|result| result.project.project_id == project_id)
    );
    assert_eq!(
        store
            .list_repositories(project_id, RepositoryPresenceFilter::All)
            .await
            .unwrap()
            .len(),
        20
    );

    store.pool.close().await;
    remove_database(&path);
}

#[tokio::test]
async fn expired_worker_cannot_renew_or_publish() {
    let (store, path) = test_store(90).await;
    let backend = store
        .connect_orchestrator(connect_command("project", "backend", "C:/project/backend"))
        .await
        .unwrap();
    let worker = store
        .create_worker_run(CreateWorkerRun {
            parent_instance_id: backend.instance.instance_id,
            role: WorkerRole::Executor,
            task_summary: "Expire this worker".to_owned(),
            context_refs: json!([]),
            audit_work_items: Vec::new(),
            addresses_audit_run_id: None,
            work_item_id: None,
        })
        .await
        .unwrap();
    store
        .activate_worker_run(worker.worker_run_id)
        .await
        .unwrap();
    sqlx::query("UPDATE worker_runs SET lease_expires_at = ? WHERE worker_run_id = ?")
        .bind("2000-01-01T00:00:00.000000Z")
        .bind(worker.worker_run_id.to_string())
        .execute(&store.pool)
        .await
        .unwrap();

    assert!(matches!(
        store.heartbeat_worker_run(worker.worker_run_id).await,
        Err(StoreError::Conflict(_))
    ));
    assert!(matches!(
        store
            .publish_message(publish_command(
                ActorRef::Worker(worker.worker_run_id),
                MessageTarget::Broadcast,
                "Expired worker must not publish",
                None,
            ))
            .await,
        Err(StoreError::Conflict(_))
    ));

    store.pool.close().await;
    remove_database(&path);
}

#[tokio::test]
async fn repository_cannot_reply_to_message_it_cannot_access() {
    let (store, path) = test_store(90).await;
    let backend = store
        .connect_orchestrator(connect_command("project", "backend", "C:/project/backend"))
        .await
        .unwrap();
    let web = store
        .connect_orchestrator(connect_command("project", "web", "C:/project/web"))
        .await
        .unwrap();
    let android = store
        .connect_orchestrator(connect_command("project", "android", "C:/project/android"))
        .await
        .unwrap();
    let mut original = publish_command(
        ActorRef::Orchestrator(backend.instance.instance_id),
        MessageTarget::Repositories(vec![web.repository.repository_id]),
        "Private backend-to-web message",
        None,
    );
    original.correlation_id = Some("private-correlation".to_owned());
    let original = store.publish_message(original).await.unwrap();

    let mut unauthorized_reply = publish_command(
        ActorRef::Orchestrator(android.instance.instance_id),
        MessageTarget::Repositories(vec![backend.repository.repository_id]),
        "Unauthorized reply",
        None,
    );
    unauthorized_reply.in_reply_to = Some(original.message.message_id);
    assert!(matches!(
        store.publish_message(unauthorized_reply).await,
        Err(StoreError::NotFound(_))
    ));

    let mut authorized_reply = publish_command(
        ActorRef::Orchestrator(web.instance.instance_id),
        MessageTarget::Repositories(vec![backend.repository.repository_id]),
        "Authorized reply",
        None,
    );
    authorized_reply.in_reply_to = Some(original.message.message_id);
    let reply = store.publish_message(authorized_reply).await.unwrap();
    assert_eq!(reply.message.thread_id, original.message.thread_id);
    assert_eq!(
        reply.message.correlation_id.as_deref(),
        Some("private-correlation")
    );

    store.pool.close().await;
    remove_database(&path);
}

#[tokio::test]
async fn inbox_and_content_reads_do_not_renew_actor_lease() {
    let (store, path) = test_store(90).await;
    let backend = store
        .connect_orchestrator(connect_command("project", "backend", "C:/project/backend"))
        .await
        .unwrap();
    let web = store
        .connect_orchestrator(connect_command("project", "web", "C:/project/web"))
        .await
        .unwrap();
    let published = store
        .publish_message(publish_command(
            ActorRef::Orchestrator(backend.instance.instance_id),
            MessageTarget::Repositories(vec![web.repository.repository_id]),
            "Read without heartbeat",
            Some("Read-only artifact"),
        ))
        .await
        .unwrap();
    let before = store.get_instance(web.instance.instance_id).await.unwrap();

    store
        .list_inbox(
            ActorRef::Orchestrator(web.instance.instance_id),
            None,
            &[],
            20,
        )
        .await
        .unwrap();
    store
        .read_message(
            ActorRef::Orchestrator(web.instance.instance_id),
            published.message.message_id,
        )
        .await
        .unwrap();
    store
        .read_artifact(
            ActorRef::Orchestrator(web.instance.instance_id),
            published.artifact.unwrap().artifact_id,
        )
        .await
        .unwrap();
    let after = store.get_instance(web.instance.instance_id).await.unwrap();
    assert_eq!(before.last_heartbeat_at, after.last_heartbeat_at);
    assert_eq!(before.lease_expires_at, after.lease_expires_at);

    store.pool.close().await;
    remove_database(&path);
}

#[tokio::test]
async fn auditor_batches_items_and_enforces_per_item_depth() {
    let (store, path) = test_store(90).await;
    let connected = store
        .connect_orchestrator(connect_command("project", "backend", "C:/project/backend"))
        .await
        .unwrap();
    let feature = store
        .create_feature(CreateFeature {
            parent_instance_id: connected.instance.instance_id,
            title: "Auditable feature".to_owned(),
            summary: "Test audit depth".to_owned(),
        })
        .await
        .unwrap();
    let first_item = store
        .create_work_item(CreateWorkItem {
            parent_instance_id: connected.instance.instance_id,
            feature_id: feature.feature_id,
            parent_work_item_id: None,
            owner_repository_id: connected.repository.repository_id,
            title: "First item".to_owned(),
            summary: "First".to_owned(),
            dependency_ids: Vec::new(),
            priority: 0,
            max_audit_iterations: 2,
        })
        .await
        .unwrap()
        .work_item_id;
    let second_item = store
        .create_work_item(CreateWorkItem {
            parent_instance_id: connected.instance.instance_id,
            feature_id: feature.feature_id,
            parent_work_item_id: None,
            owner_repository_id: connected.repository.repository_id,
            title: "Second item".to_owned(),
            summary: "Second".to_owned(),
            dependency_ids: Vec::new(),
            priority: 0,
            max_audit_iterations: 2,
        })
        .await
        .unwrap()
        .work_item_id;
    sqlx::query("UPDATE work_items SET status = 'completed' WHERE work_item_id IN (?, ?)")
        .bind(first_item.to_string())
        .bind(second_item.to_string())
        .execute(&store.pool)
        .await
        .unwrap();
    let audit_items = || {
        vec![
            AuditWorkItemInput {
                work_item_id: first_item,
            },
            AuditWorkItemInput {
                work_item_id: second_item,
            },
        ]
    };

    let first_audit = store
        .create_worker_run(CreateWorkerRun {
            parent_instance_id: connected.instance.instance_id,
            role: WorkerRole::Auditor,
            task_summary: "Audit two completed work items".to_owned(),
            context_refs: json!([]),
            audit_work_items: audit_items(),
            addresses_audit_run_id: None,
            work_item_id: None,
        })
        .await
        .unwrap();
    assert_eq!(first_audit.audit_scope.len(), 2);
    assert!(
        first_audit
            .audit_scope
            .iter()
            .all(|item| item.iteration == 1 && item.max_iterations == 2)
    );
    assert!(matches!(
        store
            .create_worker_run(CreateWorkerRun {
                parent_instance_id: connected.instance.instance_id,
                role: WorkerRole::Auditor,
                task_summary: "Cannot overlap audit iterations".to_owned(),
                context_refs: json!([]),
                audit_work_items: audit_items(),
                addresses_audit_run_id: None,
                work_item_id: None,
            })
            .await,
        Err(StoreError::Conflict(_))
    ));
    store
        .activate_worker_run(first_audit.worker_run_id)
        .await
        .unwrap();
    let audit_report = store
        .publish_message(PublishMessage {
            actor: ActorRef::Worker(first_audit.worker_run_id),
            kind: crate::domain::MessageKind::Report,
            target: MessageTarget::Broadcast,
            summary: "Audit findings".to_owned(),
            body: Some("# Audit report".to_owned()),
            artifact_kind: crate::domain::ArtifactKind::AuditReport,
            artifact_title: None,
            content_type: "text/markdown".to_owned(),
            feature_id: Some(feature.feature_id),
            work_item_id: None,
            request_id: None,
            thread_id: None,
            correlation_id: None,
            in_reply_to: None,
        })
        .await
        .unwrap()
        .artifact
        .unwrap();
    assert!(matches!(
        store
            .publish_message(PublishMessage {
                actor: ActorRef::Worker(first_audit.worker_run_id),
                kind: crate::domain::MessageKind::Report,
                target: MessageTarget::Broadcast,
                summary: "Duplicate audit report".to_owned(),
                body: Some("Must be rejected".to_owned()),
                artifact_kind: crate::domain::ArtifactKind::AuditReport,
                artifact_title: None,
                content_type: "text/markdown".to_owned(),
                feature_id: Some(feature.feature_id),
                work_item_id: None,
                request_id: None,
                thread_id: None,
                correlation_id: None,
                in_reply_to: None,
            })
            .await,
        Err(StoreError::Conflict(_))
    ));
    assert_eq!(
        store
            .get_work_item(first_item)
            .await
            .unwrap()
            .latest_audit_artifact_id,
        Some(audit_report.artifact_id)
    );
    assert_eq!(
        store
            .get_work_item(second_item)
            .await
            .unwrap()
            .latest_audit_artifact_id,
        Some(audit_report.artifact_id)
    );
    store
        .complete_worker_run(first_audit.worker_run_id, "Found one issue")
        .await
        .unwrap();

    let remediation = store
        .create_worker_run(CreateWorkerRun {
            parent_instance_id: connected.instance.instance_id,
            role: WorkerRole::Executor,
            task_summary: "Address the complete audit report".to_owned(),
            context_refs: json!([]),
            audit_work_items: Vec::new(),
            addresses_audit_run_id: Some(first_audit.worker_run_id),
            work_item_id: None,
        })
        .await
        .unwrap();
    assert_eq!(
        remediation.addresses_audit_run_id,
        Some(first_audit.worker_run_id)
    );
    assert!(matches!(
        store
            .create_worker_run(CreateWorkerRun {
                parent_instance_id: connected.instance.instance_id,
                role: WorkerRole::Executor,
                task_summary: "Second remediation must be rejected".to_owned(),
                context_refs: json!([]),
                audit_work_items: Vec::new(),
                addresses_audit_run_id: Some(first_audit.worker_run_id),
                work_item_id: None,
            })
            .await,
        Err(StoreError::Conflict(_))
    ));

    assert!(matches!(
        store
            .create_worker_run(CreateWorkerRun {
                parent_instance_id: connected.instance.instance_id,
                role: WorkerRole::Auditor,
                task_summary: "Cannot audit before remediation finishes".to_owned(),
                context_refs: json!([]),
                audit_work_items: audit_items(),
                addresses_audit_run_id: None,
                work_item_id: None,
            })
            .await,
        Err(StoreError::Conflict(_))
    ));
    store
        .activate_worker_run(remediation.worker_run_id)
        .await
        .unwrap();
    store
        .complete_worker_run(remediation.worker_run_id, "Remediation complete")
        .await
        .unwrap();

    let second_audit = store
        .create_worker_run(CreateWorkerRun {
            parent_instance_id: connected.instance.instance_id,
            role: WorkerRole::Auditor,
            task_summary: "Audit remediation".to_owned(),
            context_refs: json!([]),
            audit_work_items: audit_items(),
            addresses_audit_run_id: None,
            work_item_id: None,
        })
        .await
        .unwrap();
    assert!(
        second_audit
            .audit_scope
            .iter()
            .all(|item| item.iteration == 2)
    );
    assert!(matches!(
        store
            .create_worker_run(CreateWorkerRun {
                parent_instance_id: connected.instance.instance_id,
                role: WorkerRole::Auditor,
                task_summary: "Depth is exhausted".to_owned(),
                context_refs: json!([]),
                audit_work_items: audit_items(),
                addresses_audit_run_id: None,
                work_item_id: None,
            })
            .await,
        Err(StoreError::Conflict(_))
    ));

    store.pool.close().await;
    remove_database(&path);
}

#[tokio::test]
async fn remediation_cannot_start_after_the_next_audit_iteration() {
    let (store, path) = test_store(90).await;
    let connected = store
        .connect_orchestrator(connect_command(
            "late-remediation",
            "backend",
            "C:/audit/backend",
        ))
        .await
        .unwrap();
    let feature = store
        .create_feature(CreateFeature {
            parent_instance_id: connected.instance.instance_id,
            title: "Audit sequence".to_owned(),
            summary: "Reject late remediation".to_owned(),
        })
        .await
        .unwrap();
    let item = store
        .create_work_item(CreateWorkItem {
            parent_instance_id: connected.instance.instance_id,
            feature_id: feature.feature_id,
            parent_work_item_id: None,
            owner_repository_id: connected.repository.repository_id,
            title: "Completed item".to_owned(),
            summary: "Auditable".to_owned(),
            dependency_ids: Vec::new(),
            priority: 0,
            max_audit_iterations: 2,
        })
        .await
        .unwrap();
    sqlx::query("UPDATE work_items SET status = 'completed' WHERE work_item_id = ?")
        .bind(item.work_item_id.to_string())
        .execute(&store.pool)
        .await
        .unwrap();
    let audit_command = || CreateWorkerRun {
        parent_instance_id: connected.instance.instance_id,
        role: WorkerRole::Auditor,
        task_summary: "Audit completed item".to_owned(),
        context_refs: json!([]),
        audit_work_items: vec![AuditWorkItemInput {
            work_item_id: item.work_item_id,
        }],
        addresses_audit_run_id: None,
        work_item_id: None,
    };
    let first = store.create_worker_run(audit_command()).await.unwrap();
    store
        .activate_worker_run(first.worker_run_id)
        .await
        .unwrap();
    store
        .publish_message(PublishMessage {
            actor: ActorRef::Worker(first.worker_run_id),
            kind: MessageKind::Report,
            target: MessageTarget::Broadcast,
            summary: "No findings".to_owned(),
            body: Some("Audit report".to_owned()),
            artifact_kind: ArtifactKind::AuditReport,
            artifact_title: None,
            content_type: "text/markdown".to_owned(),
            feature_id: Some(feature.feature_id),
            work_item_id: Some(item.work_item_id),
            request_id: None,
            thread_id: None,
            correlation_id: None,
            in_reply_to: None,
        })
        .await
        .unwrap();
    store
        .complete_worker_run(first.worker_run_id, "Audited")
        .await
        .unwrap();
    store.create_worker_run(audit_command()).await.unwrap();

    assert!(matches!(
        store
            .create_worker_run(CreateWorkerRun {
                parent_instance_id: connected.instance.instance_id,
                role: WorkerRole::Executor,
                task_summary: "Late remediation".to_owned(),
                context_refs: json!([]),
                audit_work_items: Vec::new(),
                addresses_audit_run_id: Some(first.worker_run_id),
                work_item_id: None,
            })
            .await,
        Err(StoreError::Conflict(_))
    ));

    store.pool.close().await;
    remove_database(&path);
}

#[tokio::test]
async fn auditor_cannot_scope_work_items_from_another_project() {
    let (store, path) = test_store(90).await;
    let first = store
        .connect_orchestrator(connect_command("first", "backend", "C:/first/backend"))
        .await
        .unwrap();
    let second = store
        .connect_orchestrator(connect_command("second", "backend", "C:/second/backend"))
        .await
        .unwrap();
    let foreign_feature = store
        .create_feature(CreateFeature {
            parent_instance_id: second.instance.instance_id,
            title: "Foreign feature".to_owned(),
            summary: "Must remain isolated".to_owned(),
        })
        .await
        .unwrap();
    let foreign_item = store
        .create_work_item(CreateWorkItem {
            parent_instance_id: second.instance.instance_id,
            feature_id: foreign_feature.feature_id,
            parent_work_item_id: None,
            owner_repository_id: second.repository.repository_id,
            title: "Foreign item".to_owned(),
            summary: "Must not be audited by the first project".to_owned(),
            dependency_ids: Vec::new(),
            priority: 0,
            max_audit_iterations: 1,
        })
        .await
        .unwrap();
    sqlx::query("UPDATE work_items SET status = 'completed' WHERE work_item_id = ?")
        .bind(foreign_item.work_item_id.to_string())
        .execute(&store.pool)
        .await
        .unwrap();

    assert!(matches!(
        store
            .create_worker_run(CreateWorkerRun {
                parent_instance_id: first.instance.instance_id,
                role: WorkerRole::Auditor,
                task_summary: "Cross-project audit".to_owned(),
                context_refs: json!([]),
                audit_work_items: vec![AuditWorkItemInput {
                    work_item_id: foreign_item.work_item_id,
                }],
                addresses_audit_run_id: None,
                work_item_id: None,
            })
            .await,
        Err(StoreError::NotFound(_)) | Err(StoreError::Conflict(_))
    ));
    assert_eq!(
        store
            .get_work_item(foreign_item.work_item_id)
            .await
            .unwrap()
            .latest_audit_artifact_id,
        None
    );

    store.pool.close().await;
    remove_database(&path);
}

#[tokio::test]
async fn audit_scope_is_only_valid_for_auditor_role() {
    let (store, path) = test_store(90).await;
    let connected = store
        .connect_orchestrator(connect_command("project", "backend", "C:/project/backend"))
        .await
        .unwrap();
    let item = AuditWorkItemInput {
        work_item_id: WorkItemId::new(),
    };

    assert!(matches!(
        store
            .create_worker_run(CreateWorkerRun {
                parent_instance_id: connected.instance.instance_id,
                role: WorkerRole::Executor,
                task_summary: "Executor cannot declare audit scope".to_owned(),
                context_refs: json!([]),
                audit_work_items: vec![item],
                addresses_audit_run_id: None,
                work_item_id: None,
            })
            .await,
        Err(StoreError::Conflict(_))
    ));
    assert!(matches!(
        store
            .create_worker_run(CreateWorkerRun {
                parent_instance_id: connected.instance.instance_id,
                role: WorkerRole::Auditor,
                task_summary: "Auditor requires at least one item".to_owned(),
                context_refs: json!([]),
                audit_work_items: Vec::new(),
                addresses_audit_run_id: None,
                work_item_id: None,
            })
            .await,
        Err(StoreError::Conflict(_))
    ));

    store.pool.close().await;
    remove_database(&path);
}

#[tokio::test]
async fn queued_message_is_returned_as_repository_event() {
    let (store, path) = test_store(90).await;
    let backend = store
        .connect_orchestrator(connect_command("project", "backend", "C:/project/backend"))
        .await
        .unwrap();
    let web = store
        .connect_orchestrator(connect_command("project", "web", "C:/project/web"))
        .await
        .unwrap();
    let published = store
        .publish_message(publish_command(
            ActorRef::Orchestrator(backend.instance.instance_id),
            MessageTarget::Repositories(vec![web.repository.repository_id]),
            "Contract ready",
            None,
        ))
        .await
        .unwrap();

    let filtered = store
        .wait_for_events(
            ActorRef::Orchestrator(web.instance.instance_id),
            0,
            &[EventKind::WorkerRunCompleted],
            Some(Duration::ZERO),
            20,
        )
        .await
        .unwrap();
    assert_eq!(filtered.outcome, WaitOutcome::Timeout);
    assert_eq!(filtered.next_cursor, 0);

    let batch = store
        .wait_for_events(
            ActorRef::Orchestrator(web.instance.instance_id),
            0,
            &[EventKind::MessageAvailable],
            Some(Duration::ZERO),
            20,
        )
        .await
        .unwrap();

    assert_eq!(batch.outcome, WaitOutcome::Events);
    assert_eq!(batch.events.len(), 1);
    assert_eq!(batch.events[0].kind, EventKind::MessageAvailable);
    assert_eq!(
        batch.events[0].entity_id,
        published.message.message_id.to_string()
    );
    assert_eq!(batch.next_cursor, batch.events[0].event_id);

    store.pool.close().await;
    remove_database(&path);
}

#[tokio::test]
async fn waiter_is_notified_after_message_commit() {
    let (store, path) = test_store(90).await;
    let backend = store
        .connect_orchestrator(connect_command("project", "backend", "C:/project/backend"))
        .await
        .unwrap();
    let web = store
        .connect_orchestrator(connect_command("project", "web", "C:/project/web"))
        .await
        .unwrap();
    let web_instance_id = web.instance.instance_id;
    let web_repository_id = web.repository.repository_id;
    let waiting_store = store.clone();
    let waiter = tokio::spawn(async move {
        waiting_store
            .wait_for_events(
                ActorRef::Orchestrator(web_instance_id),
                0,
                &[],
                Some(Duration::from_secs(2)),
                20,
            )
            .await
    });
    tokio::task::yield_now().await;

    store
        .publish_message(publish_command(
            ActorRef::Orchestrator(backend.instance.instance_id),
            MessageTarget::Repositories(vec![web_repository_id]),
            "Wake waiter",
            None,
        ))
        .await
        .unwrap();
    let batch = waiter.await.unwrap().unwrap();

    assert_eq!(batch.outcome, WaitOutcome::Events);
    assert_eq!(batch.events.len(), 1);

    store.pool.close().await;
    remove_database(&path);
}

#[tokio::test]
async fn dozens_of_long_poll_waiters_receive_one_committed_event() {
    let (store, path) = test_store(90).await;
    let sender = store
        .connect_orchestrator(connect_command("wait-load", "backend", "C:/wait/backend"))
        .await
        .unwrap();
    let receiver = store
        .connect_orchestrator(connect_command("wait-load", "web", "C:/wait/web"))
        .await
        .unwrap();
    let mut waiters = Vec::new();
    for _ in 0..32 {
        let waiting_store = store.clone();
        let instance_id = receiver.instance.instance_id;
        waiters.push(tokio::spawn(async move {
            waiting_store
                .wait_for_events(
                    ActorRef::Orchestrator(instance_id),
                    0,
                    &[EventKind::MessageAvailable],
                    Some(Duration::from_secs(3)),
                    1,
                )
                .await
        }));
    }
    tokio::task::yield_now().await;
    store
        .publish_message(publish_command(
            ActorRef::Orchestrator(sender.instance.instance_id),
            MessageTarget::Repositories(vec![receiver.repository.repository_id]),
            "Wake all waiters",
            None,
        ))
        .await
        .unwrap();

    for waiter in waiters {
        let batch = tokio::time::timeout(Duration::from_secs(4), waiter)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(batch.outcome, WaitOutcome::Events);
        assert_eq!(batch.events.len(), 1);
        assert_eq!(batch.events[0].kind, EventKind::MessageAvailable);
    }

    store.pool.close().await;
    remove_database(&path);
}

#[tokio::test]
async fn wait_timeout_preserves_cursor_and_renews_lease() {
    let (store, path) = test_store(1).await;
    let connected = store
        .connect_orchestrator(connect_command("project", "backend", "C:/project/backend"))
        .await
        .unwrap();
    let initial_heartbeat = connected.instance.last_heartbeat_at;

    let batch = store
        .wait_for_events(
            ActorRef::Orchestrator(connected.instance.instance_id),
            42,
            &[],
            Some(Duration::from_millis(1_500)),
            20,
        )
        .await
        .unwrap();
    let renewed = store
        .get_instance(connected.instance.instance_id)
        .await
        .unwrap();

    assert_eq!(batch.outcome, WaitOutcome::Timeout);
    assert_eq!(batch.next_cursor, 42);
    assert!(batch.events.is_empty());
    assert!(renewed.last_heartbeat_at > initial_heartbeat);
    assert!(renewed.lease_expires_at > utc_now());

    store.pool.close().await;
    remove_database(&path);
}

#[tokio::test]
async fn maintenance_reaps_expired_runtime_state_and_reports_metrics() {
    let (store, path) = test_store(90).await;
    store.readiness_check().await.unwrap();
    let requester = store
        .connect_orchestrator(connect_command("operations", "web", "C:/operations/web"))
        .await
        .unwrap();
    let target = store
        .connect_orchestrator(connect_command(
            "operations",
            "backend",
            "C:/operations/backend",
        ))
        .await
        .unwrap();
    let request = store
        .ask_repository(AskRepository {
            actor: ActorRef::Orchestrator(requester.instance.instance_id),
            target_repository_id: target.repository.repository_id,
            question_summary: "Operational request".to_owned(),
            body: None,
            context_refs: json!([]),
        })
        .await
        .unwrap();
    store
        .claim_request(
            ActorRef::Orchestrator(target.instance.instance_id),
            request.request_id,
        )
        .await
        .unwrap();
    store
        .publish_message(publish_command(
            ActorRef::Orchestrator(requester.instance.instance_id),
            MessageTarget::Repositories(vec![target.repository.repository_id]),
            "Pending delivery",
            None,
        ))
        .await
        .unwrap();
    sqlx::query("UPDATE agent_instances SET lease_expires_at = ? WHERE instance_id = ?")
        .bind("2000-01-01T00:00:00.000000Z")
        .bind(target.instance.instance_id.to_string())
        .execute(&store.pool)
        .await
        .unwrap();
    sqlx::query("UPDATE requests SET claim_expires_at = ? WHERE request_id = ?")
        .bind("2000-01-01T00:00:00.000000Z")
        .bind(request.request_id.to_string())
        .execute(&store.pool)
        .await
        .unwrap();

    let before = store.operational_metrics().await.unwrap();
    assert_eq!(before.pending_deliveries, 1);
    assert_eq!(before.online_repositories, 1);
    assert_eq!(before.expired_request_claims, 1);
    assert!(before.oldest_pending_delivery_at.is_some());
    assert!(before.oldest_pending_delivery_age_seconds.is_some());
    let report = store.run_lease_maintenance().await.unwrap();
    assert_eq!(report.disconnected_orchestrators, 1);
    assert_eq!(report.requeued_requests, 1);
    assert_eq!(
        store.get_request(request.request_id).await.unwrap().status,
        RequestStatus::Pending
    );
    let after = store.operational_metrics().await.unwrap();
    assert_eq!(after.expired_request_claims, 0);

    sqlx::query(
        "WITH RECURSIVE sequence(value) AS (
             SELECT 1 UNION ALL SELECT value + 1 FROM sequence WHERE value < 1005
         )
         INSERT INTO events(
             project_id, recipient_repository_id, kind, entity_type, entity_id, created_at
         )
         SELECT ?, ?, 'request_available', 'request', 'retained-' || value,
                '2000-01-01T00:00:00.000000Z'
         FROM sequence",
    )
    .bind(requester.project.project_id.to_string())
    .bind(requester.repository.repository_id.to_string())
    .execute(&store.pool)
    .await
    .unwrap();
    sqlx::query(
        "WITH RECURSIVE sequence(value) AS (
             SELECT 1 UNION ALL SELECT value + 1 FROM sequence WHERE value < 1005
         )
         INSERT INTO idempotency_records(
             actor_key, operation, idempotency_key, request_digest,
             result_entity_id, created_at
         )
         SELECT 'retention-test', 'test', 'key-' || value,
                'digest', 'result', '2000-01-01T00:00:00.000000Z'
         FROM sequence",
    )
    .execute(&store.pool)
    .await
    .unwrap();
    let retention = store
        .run_retention(Duration::from_secs(7 * 24 * 60 * 60), 1000)
        .await
        .unwrap();
    assert_eq!(retention.deleted_events, 1000);
    assert_eq!(retention.deleted_idempotency_records, 1000);
    let remaining_old_events: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM events WHERE created_at < '2001-01-01'")
            .fetch_one(&store.pool)
            .await
            .unwrap();
    assert_eq!(remaining_old_events, 5);
    let maintenance_indexes: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM sqlite_master
         WHERE type = 'index' AND name IN (
             'agent_instances_expiry_idx', 'worker_runs_expiry_idx',
             'requests_claim_expiry_idx', 'events_retention_idx',
             'idempotency_records_retention_idx'
         )",
    )
    .fetch_one(&store.pool)
    .await
    .unwrap();
    assert_eq!(maintenance_indexes, 5);
    for (query, expected_index) in [
        (
            "EXPLAIN QUERY PLAN SELECT COUNT(*) FROM deliveries WHERE status = 'pending'",
            "deliveries_pending_metrics_idx",
        ),
        (
            "EXPLAIN QUERY PLAN SELECT COUNT(*) FROM requests WHERE status IN ('pending', 'claimed')",
            "requests_open_metrics_idx",
        ),
        (
            "EXPLAIN QUERY PLAN SELECT COUNT(*) FROM work_items WHERE status IN ('claimed', 'in_progress', 'blocked') AND claim_expires_at > '2020-01-01'",
            "work_items_active_claim_metrics_idx",
        ),
        (
            "EXPLAIN QUERY PLAN SELECT COUNT(*) FROM worker_runs WHERE status = 'active' AND lease_expires_at > '2020-01-01'",
            "worker_runs_active_metrics_idx",
        ),
        (
            "EXPLAIN QUERY PLAN SELECT request_id FROM requests WHERE status = 'claimed' AND claimed_by_instance_id = 'instance' AND claim_expires_at > '2020-01-01'",
            "requests_claimed_instance_idx",
        ),
        (
            "EXPLAIN QUERY PLAN SELECT request_id FROM requests WHERE status = 'claimed' AND claimed_by_worker_run_id = 'worker' AND claim_expires_at > '2020-01-01'",
            "requests_claimed_worker_idx",
        ),
        (
            "EXPLAIN QUERY PLAN SELECT work_item_id FROM work_items WHERE status IN ('claimed', 'in_progress', 'blocked') AND assignee_instance_id = 'instance' AND claim_expires_at > '2020-01-01'",
            "work_items_claim_owner_idx",
        ),
        (
            "EXPLAIN QUERY PLAN SELECT message_id FROM messages WHERE artifact_id = 'artifact'",
            "messages_artifact_idx",
        ),
        (
            "EXPLAIN QUERY PLAN SELECT request_id FROM requests WHERE question_artifact_id = 'artifact'",
            "requests_question_artifact_idx",
        ),
        (
            "EXPLAIN QUERY PLAN SELECT artifact_id FROM artifacts WHERE producer_worker_run_id = 'worker' AND kind = 'implementation_report'",
            "artifacts_producer_kind_idx",
        ),
        (
            "EXPLAIN QUERY PLAN SELECT event_id FROM events WHERE recipient_repository_id = 'repository' AND kind = 'message_available' AND event_id > 0 ORDER BY event_id",
            "events_repository_kind_cursor_idx",
        ),
    ] {
        let plan_rows = sqlx::query(query).fetch_all(&store.pool).await.unwrap();
        let plan = plan_rows
            .iter()
            .map(|row| row.try_get::<String, _>("detail").unwrap())
            .collect::<Vec<_>>()
            .join(" ");
        assert!(
            plan.contains(expected_index),
            "unexpected query plan: {plan}"
        );
    }

    store.expire_local_leases().await.unwrap();
    assert_eq!(
        store
            .operational_metrics()
            .await
            .unwrap()
            .online_orchestrators,
        0
    );

    store.pool.close().await;
    remove_database(&path);
}

#[tokio::test]
async fn only_one_actor_can_claim_the_same_request() {
    let (store, path) = test_store(90).await;
    let requester = store
        .connect_orchestrator(connect_command("project", "web", "C:/project/web"))
        .await
        .unwrap();
    let target_one = store
        .connect_orchestrator(connect_command("project", "backend", "C:/project/backend"))
        .await
        .unwrap();
    let target_two = store
        .connect_orchestrator(connect_command("project", "backend", "C:/project/backend"))
        .await
        .unwrap();
    let request = store
        .ask_repository(AskRepository {
            actor: ActorRef::Orchestrator(requester.instance.instance_id),
            target_repository_id: target_one.repository.repository_id,
            question_summary: "Which response fields are guaranteed?".to_owned(),
            body: Some("Need the complete API contract.".to_owned()),
            context_refs: json!([]),
        })
        .await
        .unwrap();
    let target_snapshot = store
        .connect_orchestrator(connect_command("project", "backend", "C:/project/backend"))
        .await
        .unwrap();
    assert_eq!(target_snapshot.open_request_count, 1);

    let (first, second) = tokio::join!(
        store.claim_request(
            ActorRef::Orchestrator(target_one.instance.instance_id),
            request.request_id
        ),
        store.claim_request(
            ActorRef::Orchestrator(target_two.instance.instance_id),
            request.request_id
        )
    );

    assert_ne!(first.is_ok(), second.is_ok());
    let conflict = match (first, second) {
        (Ok(_), Err(error)) | (Err(error), Ok(_)) => error,
        _ => unreachable!("exactly one request claim succeeds"),
    };
    assert!(matches!(
        conflict,
        StoreError::ClaimConflict {
            retry_after_seconds,
            retry_at: Some(_),
            ..
        } if retry_after_seconds.is_some_and(|seconds| seconds > 0)
    ));
    let claimed = store.get_request(request.request_id).await.unwrap();
    assert_eq!(claimed.status, RequestStatus::Claimed);
    assert!(claimed.claim_expires_at.is_some());

    store.pool.close().await;
    remove_database(&path);
}

#[tokio::test]
async fn expired_claim_can_be_taken_over_and_retryable_failure_requeues() {
    let (store, path) = test_store(90).await;
    let requester = store
        .connect_orchestrator(connect_command("project", "web", "C:/project/web"))
        .await
        .unwrap();
    let target_one = store
        .connect_orchestrator(connect_command("project", "backend", "C:/project/backend"))
        .await
        .unwrap();
    let target_two = store
        .connect_orchestrator(connect_command("project", "backend", "C:/project/backend"))
        .await
        .unwrap();
    let request = store
        .ask_repository(AskRepository {
            actor: ActorRef::Orchestrator(requester.instance.instance_id),
            target_repository_id: target_one.repository.repository_id,
            question_summary: "Can this request be retried?".to_owned(),
            body: None,
            context_refs: json!([]),
        })
        .await
        .unwrap();
    store
        .claim_request(
            ActorRef::Orchestrator(target_one.instance.instance_id),
            request.request_id,
        )
        .await
        .unwrap();
    sqlx::query("UPDATE requests SET claim_expires_at = ? WHERE request_id = ?")
        .bind("2000-01-01T00:00:00.000000Z")
        .bind(request.request_id.to_string())
        .execute(&store.pool)
        .await
        .unwrap();

    store
        .claim_request(
            ActorRef::Orchestrator(target_two.instance.instance_id),
            request.request_id,
        )
        .await
        .unwrap();
    let requeued = store
        .fail_request(FailRequest {
            actor: ActorRef::Orchestrator(target_two.instance.instance_id),
            request_id: request.request_id,
            retryable: true,
            summary: "Temporary dependency failure".to_owned(),
        })
        .await
        .unwrap();
    assert_eq!(requeued.status, RequestStatus::Pending);
    assert_eq!(
        requeued.failure_summary.as_deref(),
        Some("Temporary dependency failure")
    );

    store.pool.close().await;
    remove_database(&path);
}

#[tokio::test]
async fn answer_atomically_closes_request_and_delivers_artifact() {
    let (store, path) = test_store(90).await;
    let requester = store
        .connect_orchestrator(connect_command("project", "web", "C:/project/web"))
        .await
        .unwrap();
    let target = store
        .connect_orchestrator(connect_command("project", "backend", "C:/project/backend"))
        .await
        .unwrap();
    let request = store
        .ask_repository(AskRepository {
            actor: ActorRef::Orchestrator(requester.instance.instance_id),
            target_repository_id: target.repository.repository_id,
            question_summary: "Return the contract".to_owned(),
            body: None,
            context_refs: json!([]),
        })
        .await
        .unwrap();
    store
        .claim_request(
            ActorRef::Orchestrator(target.instance.instance_id),
            request.request_id,
        )
        .await
        .unwrap();

    let answered = store
        .answer_request(AnswerRequest {
            actor: ActorRef::Orchestrator(target.instance.instance_id),
            request_id: request.request_id,
            answer_summary: "Contract attached".to_owned(),
            body: Some("# API contract".to_owned()),
            content_type: "text/markdown".to_owned(),
        })
        .await
        .unwrap();

    assert_eq!(answered.request.status, RequestStatus::Answered);
    assert_eq!(
        answered.message.correlation_id,
        Some(request.request_id.to_string())
    );
    assert_eq!(answered.message.request_id, Some(request.request_id));
    assert_eq!(answered.artifact.unwrap().content, "# API contract");
    let inbox = store
        .list_inbox(
            ActorRef::Orchestrator(requester.instance.instance_id),
            None,
            &[],
            20,
        )
        .await
        .unwrap();
    assert_eq!(inbox.items.len(), 1);
    assert!(
        store
            .answer_request(AnswerRequest {
                actor: ActorRef::Orchestrator(target.instance.instance_id),
                request_id: request.request_id,
                answer_summary: "Duplicate".to_owned(),
                body: None,
                content_type: "text/plain".to_owned(),
            })
            .await
            .is_err()
    );

    store.pool.close().await;
    remove_database(&path);
}

#[tokio::test]
async fn claim_owner_can_renew_release_and_terminally_fail_request() {
    let (store, path) = test_store(90).await;
    let requester = store
        .connect_orchestrator(connect_command("project", "web", "C:/project/web"))
        .await
        .unwrap();
    let owner = store
        .connect_orchestrator(connect_command("project", "backend", "C:/project/backend"))
        .await
        .unwrap();
    let peer = store
        .connect_orchestrator(connect_command("project", "backend", "C:/project/backend"))
        .await
        .unwrap();
    let request = store
        .ask_repository(AskRepository {
            actor: ActorRef::Orchestrator(requester.instance.instance_id),
            target_repository_id: owner.repository.repository_id,
            question_summary: "Resolve ownership rules".to_owned(),
            body: None,
            context_refs: json!([]),
        })
        .await
        .unwrap();
    let claimed = store
        .claim_request(
            ActorRef::Orchestrator(owner.instance.instance_id),
            request.request_id,
        )
        .await
        .unwrap();
    assert!(
        store
            .renew_request_claim(
                ActorRef::Orchestrator(peer.instance.instance_id),
                request.request_id,
            )
            .await
            .is_err()
    );
    let renewed = store
        .renew_request_claim(
            ActorRef::Orchestrator(owner.instance.instance_id),
            request.request_id,
        )
        .await
        .unwrap();
    assert!(renewed.claim_expires_at >= claimed.claim_expires_at);
    let released = store
        .release_request_claim(
            ActorRef::Orchestrator(owner.instance.instance_id),
            request.request_id,
        )
        .await
        .unwrap();
    assert_eq!(released.status, RequestStatus::Pending);
    store
        .claim_request(
            ActorRef::Orchestrator(peer.instance.instance_id),
            request.request_id,
        )
        .await
        .unwrap();
    let failed = store
        .fail_request(FailRequest {
            actor: ActorRef::Orchestrator(peer.instance.instance_id),
            request_id: request.request_id,
            retryable: false,
            summary: "No supported contract exists".to_owned(),
        })
        .await
        .unwrap();
    assert_eq!(failed.status, RequestStatus::Failed);
    assert!(failed.finished_at.is_some());
    let events = store
        .wait_for_events(
            ActorRef::Orchestrator(requester.instance.instance_id),
            0,
            &[EventKind::RequestFailed],
            Some(Duration::ZERO),
            20,
        )
        .await
        .unwrap();
    assert_eq!(events.events.len(), 1);
    assert_eq!(events.events[0].entity_id, request.request_id.to_string());

    store.pool.close().await;
    remove_database(&path);
}

#[tokio::test]
async fn creation_idempotency_replays_result_and_rejects_changed_payload() {
    let (store, path) = test_store(90).await;
    let backend = store
        .connect_orchestrator(connect_command("project", "backend", "C:/project/backend"))
        .await
        .unwrap();
    let web = store
        .connect_orchestrator(connect_command("project", "web", "C:/project/web"))
        .await
        .unwrap();

    let feature_command = CreateFeature {
        parent_instance_id: backend.instance.instance_id,
        title: "Idempotent feature".to_owned(),
        summary: "Created once".to_owned(),
    };
    let first_feature = store
        .create_feature_idempotent(feature_command.clone(), Some("feature-key"))
        .await
        .unwrap();
    let replayed_feature = store
        .create_feature_idempotent(feature_command, Some("feature-key"))
        .await
        .unwrap();
    assert_eq!(first_feature.feature_id, replayed_feature.feature_id);
    let work_command = CreateWorkItem {
        parent_instance_id: backend.instance.instance_id,
        feature_id: first_feature.feature_id,
        parent_work_item_id: None,
        owner_repository_id: backend.repository.repository_id,
        title: "Idempotent item".to_owned(),
        summary: "Created once".to_owned(),
        dependency_ids: Vec::new(),
        priority: 0,
        max_audit_iterations: 1,
    };
    let first_work_item = store
        .create_work_item_idempotent(work_command.clone(), Some("work-key"))
        .await
        .unwrap();
    let replayed_work_item = store
        .create_work_item_idempotent(work_command, Some("work-key"))
        .await
        .unwrap();
    assert_eq!(
        first_work_item.work_item_id,
        replayed_work_item.work_item_id
    );

    let worker_command = CreateWorkerRun {
        parent_instance_id: backend.instance.instance_id,
        role: WorkerRole::Executor,
        task_summary: "Idempotent worker".to_owned(),
        context_refs: json!([]),
        audit_work_items: Vec::new(),
        addresses_audit_run_id: None,
        work_item_id: None,
    };
    let first_worker = store
        .create_worker_run_idempotent(worker_command.clone(), Some("worker-key"))
        .await
        .unwrap();
    let replayed_worker = store
        .create_worker_run_idempotent(worker_command, Some("worker-key"))
        .await
        .unwrap();
    assert_eq!(first_worker.worker_run_id, replayed_worker.worker_run_id);

    let message_command = publish_command(
        ActorRef::Orchestrator(backend.instance.instance_id),
        MessageTarget::Repositories(vec![web.repository.repository_id]),
        "Idempotent message",
        Some("One body"),
    );
    let first_message = store
        .publish_message_idempotent(message_command.clone(), Some("message-key"))
        .await
        .unwrap();
    let replayed_message = store
        .publish_message_idempotent(message_command, Some("message-key"))
        .await
        .unwrap();
    assert_eq!(
        first_message.message.message_id,
        replayed_message.message.message_id
    );

    let request_command = AskRepository {
        actor: ActorRef::Orchestrator(web.instance.instance_id),
        target_repository_id: backend.repository.repository_id,
        question_summary: "Idempotent question".to_owned(),
        body: None,
        context_refs: json!([]),
    };
    let first_request = store
        .ask_repository_idempotent(request_command.clone(), Some("request-key"))
        .await
        .unwrap();
    let replayed_request = store
        .ask_repository_idempotent(request_command.clone(), Some("request-key"))
        .await
        .unwrap();
    assert_eq!(first_request.request_id, replayed_request.request_id);
    let mut changed_request = request_command;
    changed_request.question_summary = "Changed payload".to_owned();
    assert!(matches!(
        store
            .ask_repository_idempotent(changed_request, Some("request-key"))
            .await,
        Err(StoreError::Conflict(_))
    ));

    let request_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM requests")
        .fetch_one(&store.pool)
        .await
        .unwrap();
    let message_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM messages")
        .fetch_one(&store.pool)
        .await
        .unwrap();
    let worker_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM worker_runs")
        .fetch_one(&store.pool)
        .await
        .unwrap();
    assert_eq!((request_count, message_count, worker_count), (1, 1, 1));
    let digest_lengths: Vec<i64> =
        sqlx::query_scalar("SELECT LENGTH(request_digest) FROM idempotency_records")
            .fetch_all(&store.pool)
            .await
            .unwrap();
    assert_eq!(digest_lengths.len(), 5);
    assert!(digest_lengths.iter().all(|length| *length == 64));

    store.pool.close().await;
    remove_database(&path);
}

#[tokio::test]
async fn actor_activity_renews_only_unexpired_request_claims() {
    let (store, path) = test_store(90).await;
    let requester = store
        .connect_orchestrator(connect_command("project", "web", "C:/project/web"))
        .await
        .unwrap();
    let target = store
        .connect_orchestrator(connect_command("project", "backend", "C:/project/backend"))
        .await
        .unwrap();
    let request = store
        .ask_repository(AskRepository {
            actor: ActorRef::Orchestrator(requester.instance.instance_id),
            target_repository_id: target.repository.repository_id,
            question_summary: "Renew me".to_owned(),
            body: None,
            context_refs: json!([]),
        })
        .await
        .unwrap();
    let claimed = store
        .claim_request(
            ActorRef::Orchestrator(target.instance.instance_id),
            request.request_id,
        )
        .await
        .unwrap();
    let short_expiry =
        (Utc::now() + chrono::Duration::seconds(2)).to_rfc3339_opts(SecondsFormat::Micros, true);
    sqlx::query("UPDATE requests SET claim_expires_at = ? WHERE request_id = ?")
        .bind(&short_expiry)
        .bind(request.request_id.to_string())
        .execute(&store.pool)
        .await
        .unwrap();
    store.heartbeat(target.instance.instance_id).await.unwrap();
    let renewed = store.get_request(request.request_id).await.unwrap();
    assert!(renewed.claim_expires_at > Some(short_expiry));
    assert!(renewed.claim_expires_at > claimed.claim_expires_at);

    sqlx::query("UPDATE requests SET claim_expires_at = ? WHERE request_id = ?")
        .bind("2000-01-01T00:00:00.000000Z")
        .bind(request.request_id.to_string())
        .execute(&store.pool)
        .await
        .unwrap();
    store.heartbeat(target.instance.instance_id).await.unwrap();
    let expired = store.get_request(request.request_id).await.unwrap();
    assert_eq!(
        expired.claim_expires_at.as_deref(),
        Some("2000-01-01T00:00:00.000000Z")
    );

    store.pool.close().await;
    remove_database(&path);
}

#[tokio::test]
async fn three_worker_runs_claim_and_answer_independent_requests() {
    let (store, path) = test_store(90).await;
    let requester = store
        .connect_orchestrator(connect_command("project", "web", "C:/project/web"))
        .await
        .unwrap();
    let target = store
        .connect_orchestrator(connect_command("project", "backend", "C:/project/backend"))
        .await
        .unwrap();

    let mut requests = Vec::new();
    let mut workers = Vec::new();
    for index in 0..3 {
        requests.push(
            store
                .ask_repository(AskRepository {
                    actor: ActorRef::Orchestrator(requester.instance.instance_id),
                    target_repository_id: target.repository.repository_id,
                    question_summary: format!("Question {index}"),
                    body: None,
                    context_refs: json!([]),
                })
                .await
                .unwrap(),
        );
        let worker = store
            .create_worker_run(CreateWorkerRun {
                parent_instance_id: target.instance.instance_id,
                role: WorkerRole::Butler,
                task_summary: format!("Answer question {index}"),
                context_refs: json!([]),
                audit_work_items: Vec::new(),
                addresses_audit_run_id: None,
                work_item_id: None,
            })
            .await
            .unwrap();
        workers.push(
            store
                .activate_worker_run(worker.worker_run_id)
                .await
                .unwrap(),
        );
    }

    let (claim_one, claim_two, claim_three) = tokio::join!(
        store.claim_request(
            ActorRef::Worker(workers[0].worker_run_id),
            requests[0].request_id,
        ),
        store.claim_request(
            ActorRef::Worker(workers[1].worker_run_id),
            requests[1].request_id,
        ),
        store.claim_request(
            ActorRef::Worker(workers[2].worker_run_id),
            requests[2].request_id,
        ),
    );
    assert!(claim_one.is_ok() && claim_two.is_ok() && claim_three.is_ok());

    let (answer_one, answer_two, answer_three) = tokio::join!(
        store.answer_request(AnswerRequest {
            actor: ActorRef::Worker(workers[0].worker_run_id),
            request_id: requests[0].request_id,
            answer_summary: "Answer 0".to_owned(),
            body: None,
            content_type: "text/plain".to_owned(),
        }),
        store.answer_request(AnswerRequest {
            actor: ActorRef::Worker(workers[1].worker_run_id),
            request_id: requests[1].request_id,
            answer_summary: "Answer 1".to_owned(),
            body: None,
            content_type: "text/plain".to_owned(),
        }),
        store.answer_request(AnswerRequest {
            actor: ActorRef::Worker(workers[2].worker_run_id),
            request_id: requests[2].request_id,
            answer_summary: "Answer 2".to_owned(),
            body: None,
            content_type: "text/plain".to_owned(),
        }),
    );
    let answers = [
        answer_one.unwrap(),
        answer_two.unwrap(),
        answer_three.unwrap(),
    ];
    for (index, answer) in answers.iter().enumerate() {
        assert_eq!(answer.request.request_id, requests[index].request_id);
        assert_eq!(
            answer.message.correlation_id,
            Some(requests[index].request_id.to_string())
        );
    }
    let inbox = store
        .list_inbox(
            ActorRef::Orchestrator(requester.instance.instance_id),
            None,
            &[],
            20,
        )
        .await
        .unwrap();
    assert_eq!(inbox.items.len(), 3);

    store.pool.close().await;
    remove_database(&path);
}

#[tokio::test]
async fn work_graph_unlocks_dependency_and_links_worker_report() {
    let (store, path) = test_store(90).await;
    let backend = store
        .connect_orchestrator(connect_command("project", "backend", "C:/project/backend"))
        .await
        .unwrap();
    let web = store
        .connect_orchestrator(connect_command("project", "web", "C:/project/web"))
        .await
        .unwrap();
    let feature = store
        .create_feature(CreateFeature {
            parent_instance_id: backend.instance.instance_id,
            title: "Cross-repository feature".to_owned(),
            summary: "Implement API then UI".to_owned(),
        })
        .await
        .unwrap();
    let api = store
        .create_work_item(CreateWorkItem {
            parent_instance_id: backend.instance.instance_id,
            feature_id: feature.feature_id,
            parent_work_item_id: None,
            owner_repository_id: backend.repository.repository_id,
            title: "Implement API".to_owned(),
            summary: "Backend contract".to_owned(),
            dependency_ids: Vec::new(),
            priority: 10,
            max_audit_iterations: 2,
        })
        .await
        .unwrap();
    let ui = store
        .create_work_item(CreateWorkItem {
            parent_instance_id: backend.instance.instance_id,
            feature_id: feature.feature_id,
            parent_work_item_id: None,
            owner_repository_id: web.repository.repository_id,
            title: "Implement UI".to_owned(),
            summary: "Consume backend contract".to_owned(),
            dependency_ids: vec![api.work_item_id],
            priority: 5,
            max_audit_iterations: 1,
        })
        .await
        .unwrap();
    assert_eq!(api.status, WorkItemStatus::Ready);
    assert_eq!(ui.status, WorkItemStatus::Planned);
    let bootstrap = store
        .connect_orchestrator(connect_command("project", "backend", "C:/project/backend"))
        .await
        .unwrap();
    assert_eq!(bootstrap.active_feature_count, 1);

    let claimed = store
        .claim_work_item(backend.instance.instance_id, api.work_item_id, api.revision)
        .await
        .unwrap();
    let worker = store
        .create_worker_run(CreateWorkerRun {
            parent_instance_id: backend.instance.instance_id,
            role: WorkerRole::Executor,
            task_summary: "Implement API".to_owned(),
            context_refs: json!([]),
            audit_work_items: Vec::new(),
            addresses_audit_run_id: None,
            work_item_id: Some(api.work_item_id),
        })
        .await
        .unwrap();
    store
        .activate_worker_run(worker.worker_run_id)
        .await
        .unwrap();
    let in_progress = store.get_work_item(api.work_item_id).await.unwrap();
    assert_eq!(in_progress.status, WorkItemStatus::InProgress);
    assert!(in_progress.revision > claimed.revision);
    let unrelated_report = store
        .publish_message(PublishMessage {
            actor: ActorRef::Orchestrator(backend.instance.instance_id),
            kind: crate::domain::MessageKind::Report,
            target: MessageTarget::Repositories(vec![web.repository.repository_id]),
            summary: "Unrelated implementation report".to_owned(),
            body: Some("Not produced by the linked executor".to_owned()),
            artifact_kind: crate::domain::ArtifactKind::ImplementationReport,
            artifact_title: None,
            content_type: "text/markdown".to_owned(),
            feature_id: Some(feature.feature_id),
            work_item_id: Some(api.work_item_id),
            request_id: None,
            thread_id: None,
            correlation_id: None,
            in_reply_to: None,
        })
        .await
        .unwrap()
        .artifact
        .unwrap();
    let report = store
        .publish_message(PublishMessage {
            actor: ActorRef::Worker(worker.worker_run_id),
            kind: crate::domain::MessageKind::Report,
            target: MessageTarget::Repositories(vec![web.repository.repository_id]),
            summary: "API implemented".to_owned(),
            body: Some("# Implementation report".to_owned()),
            artifact_kind: crate::domain::ArtifactKind::ImplementationReport,
            artifact_title: None,
            content_type: "text/markdown".to_owned(),
            feature_id: Some(feature.feature_id),
            work_item_id: Some(api.work_item_id),
            request_id: None,
            thread_id: None,
            correlation_id: Some(api.work_item_id.to_string()),
            in_reply_to: None,
        })
        .await
        .unwrap();
    assert_eq!(report.message.feature_id, Some(feature.feature_id));
    assert_eq!(report.message.work_item_id, Some(api.work_item_id));
    store
        .complete_worker_run(worker.worker_run_id, "API complete")
        .await
        .unwrap();
    assert!(matches!(
        store
            .update_work_item(UpdateWorkItem {
                parent_instance_id: backend.instance.instance_id,
                work_item_id: api.work_item_id,
                expected_revision: in_progress.revision,
                title: None,
                summary: None,
                owner_repository_id: None,
                parent_work_item_id: None,
                dependency_ids: None,
                priority: None,
                status: Some(WorkItemStatus::Completed),
                implementation_artifact_id: Some(unrelated_report.artifact_id),
            })
            .await,
        Err(StoreError::Conflict(_))
    ));
    let completed = store
        .update_work_item(UpdateWorkItem {
            parent_instance_id: backend.instance.instance_id,
            work_item_id: api.work_item_id,
            expected_revision: in_progress.revision,
            title: None,
            summary: None,
            owner_repository_id: None,
            parent_work_item_id: None,
            dependency_ids: None,
            priority: None,
            status: Some(WorkItemStatus::Completed),
            implementation_artifact_id: Some(report.artifact.unwrap().artifact_id),
        })
        .await
        .unwrap();
    assert_eq!(completed.status, WorkItemStatus::Completed);
    assert!(completed.implementation_artifact_id.is_some());
    let unlocked_ui = store.get_work_item(ui.work_item_id).await.unwrap();
    assert_eq!(unlocked_ui.status, WorkItemStatus::Ready);
    assert_eq!(unlocked_ui.revision, ui.revision + 1);

    let stale = store
        .update_work_item(UpdateWorkItem {
            parent_instance_id: backend.instance.instance_id,
            work_item_id: api.work_item_id,
            expected_revision: in_progress.revision,
            title: Some("Stale overwrite".to_owned()),
            summary: None,
            owner_repository_id: None,
            parent_work_item_id: None,
            dependency_ids: None,
            priority: None,
            status: None,
            implementation_artifact_id: None,
        })
        .await
        .unwrap_err();
    assert!(matches!(
        stale,
        StoreError::RevisionConflict { actual, .. } if actual == completed.revision
    ));

    store.pool.close().await;
    remove_database(&path);
}

#[tokio::test]
async fn work_item_claim_and_dependency_cycle_are_conflict_safe() {
    let (store, path) = test_store(90).await;
    let first = store
        .connect_orchestrator(connect_command("project", "backend", "C:/project/backend"))
        .await
        .unwrap();
    let second = store
        .connect_orchestrator(connect_command("project", "backend", "C:/project/backend"))
        .await
        .unwrap();
    let feature = store
        .create_feature(CreateFeature {
            parent_instance_id: first.instance.instance_id,
            title: "Claim feature".to_owned(),
            summary: "Concurrency".to_owned(),
        })
        .await
        .unwrap();
    let root = store
        .create_work_item(CreateWorkItem {
            parent_instance_id: first.instance.instance_id,
            feature_id: feature.feature_id,
            parent_work_item_id: None,
            owner_repository_id: first.repository.repository_id,
            title: "Root".to_owned(),
            summary: "Root item".to_owned(),
            dependency_ids: Vec::new(),
            priority: 0,
            max_audit_iterations: 1,
        })
        .await
        .unwrap();
    let dependent = store
        .create_work_item(CreateWorkItem {
            parent_instance_id: first.instance.instance_id,
            feature_id: feature.feature_id,
            parent_work_item_id: None,
            owner_repository_id: first.repository.repository_id,
            title: "Dependent".to_owned(),
            summary: "Depends on root".to_owned(),
            dependency_ids: vec![root.work_item_id],
            priority: 0,
            max_audit_iterations: 1,
        })
        .await
        .unwrap();
    assert!(
        store
            .update_work_item(UpdateWorkItem {
                parent_instance_id: first.instance.instance_id,
                work_item_id: root.work_item_id,
                expected_revision: root.revision,
                title: None,
                summary: None,
                owner_repository_id: None,
                parent_work_item_id: None,
                dependency_ids: Some(vec![dependent.work_item_id]),
                priority: None,
                status: None,
                implementation_artifact_id: None,
            })
            .await
            .is_err()
    );

    let (one, two) = tokio::join!(
        store.claim_work_item(first.instance.instance_id, root.work_item_id, root.revision),
        store.claim_work_item(
            second.instance.instance_id,
            root.work_item_id,
            root.revision
        ),
    );
    assert_ne!(one.is_ok(), two.is_ok());
    let (claimed, loser, takeover_instance_id) = match (one, two) {
        (Ok(claimed), Err(loser)) => (claimed, loser, second.instance.instance_id),
        (Err(loser), Ok(claimed)) => (claimed, loser, first.instance.instance_id),
        _ => unreachable!("exactly one concurrent work-item claim succeeds"),
    };
    assert!(matches!(loser, StoreError::RevisionConflict { .. }));
    assert!(matches!(
        store
            .claim_work_item(takeover_instance_id, root.work_item_id, claimed.revision)
            .await,
        Err(StoreError::ClaimConflict {
            resource_type: "work_item",
            retry_after_seconds: Some(_),
            ..
        })
    ));
    assert!(matches!(
        store
            .update_work_item(UpdateWorkItem {
                parent_instance_id: takeover_instance_id,
                work_item_id: root.work_item_id,
                expected_revision: claimed.revision,
                title: None,
                summary: None,
                owner_repository_id: None,
                parent_work_item_id: None,
                dependency_ids: None,
                priority: None,
                status: Some(WorkItemStatus::Cancelled),
                implementation_artifact_id: None,
            })
            .await,
        Err(StoreError::Conflict(_))
    ));
    let short_expiry =
        (Utc::now() + chrono::Duration::seconds(2)).to_rfc3339_opts(SecondsFormat::Micros, true);
    sqlx::query("UPDATE work_items SET claim_expires_at = ? WHERE work_item_id = ?")
        .bind(&short_expiry)
        .bind(root.work_item_id.to_string())
        .execute(&store.pool)
        .await
        .unwrap();
    store
        .heartbeat(claimed.assignee_instance_id.unwrap())
        .await
        .unwrap();
    assert!(
        store
            .get_work_item(root.work_item_id)
            .await
            .unwrap()
            .claim_expires_at
            > Some(short_expiry)
    );
    sqlx::query(
        "UPDATE work_items SET status = 'in_progress', claim_expires_at = ? WHERE work_item_id = ?",
    )
    .bind("2000-01-01T00:00:00.000000Z")
    .bind(root.work_item_id.to_string())
    .execute(&store.pool)
    .await
    .unwrap();
    let taken_over = store
        .claim_work_item(takeover_instance_id, root.work_item_id, claimed.revision)
        .await
        .unwrap();
    assert_eq!(taken_over.assignee_instance_id, Some(takeover_instance_id));
    assert_eq!(taken_over.status, WorkItemStatus::Claimed);

    store.pool.close().await;
    remove_database(&path);
}

#[tokio::test]
async fn work_item_pages_are_bounded_and_reads_keep_revision_with_dependencies() {
    let (store, path) = test_store(90).await;
    let connected = store
        .connect_orchestrator(connect_command(
            "consistent-work",
            "backend",
            "C:/work/backend",
        ))
        .await
        .unwrap();
    let feature = store
        .create_feature(CreateFeature {
            parent_instance_id: connected.instance.instance_id,
            title: "Consistent reads".to_owned(),
            summary: "Exercise paging and snapshots".to_owned(),
        })
        .await
        .unwrap();
    let mut created = Vec::new();
    for priority in [3, 2, 1] {
        created.push(
            store
                .create_work_item(CreateWorkItem {
                    parent_instance_id: connected.instance.instance_id,
                    feature_id: feature.feature_id,
                    parent_work_item_id: None,
                    owner_repository_id: connected.repository.repository_id,
                    title: format!("Priority {priority}"),
                    summary: "Paged item".to_owned(),
                    dependency_ids: Vec::new(),
                    priority,
                    max_audit_iterations: 1,
                })
                .await
                .unwrap(),
        );
    }
    let first_page = store
        .list_work_items(feature.feature_id, None, 2)
        .await
        .unwrap();
    assert_eq!(first_page.items.len(), 2);
    assert!(first_page.has_more);
    assert_eq!(first_page.items[0].priority, 3);
    let second_page = store
        .list_work_items(feature.feature_id, first_page.next_cursor, 2)
        .await
        .unwrap();
    assert_eq!(second_page.items.len(), 1);
    assert!(!second_page.has_more);
    assert_eq!(second_page.items[0].work_item_id, created[2].work_item_id);

    let dependency_one = created[0].work_item_id;
    let dependency_two = created[1].work_item_id;
    let observed = created[2].work_item_id;
    sqlx::query("UPDATE work_items SET status = 'completed' WHERE work_item_id IN (?, ?)")
        .bind(dependency_one.to_string())
        .bind(dependency_two.to_string())
        .execute(&store.pool)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO work_item_dependencies(work_item_id, dependency_work_item_id) VALUES (?, ?)",
    )
    .bind(observed.to_string())
    .bind(dependency_one.to_string())
    .execute(&store.pool)
    .await
    .unwrap();

    let writer_pool = store.pool.clone();
    let writer = async move {
        for revision in 2..=200_i64 {
            let dependency = if revision % 2 == 0 {
                dependency_two
            } else {
                dependency_one
            };
            let mut transaction = writer_pool.begin().await.unwrap();
            sqlx::query("UPDATE work_items SET revision = ? WHERE work_item_id = ?")
                .bind(revision)
                .bind(observed.to_string())
                .execute(&mut *transaction)
                .await
                .unwrap();
            sqlx::query("DELETE FROM work_item_dependencies WHERE work_item_id = ?")
                .bind(observed.to_string())
                .execute(&mut *transaction)
                .await
                .unwrap();
            sqlx::query(
                "INSERT INTO work_item_dependencies(work_item_id, dependency_work_item_id) VALUES (?, ?)",
            )
            .bind(observed.to_string())
            .bind(dependency.to_string())
            .execute(&mut *transaction)
            .await
            .unwrap();
            transaction.commit().await.unwrap();
            tokio::task::yield_now().await;
        }
    };
    let reader_store = store.clone();
    let reader = async move {
        for _ in 0..400 {
            let item = reader_store.get_work_item(observed).await.unwrap();
            let expected = if item.revision % 2 == 0 {
                dependency_two
            } else {
                dependency_one
            };
            assert_eq!(item.dependency_ids, vec![expected]);
            tokio::task::yield_now().await;
        }
    };
    tokio::join!(writer, reader);

    store.pool.close().await;
    remove_database(&path);
}
