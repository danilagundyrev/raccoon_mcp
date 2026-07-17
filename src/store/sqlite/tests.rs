use std::{fs, path::PathBuf, time::Duration};

use serde_json::json;
use uuid::Uuid;

use super::*;
use crate::domain::{
    AnswerRequest, AskRepository, CancelFeature, CompleteFeature, FailRequest, FeatureStatus,
    RequestStatus, WorkItemId, WorkerRole,
};

fn temp_database_path() -> PathBuf {
    std::env::temp_dir()
        .join("raccoon-mcp-tests")
        .join(format!("{}.sqlite3", Uuid::new_v4()))
}

async fn test_store(lease_seconds: u16) -> (SqliteStore, PathBuf) {
    let path = temp_database_path();
    let store = SqliteStore::open(&path, 2, lease_seconds, 3600, 600, 3600)
        .await
        .unwrap();
    (store, path)
}

async fn event_recipients(
    store: &SqliteStore,
    kind: EventKind,
    entity_type: EventEntityType,
    entity_id: &str,
) -> Vec<RepositoryId> {
    let rows = sqlx::query(
        "SELECT recipient_repository_id FROM events
         WHERE kind = ? AND entity_type = ? AND entity_id = ?",
    )
    .bind(kind.as_str())
    .bind(entity_type.as_str())
    .bind(entity_id)
    .fetch_all(&store.pool)
    .await
    .unwrap();
    rows.iter()
        .map(|row| RepositoryId::parse(row.try_get("recipient_repository_id").unwrap()).unwrap())
        .collect()
}

fn assert_exact_recipients(actual: &[RepositoryId], expected: &[RepositoryId]) {
    assert_eq!(actual.len(), expected.len(), "unexpected recipient count");
    for repository_id in expected {
        assert!(
            actual.contains(repository_id),
            "missing event recipient {repository_id}"
        );
    }
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

fn audit_outcome(work_item_id: WorkItemId, verdict: AuditVerdict) -> AuditOutcomeInput {
    AuditOutcomeInput {
        work_item_id,
        verdict,
        summary: format!("Structured verdict for {work_item_id}"),
    }
}

async fn create_claimed_audit_item(
    store: &SqliteStore,
    connected: &ConnectResult,
    feature_id: FeatureId,
    scope: Vec<WorkItemId>,
    label: &str,
) -> WorkItem {
    let item = store
        .create_work_item(CreateWorkItem {
            parent_instance_id: connected.instance.instance_id,
            feature_id,
            parent_work_item_id: None,
            owner_repository_id: connected.repository.repository_id,
            title: format!("Audit {label}"),
            summary: format!("Independent review of {label}"),
            kind: WorkItemKind::Audit,
            audit_scope_work_item_ids: scope,
            addresses_audit_run_id: None,
            dependency_ids: Vec::new(),
            priority: 0,
            max_audit_iterations: 1,
        })
        .await
        .unwrap();
    store
        .claim_work_item(
            connected.instance.instance_id,
            item.work_item_id,
            item.revision,
        )
        .await
        .unwrap()
}

async fn create_auditor_run(
    store: &SqliteStore,
    connected: &ConnectResult,
    audit_item: &WorkItem,
    summary: &str,
) -> WorkerRun {
    store
        .create_worker_run(CreateWorkerRun {
            parent_instance_id: connected.instance.instance_id,
            role: WorkerRole::Auditor,
            task_summary: summary.to_owned(),
            context_refs: json!([]),
            addresses_audit_run_id: None,
            request_id: None,
            work_item_id: Some(audit_item.work_item_id),
        })
        .await
        .unwrap()
}

async fn complete_audit_work_item(
    store: &SqliteStore,
    connected: &ConnectResult,
    audit_item_id: WorkItemId,
) -> WorkItem {
    let current = store.get_work_item(audit_item_id).await.unwrap();
    store
        .update_work_item(UpdateWorkItem {
            parent_instance_id: connected.instance.instance_id,
            work_item_id: audit_item_id,
            expected_revision: current.revision,
            title: None,
            summary: None,
            owner_repository_id: None,
            parent_work_item_id: None,
            dependency_ids: None,
            priority: None,
            status: Some(WorkItemStatus::Completed),
            implementation_artifact_id: None,
        })
        .await
        .unwrap()
}

async fn fail_reopen_and_claim_work_item(
    store: &SqliteStore,
    connected: &ConnectResult,
    work_item_id: WorkItemId,
) -> WorkItem {
    let active = store.get_work_item(work_item_id).await.unwrap();
    let failed = store
        .update_work_item(UpdateWorkItem {
            parent_instance_id: connected.instance.instance_id,
            work_item_id,
            expected_revision: active.revision,
            title: None,
            summary: None,
            owner_repository_id: None,
            parent_work_item_id: None,
            dependency_ids: None,
            priority: None,
            status: Some(WorkItemStatus::Failed),
            implementation_artifact_id: None,
        })
        .await
        .unwrap();
    let ready = store
        .update_work_item(UpdateWorkItem {
            parent_instance_id: connected.instance.instance_id,
            work_item_id,
            expected_revision: failed.revision,
            title: None,
            summary: None,
            owner_repository_id: None,
            parent_work_item_id: None,
            dependency_ids: None,
            priority: None,
            status: Some(WorkItemStatus::Ready),
            implementation_artifact_id: None,
        })
        .await
        .unwrap();
    store
        .claim_work_item(connected.instance.instance_id, work_item_id, ready.revision)
        .await
        .unwrap()
}

async fn feature_with_completed_item_and_audit(
    store: &SqliteStore,
    connected: &ConnectResult,
    label: &str,
) -> (Feature, WorkItem, WorkerRun) {
    let feature = store
        .create_feature(CreateFeature {
            parent_instance_id: connected.instance.instance_id,
            title: format!("{label} feature"),
            summary: "Terminal worker-link regression".to_owned(),
        })
        .await
        .unwrap();
    let item = store
        .create_work_item(CreateWorkItem {
            parent_instance_id: connected.instance.instance_id,
            feature_id: feature.feature_id,
            parent_work_item_id: None,
            owner_repository_id: connected.repository.repository_id,
            title: format!("{label} item"),
            summary: "Completed audit target".to_owned(),
            dependency_ids: Vec::new(),
            priority: 0,
            kind: WorkItemKind::Implementation,
            audit_scope_work_item_ids: Vec::new(),
            addresses_audit_run_id: None,
            max_audit_iterations: 2,
        })
        .await
        .unwrap();
    sqlx::query("UPDATE work_items SET status = 'completed' WHERE work_item_id = ?")
        .bind(item.work_item_id.to_string())
        .execute(&store.pool)
        .await
        .unwrap();
    let audit_item = create_claimed_audit_item(
        store,
        connected,
        feature.feature_id,
        vec![item.work_item_id],
        label,
    )
    .await;
    let audit = create_auditor_run(
        store,
        connected,
        &audit_item,
        &format!("Audit {label} item"),
    )
    .await;
    store
        .activate_worker_run(audit.worker_run_id)
        .await
        .unwrap();
    store
        .publish_message_with_audit_outcomes(
            PublishMessage {
                actor: ActorRef::Worker(audit.worker_run_id),
                kind: MessageKind::Report,
                target: MessageTarget::Broadcast,
                summary: format!("{label} audit report"),
                body: Some("No findings".to_owned()),
                artifact_kind: ArtifactKind::AuditReport,
                artifact_title: None,
                content_type: "text/markdown".to_owned(),
                feature_id: Some(feature.feature_id),
                work_item_id: Some(item.work_item_id),
                request_id: None,
                thread_id: None,
                correlation_id: None,
                in_reply_to: None,
            },
            &[audit_outcome(
                item.work_item_id,
                AuditVerdict::ChangesRequired,
            )],
        )
        .await
        .unwrap();
    let audit = store
        .complete_worker_run(audit.worker_run_id, "Audit complete")
        .await
        .unwrap();
    complete_audit_work_item(store, connected, audit_item.work_item_id).await;
    (feature, item, audit)
}

async fn complete_implementation_work(
    store: &SqliteStore,
    connected: &ConnectResult,
    feature_id: FeatureId,
    item: &WorkItem,
) -> WorkItem {
    let claimed = store
        .claim_work_item(
            connected.instance.instance_id,
            item.work_item_id,
            item.revision,
        )
        .await
        .unwrap();
    let worker = store
        .create_worker_run(CreateWorkerRun {
            parent_instance_id: connected.instance.instance_id,
            role: WorkerRole::Executor,
            task_summary: format!("Implement {}", item.title),
            context_refs: json!([]),
            addresses_audit_run_id: None,
            request_id: None,
            work_item_id: Some(item.work_item_id),
        })
        .await
        .unwrap();
    store
        .activate_worker_run(worker.worker_run_id)
        .await
        .unwrap();
    let report = store
        .publish_message(PublishMessage {
            actor: ActorRef::Worker(worker.worker_run_id),
            kind: MessageKind::Report,
            target: MessageTarget::Broadcast,
            summary: format!("{} implemented", item.title),
            body: Some("Implementation report".to_owned()),
            artifact_kind: ArtifactKind::ImplementationReport,
            artifact_title: None,
            content_type: "text/markdown".to_owned(),
            feature_id: Some(feature_id),
            work_item_id: Some(item.work_item_id),
            request_id: None,
            thread_id: None,
            correlation_id: None,
            in_reply_to: None,
        })
        .await
        .unwrap()
        .artifact
        .unwrap();
    store
        .complete_worker_run(worker.worker_run_id, "Implementation complete")
        .await
        .unwrap();
    let current = store.get_work_item(item.work_item_id).await.unwrap();
    assert!(current.revision > claimed.revision);
    store
        .update_work_item(UpdateWorkItem {
            parent_instance_id: connected.instance.instance_id,
            work_item_id: item.work_item_id,
            expected_revision: current.revision,
            title: None,
            summary: None,
            owner_repository_id: None,
            parent_work_item_id: None,
            dependency_ids: None,
            priority: None,
            status: Some(WorkItemStatus::Completed),
            implementation_artifact_id: Some(report.artifact_id),
        })
        .await
        .unwrap()
}

async fn complete_audit_with_outcomes(
    store: &SqliteStore,
    connected: &ConnectResult,
    feature_id: FeatureId,
    scope: Vec<WorkItemId>,
    outcomes: &[AuditOutcomeInput],
) -> WorkerRun {
    let audit_item =
        create_claimed_audit_item(store, connected, feature_id, scope, "verification scope").await;
    let audit = create_auditor_run(store, connected, &audit_item, "Independent audit").await;
    store
        .activate_worker_run(audit.worker_run_id)
        .await
        .unwrap();
    store
        .publish_message_with_audit_outcomes(
            PublishMessage {
                actor: ActorRef::Worker(audit.worker_run_id),
                kind: MessageKind::Report,
                target: MessageTarget::Broadcast,
                summary: "Independent audit report".to_owned(),
                body: Some("Audit report".to_owned()),
                artifact_kind: ArtifactKind::AuditReport,
                artifact_title: None,
                content_type: "text/markdown".to_owned(),
                feature_id: Some(feature_id),
                work_item_id: Some(audit_item.work_item_id),
                request_id: None,
                thread_id: None,
                correlation_id: None,
                in_reply_to: None,
            },
            outcomes,
        )
        .await
        .unwrap();
    store
        .complete_worker_run(audit.worker_run_id, "Audit complete")
        .await
        .unwrap();
    complete_audit_work_item(store, connected, audit_item.work_item_id).await;
    audit
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
                            WorkItemStatus::InProgress
                                | WorkItemStatus::Completed
                                | WorkItemStatus::Cancelled
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
async fn feature_completion_is_revisioned_coordinator_owned_and_waits_for_linked_requests() {
    let (store, path) = test_store(90).await;
    let backend = store
        .connect_orchestrator(connect_command(
            "feature-lifecycle",
            "backend",
            "C:/feature/backend",
        ))
        .await
        .unwrap();
    let android = store
        .connect_orchestrator(connect_command(
            "feature-lifecycle",
            "android",
            "C:/feature/android",
        ))
        .await
        .unwrap();
    let feature = store
        .create_feature(CreateFeature {
            parent_instance_id: backend.instance.instance_id,
            title: "Lifecycle".to_owned(),
            summary: "Verify completion".to_owned(),
        })
        .await
        .unwrap();
    assert_eq!(feature.status, FeatureStatus::Active);
    assert_eq!(feature.revision, 1);
    assert_eq!(
        feature.coordinator_repository_id,
        backend.repository.repository_id
    );

    let request = store
        .ask_repository(AskRepository {
            actor: ActorRef::Orchestrator(backend.instance.instance_id),
            target_repository_id: android.repository.repository_id,
            feature_id: Some(feature.feature_id),
            work_item_id: None,
            question_summary: "Confirm Android scope".to_owned(),
            body: None,
            context_refs: json!([]),
            handler_requirement: HandlerRequirement::OrchestratorAllowed,
            answer_requirement: AnswerRequirement::SummaryAllowed,
        })
        .await
        .unwrap();
    assert_exact_recipients(
        &event_recipients(
            &store,
            EventKind::RequestAvailable,
            EventEntityType::Request,
            &request.request_id.to_string(),
        )
        .await,
        &[
            backend.repository.repository_id,
            android.repository.repository_id,
        ],
    );
    assert_eq!(request.feature_id, Some(feature.feature_id));
    assert!(
        store
            .complete_feature(CompleteFeature {
                parent_instance_id: android.instance.instance_id,
                feature_id: feature.feature_id,
                expected_revision: feature.revision,
            })
            .await
            .is_err()
    );
    assert!(
        store
            .complete_feature(CompleteFeature {
                parent_instance_id: backend.instance.instance_id,
                feature_id: feature.feature_id,
                expected_revision: feature.revision,
            })
            .await
            .is_err()
    );

    store
        .claim_request(
            ActorRef::Orchestrator(android.instance.instance_id),
            request.request_id,
        )
        .await
        .unwrap();
    let answer = store
        .answer_request(AnswerRequest {
            actor: ActorRef::Orchestrator(android.instance.instance_id),
            request_id: request.request_id,
            answer_summary: "Scope confirmed".to_owned(),
            body: None,
            content_type: "text/plain".to_owned(),
        })
        .await
        .unwrap();
    assert_eq!(answer.message.feature_id, Some(feature.feature_id));

    let completed = store
        .complete_feature(CompleteFeature {
            parent_instance_id: backend.instance.instance_id,
            feature_id: feature.feature_id,
            expected_revision: feature.revision,
        })
        .await
        .unwrap();
    assert_eq!(completed.status, FeatureStatus::Completed);
    assert_eq!(completed.revision, feature.revision + 1);
    assert!(completed.completed_at.is_some());
    assert!(matches!(
        store
            .complete_feature(CompleteFeature {
                parent_instance_id: backend.instance.instance_id,
                feature_id: feature.feature_id,
                expected_revision: feature.revision,
            })
            .await
            .unwrap_err(),
        StoreError::FeatureRevisionConflict { .. }
    ));

    let events = store
        .wait_for_events(
            ActorRef::Orchestrator(android.instance.instance_id),
            0,
            &[EventKind::FeatureCompleted],
            Duration::ZERO,
            20,
        )
        .await
        .unwrap();
    assert_eq!(events.outcome, WaitOutcome::Events);
    assert_eq!(events.events.len(), 1);
    let updated_events = store
        .wait_for_events(
            ActorRef::Orchestrator(android.instance.instance_id),
            0,
            &[EventKind::FeatureUpdated],
            Duration::ZERO,
            20,
        )
        .await
        .unwrap();
    assert_eq!(updated_events.events.len(), 1);
    let coordinator_events = store
        .wait_for_events(
            ActorRef::Orchestrator(backend.instance.instance_id),
            0,
            &[EventKind::FeatureCompleted],
            Duration::ZERO,
            20,
        )
        .await
        .unwrap();
    assert_eq!(coordinator_events.events.len(), 1);
    let reconnect = store
        .connect_orchestrator(connect_command(
            "feature-lifecycle",
            "backend",
            "C:/feature/backend",
        ))
        .await
        .unwrap();
    assert_eq!(reconnect.active_feature_count, 0);
    let late_joiner = store
        .connect_orchestrator(connect_command(
            "feature-lifecycle",
            "web",
            "C:/feature/web",
        ))
        .await
        .unwrap();
    assert_eq!(late_joiner.active_feature_count, 0);
    assert_eq!(
        store.get_feature(feature.feature_id).await.unwrap().status,
        FeatureStatus::Completed
    );

    store.pool.close().await;
    remove_database(&path);
}

#[tokio::test]
async fn reported_implementation_cannot_be_cancelled_to_bypass_audit() {
    let (store, path) = test_store(90).await;
    let connected = store
        .connect_orchestrator(connect_command(
            "reported-cancellation",
            "backend",
            "C:/reported-cancellation/backend",
        ))
        .await
        .unwrap();
    let feature = store
        .create_feature(CreateFeature {
            parent_instance_id: connected.instance.instance_id,
            title: "Reported implementation".to_owned(),
            summary: "A report must not disappear through cancellation".to_owned(),
        })
        .await
        .unwrap();
    let item = store
        .create_work_item(CreateWorkItem {
            parent_instance_id: connected.instance.instance_id,
            feature_id: feature.feature_id,
            parent_work_item_id: None,
            owner_repository_id: connected.repository.repository_id,
            title: "Implement endpoint".to_owned(),
            summary: "Implementation that has produced evidence".to_owned(),
            kind: WorkItemKind::Implementation,
            audit_scope_work_item_ids: Vec::new(),
            addresses_audit_run_id: None,
            dependency_ids: Vec::new(),
            priority: 0,
            max_audit_iterations: 2,
        })
        .await
        .unwrap();
    let item = store
        .claim_work_item(
            connected.instance.instance_id,
            item.work_item_id,
            item.revision,
        )
        .await
        .unwrap();
    let executor = store
        .create_worker_run(CreateWorkerRun {
            parent_instance_id: connected.instance.instance_id,
            role: WorkerRole::Executor,
            task_summary: "Implement endpoint".to_owned(),
            context_refs: json!([]),
            addresses_audit_run_id: None,
            request_id: None,
            work_item_id: Some(item.work_item_id),
        })
        .await
        .unwrap();
    store
        .activate_worker_run(executor.worker_run_id)
        .await
        .unwrap();
    store
        .publish_message(PublishMessage {
            actor: ActorRef::Worker(executor.worker_run_id),
            kind: MessageKind::Report,
            target: MessageTarget::Broadcast,
            summary: "Endpoint implemented".to_owned(),
            body: Some("Implementation evidence".to_owned()),
            artifact_kind: ArtifactKind::ImplementationReport,
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
        .complete_worker_run(executor.worker_run_id, "Implementation complete")
        .await
        .unwrap();

    let current = store.get_work_item(item.work_item_id).await.unwrap();
    assert!(matches!(
        store
            .update_work_item(UpdateWorkItem {
                parent_instance_id: connected.instance.instance_id,
                work_item_id: item.work_item_id,
                expected_revision: current.revision,
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
    assert!(matches!(
        store
            .complete_feature(CompleteFeature {
                parent_instance_id: connected.instance.instance_id,
                feature_id: feature.feature_id,
                expected_revision: feature.revision,
            })
            .await,
        Err(StoreError::Conflict(_))
    ));

    store.pool.close().await;
    remove_database(&path);
}

#[tokio::test]
async fn feature_waits_for_answered_request_butler_to_finish() {
    let (store, path) = test_store(90).await;
    let coordinator = store
        .connect_orchestrator(connect_command(
            "feature-butler",
            "backend",
            "C:/feature-butler/backend",
        ))
        .await
        .unwrap();
    let target = store
        .connect_orchestrator(connect_command(
            "feature-butler",
            "android",
            "C:/feature-butler/android",
        ))
        .await
        .unwrap();
    let feature = store
        .create_feature(CreateFeature {
            parent_instance_id: coordinator.instance.instance_id,
            title: "Butler lifecycle".to_owned(),
            summary: "The child must finish before the feature".to_owned(),
        })
        .await
        .unwrap();
    let request = store
        .ask_repository(AskRepository {
            actor: ActorRef::Orchestrator(coordinator.instance.instance_id),
            target_repository_id: target.repository.repository_id,
            feature_id: Some(feature.feature_id),
            work_item_id: None,
            question_summary: "Inspect the Android contract".to_owned(),
            body: None,
            context_refs: json!([]),
            handler_requirement: HandlerRequirement::ButlerRequired,
            answer_requirement: AnswerRequirement::SummaryAllowed,
        })
        .await
        .unwrap();
    let butler = store
        .create_worker_run(CreateWorkerRun {
            parent_instance_id: target.instance.instance_id,
            role: WorkerRole::Butler,
            task_summary: "Inspect the Android contract".to_owned(),
            context_refs: json!([]),
            addresses_audit_run_id: None,
            request_id: Some(request.request_id),
            work_item_id: None,
        })
        .await
        .unwrap();
    let butler = store
        .activate_worker_run(butler.worker_run_id)
        .await
        .unwrap();
    store
        .claim_request(ActorRef::Worker(butler.worker_run_id), request.request_id)
        .await
        .unwrap();
    store
        .answer_request(AnswerRequest {
            actor: ActorRef::Worker(butler.worker_run_id),
            request_id: request.request_id,
            answer_summary: "Android contract inspected".to_owned(),
            body: None,
            content_type: "text/plain".to_owned(),
        })
        .await
        .unwrap();

    assert!(matches!(
        store
            .complete_feature(CompleteFeature {
                parent_instance_id: coordinator.instance.instance_id,
                feature_id: feature.feature_id,
                expected_revision: feature.revision,
            })
            .await,
        Err(StoreError::Conflict(_))
    ));
    store
        .complete_worker_run(butler.worker_run_id, "Request answered")
        .await
        .unwrap();
    assert_eq!(
        store
            .complete_feature(CompleteFeature {
                parent_instance_id: coordinator.instance.instance_id,
                feature_id: feature.feature_id,
                expected_revision: feature.revision,
            })
            .await
            .unwrap()
            .status,
        FeatureStatus::Completed
    );

    store.pool.close().await;
    remove_database(&path);
}

#[tokio::test]
async fn repository_request_infers_and_validates_work_item_feature_scope() {
    let (store, path) = test_store(90).await;
    let backend = store
        .connect_orchestrator(connect_command(
            "request-scope",
            "backend",
            "C:/scope/backend",
        ))
        .await
        .unwrap();
    let android = store
        .connect_orchestrator(connect_command(
            "request-scope",
            "android",
            "C:/scope/android",
        ))
        .await
        .unwrap();
    let feature = store
        .create_feature(CreateFeature {
            parent_instance_id: backend.instance.instance_id,
            title: "Scoped request".to_owned(),
            summary: "Infer the feature from work".to_owned(),
        })
        .await
        .unwrap();
    let other_feature = store
        .create_feature(CreateFeature {
            parent_instance_id: backend.instance.instance_id,
            title: "Other feature".to_owned(),
            summary: "Must not accept the work link".to_owned(),
        })
        .await
        .unwrap();
    let item = store
        .create_work_item(CreateWorkItem {
            parent_instance_id: backend.instance.instance_id,
            feature_id: feature.feature_id,
            parent_work_item_id: None,
            owner_repository_id: backend.repository.repository_id,
            title: "Backend contract".to_owned(),
            summary: "Explain the contract".to_owned(),
            dependency_ids: Vec::new(),
            priority: 0,
            kind: WorkItemKind::Implementation,
            audit_scope_work_item_ids: Vec::new(),
            addresses_audit_run_id: None,
            max_audit_iterations: 1,
        })
        .await
        .unwrap();
    let request = store
        .ask_repository(AskRepository {
            actor: ActorRef::Orchestrator(backend.instance.instance_id),
            target_repository_id: android.repository.repository_id,
            feature_id: None,
            work_item_id: Some(item.work_item_id),
            question_summary: "Confirm the client contract".to_owned(),
            body: None,
            context_refs: json!([]),
            handler_requirement: HandlerRequirement::OrchestratorAllowed,
            answer_requirement: AnswerRequirement::SummaryAllowed,
        })
        .await
        .unwrap();
    assert_eq!(request.feature_id, Some(feature.feature_id));
    assert_eq!(request.work_item_id, Some(item.work_item_id));
    assert!(
        store
            .ask_repository(AskRepository {
                actor: ActorRef::Orchestrator(backend.instance.instance_id),
                target_repository_id: android.repository.repository_id,
                feature_id: Some(other_feature.feature_id),
                work_item_id: Some(item.work_item_id),
                question_summary: "Invalid scope".to_owned(),
                body: None,
                context_refs: json!([]),
                handler_requirement: HandlerRequirement::OrchestratorAllowed,
                answer_requirement: AnswerRequirement::SummaryAllowed,
            })
            .await
            .is_err()
    );

    store.pool.close().await;
    remove_database(&path);
}

#[tokio::test]
async fn feature_cancellation_cancels_unclaimed_items_but_rejects_active_work() {
    let (store, path) = test_store(90).await;
    let backend = store
        .connect_orchestrator(connect_command(
            "feature-cancel",
            "backend",
            "C:/cancel/backend",
        ))
        .await
        .unwrap();
    let feature = store
        .create_feature(CreateFeature {
            parent_instance_id: backend.instance.instance_id,
            title: "Cancellation".to_owned(),
            summary: "Cancel idle work".to_owned(),
        })
        .await
        .unwrap();
    let item = store
        .create_work_item(CreateWorkItem {
            parent_instance_id: backend.instance.instance_id,
            feature_id: feature.feature_id,
            parent_work_item_id: None,
            owner_repository_id: backend.repository.repository_id,
            title: "Idle item".to_owned(),
            summary: "Not started".to_owned(),
            dependency_ids: Vec::new(),
            priority: 0,
            kind: WorkItemKind::Implementation,
            audit_scope_work_item_ids: Vec::new(),
            addresses_audit_run_id: None,
            max_audit_iterations: 1,
        })
        .await
        .unwrap();
    let cancelled = store
        .cancel_feature(CancelFeature {
            parent_instance_id: backend.instance.instance_id,
            feature_id: feature.feature_id,
            expected_revision: feature.revision,
        })
        .await
        .unwrap();
    assert_eq!(cancelled.status, FeatureStatus::Cancelled);
    assert!(cancelled.cancelled_at.is_some());
    assert_eq!(
        store.get_work_item(item.work_item_id).await.unwrap().status,
        WorkItemStatus::Cancelled
    );

    let active_feature = store
        .create_feature(CreateFeature {
            parent_instance_id: backend.instance.instance_id,
            title: "Active cancellation".to_owned(),
            summary: "Reject active work".to_owned(),
        })
        .await
        .unwrap();
    let active_item = store
        .create_work_item(CreateWorkItem {
            parent_instance_id: backend.instance.instance_id,
            feature_id: active_feature.feature_id,
            parent_work_item_id: None,
            owner_repository_id: backend.repository.repository_id,
            title: "Claimed item".to_owned(),
            summary: "Already claimed".to_owned(),
            dependency_ids: Vec::new(),
            priority: 0,
            kind: WorkItemKind::Implementation,
            audit_scope_work_item_ids: Vec::new(),
            addresses_audit_run_id: None,
            max_audit_iterations: 1,
        })
        .await
        .unwrap();
    store
        .claim_work_item(
            backend.instance.instance_id,
            active_item.work_item_id,
            active_item.revision,
        )
        .await
        .unwrap();
    assert!(
        store
            .cancel_feature(CancelFeature {
                parent_instance_id: backend.instance.instance_id,
                feature_id: active_feature.feature_id,
                expected_revision: active_feature.revision,
            })
            .await
            .is_err()
    );

    store.pool.close().await;
    remove_database(&path);
}

#[tokio::test]
async fn terminal_features_reject_new_audit_and_remediation_workers() {
    let (store, path) = test_store(90).await;
    let connected = store
        .connect_orchestrator(connect_command(
            "terminal-worker-links",
            "backend",
            "C:/terminal/backend",
        ))
        .await
        .unwrap();

    for terminal_status in [FeatureStatus::Completed, FeatureStatus::Cancelled] {
        let label = terminal_status.as_str();
        let (feature, item, audit) =
            feature_with_completed_item_and_audit(&store, &connected, label).await;
        match terminal_status {
            FeatureStatus::Completed => {
                store
                    .complete_feature(CompleteFeature {
                        parent_instance_id: connected.instance.instance_id,
                        feature_id: feature.feature_id,
                        expected_revision: feature.revision,
                    })
                    .await
                    .unwrap();
            }
            FeatureStatus::Cancelled => {
                store
                    .cancel_feature(CancelFeature {
                        parent_instance_id: connected.instance.instance_id,
                        feature_id: feature.feature_id,
                        expected_revision: feature.revision,
                    })
                    .await
                    .unwrap();
            }
            FeatureStatus::Active => unreachable!(),
        }

        let new_audit = store
            .create_work_item(CreateWorkItem {
                parent_instance_id: connected.instance.instance_id,
                feature_id: feature.feature_id,
                parent_work_item_id: None,
                owner_repository_id: connected.repository.repository_id,
                title: "Audit terminal feature".to_owned(),
                summary: "Must be rejected".to_owned(),
                kind: WorkItemKind::Audit,
                audit_scope_work_item_ids: vec![item.work_item_id],
                addresses_audit_run_id: None,
                dependency_ids: Vec::new(),
                priority: 0,
                max_audit_iterations: 1,
            })
            .await;
        assert!(matches!(new_audit, Err(StoreError::Conflict(_))));

        let remediation = store
            .create_work_item(CreateWorkItem {
                parent_instance_id: connected.instance.instance_id,
                feature_id: feature.feature_id,
                parent_work_item_id: None,
                owner_repository_id: connected.repository.repository_id,
                title: "Remediate terminal feature".to_owned(),
                summary: "Must be rejected".to_owned(),
                kind: WorkItemKind::Remediation,
                audit_scope_work_item_ids: Vec::new(),
                addresses_audit_run_id: Some(audit.worker_run_id),
                dependency_ids: Vec::new(),
                priority: 0,
                max_audit_iterations: 1,
            })
            .await;
        assert!(matches!(remediation, Err(StoreError::Conflict(_))));
    }

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
            addresses_audit_run_id: None,
            request_id: None,
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
            Duration::ZERO,
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
async fn implementation_worker_attempts_are_sequential_and_retryable() {
    let (store, path) = test_store(90).await;
    let connected = store
        .connect_orchestrator(connect_command(
            "implementation-attempts",
            "backend",
            "C:/implementation-attempts/backend",
        ))
        .await
        .unwrap();
    let feature = store
        .create_feature(CreateFeature {
            parent_instance_id: connected.instance.instance_id,
            title: "Sequential attempts".to_owned(),
            summary: "Only one implementation executor may run at once".to_owned(),
        })
        .await
        .unwrap();
    let item = store
        .create_work_item(CreateWorkItem {
            parent_instance_id: connected.instance.instance_id,
            feature_id: feature.feature_id,
            parent_work_item_id: None,
            owner_repository_id: connected.repository.repository_id,
            title: "Implement sequentially".to_owned(),
            summary: "Retry only after a terminal unsuccessful attempt".to_owned(),
            kind: WorkItemKind::Implementation,
            audit_scope_work_item_ids: Vec::new(),
            addresses_audit_run_id: None,
            dependency_ids: Vec::new(),
            priority: 0,
            max_audit_iterations: 2,
        })
        .await
        .unwrap();
    let item = store
        .claim_work_item(
            connected.instance.instance_id,
            item.work_item_id,
            item.revision,
        )
        .await
        .unwrap();
    let worker_command = || CreateWorkerRun {
        parent_instance_id: connected.instance.instance_id,
        role: WorkerRole::Executor,
        task_summary: "Implementation attempt".to_owned(),
        context_refs: json!([]),
        addresses_audit_run_id: None,
        request_id: None,
        work_item_id: Some(item.work_item_id),
    };
    let first = store.create_worker_run(worker_command()).await.unwrap();
    store
        .activate_worker_run(first.worker_run_id)
        .await
        .unwrap();
    assert!(matches!(
        store.create_worker_run(worker_command()).await,
        Err(StoreError::Conflict(_))
    ));

    store
        .fail_worker_run(first.worker_run_id, "Transient failure")
        .await
        .unwrap();
    fail_reopen_and_claim_work_item(&store, &connected, item.work_item_id).await;
    let retry = store.create_worker_run(worker_command()).await.unwrap();
    assert_ne!(retry.worker_run_id, first.worker_run_id);
    assert!(matches!(
        store.create_worker_run(worker_command()).await,
        Err(StoreError::Conflict(_))
    ));

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
            role: WorkerRole::Executor,
            task_summary: "Inspect a fake contract".to_owned(),
            context_refs: json!([]),
            addresses_audit_run_id: None,
            request_id: None,
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
            addresses_audit_run_id: None,
            request_id: None,
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
            role: WorkerRole::Executor,
            task_summary: "Expire before finishing".to_owned(),
            context_refs: json!([]),
            addresses_audit_run_id: None,
            request_id: None,
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
            addresses_audit_run_id: None,
            request_id: None,
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
            addresses_audit_run_id: None,
            request_id: None,
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
            kind: WorkItemKind::Implementation,
            audit_scope_work_item_ids: Vec::new(),
            addresses_audit_run_id: None,
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
            kind: WorkItemKind::Implementation,
            audit_scope_work_item_ids: Vec::new(),
            addresses_audit_run_id: None,
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
    let peer = store
        .connect_orchestrator(connect_command("project", "web", "C:/project/web"))
        .await
        .unwrap();
    let peer_item = store
        .create_work_item(CreateWorkItem {
            parent_instance_id: connected.instance.instance_id,
            feature_id: feature.feature_id,
            parent_work_item_id: None,
            owner_repository_id: peer.repository.repository_id,
            title: "Peer item".to_owned(),
            summary: "Must be reviewed in the peer checkout".to_owned(),
            kind: WorkItemKind::Implementation,
            audit_scope_work_item_ids: Vec::new(),
            addresses_audit_run_id: None,
            dependency_ids: Vec::new(),
            priority: 0,
            max_audit_iterations: 2,
        })
        .await
        .unwrap();
    sqlx::query("UPDATE work_items SET status = 'completed' WHERE work_item_id = ?")
        .bind(peer_item.work_item_id.to_string())
        .execute(&store.pool)
        .await
        .unwrap();
    assert!(matches!(
        store
            .create_work_item(CreateWorkItem {
                parent_instance_id: connected.instance.instance_id,
                feature_id: feature.feature_id,
                parent_work_item_id: None,
                owner_repository_id: connected.repository.repository_id,
                title: "Mixed-repository audit".to_owned(),
                summary: "Must be split by owner".to_owned(),
                kind: WorkItemKind::Audit,
                audit_scope_work_item_ids: vec![first_item, peer_item.work_item_id],
                addresses_audit_run_id: None,
                dependency_ids: Vec::new(),
                priority: 0,
                max_audit_iterations: 1,
            })
            .await,
        Err(StoreError::Conflict(_))
    ));
    let first_audit_item = create_claimed_audit_item(
        &store,
        &connected,
        feature.feature_id,
        vec![first_item, second_item],
        "two completed work items",
    )
    .await;
    assert_eq!(first_audit_item.audit_scope_work_item_ids.len(), 2);
    assert!(
        first_audit_item
            .audit_scope_work_item_ids
            .contains(&first_item)
    );
    assert!(
        first_audit_item
            .audit_scope_work_item_ids
            .contains(&second_item)
    );
    let first_audit = create_auditor_run(
        &store,
        &connected,
        &first_audit_item,
        "Audit two completed work items",
    )
    .await;
    assert_eq!(first_audit.audit_scope.len(), 2);
    assert!(
        first_audit
            .audit_scope
            .iter()
            .all(|item| item.iteration == 1 && item.max_iterations == 2)
    );
    let overlapping = store
        .create_work_item(CreateWorkItem {
            parent_instance_id: connected.instance.instance_id,
            feature_id: feature.feature_id,
            parent_work_item_id: None,
            owner_repository_id: connected.repository.repository_id,
            title: "Overlapping audit".to_owned(),
            summary: "Must wait for the first audit".to_owned(),
            kind: WorkItemKind::Audit,
            audit_scope_work_item_ids: vec![first_item, second_item],
            addresses_audit_run_id: None,
            dependency_ids: Vec::new(),
            priority: 0,
            max_audit_iterations: 1,
        })
        .await;
    assert!(matches!(overlapping, Err(StoreError::Conflict(_))));
    store
        .activate_worker_run(first_audit.worker_run_id)
        .await
        .unwrap();
    let audit_report = store
        .publish_message_with_audit_outcomes(
            PublishMessage {
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
            },
            &[
                audit_outcome(first_item, AuditVerdict::ChangesRequired),
                audit_outcome(second_item, AuditVerdict::ChangesRequired),
            ],
        )
        .await
        .unwrap()
        .artifact
        .unwrap();
    assert!(matches!(
        store
            .publish_message_with_audit_outcomes(
                PublishMessage {
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
                },
                &[
                    audit_outcome(first_item, AuditVerdict::ChangesRequired),
                    audit_outcome(second_item, AuditVerdict::ChangesRequired),
                ]
            )
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
    complete_audit_work_item(&store, &connected, first_audit_item.work_item_id).await;

    let remediation_item = store
        .create_work_item(CreateWorkItem {
            parent_instance_id: connected.instance.instance_id,
            feature_id: feature.feature_id,
            parent_work_item_id: None,
            owner_repository_id: connected.repository.repository_id,
            title: "Address audit findings".to_owned(),
            summary: "Address the complete audit report".to_owned(),
            kind: WorkItemKind::Remediation,
            audit_scope_work_item_ids: Vec::new(),
            addresses_audit_run_id: Some(first_audit.worker_run_id),
            dependency_ids: Vec::new(),
            priority: 0,
            max_audit_iterations: 1,
        })
        .await
        .unwrap();
    let remediation_item = store
        .claim_work_item(
            connected.instance.instance_id,
            remediation_item.work_item_id,
            remediation_item.revision,
        )
        .await
        .unwrap();
    let remediation = store
        .create_worker_run(CreateWorkerRun {
            parent_instance_id: connected.instance.instance_id,
            role: WorkerRole::Executor,
            task_summary: "Address the complete audit report".to_owned(),
            context_refs: json!([]),
            addresses_audit_run_id: Some(first_audit.worker_run_id),
            request_id: None,
            work_item_id: Some(remediation_item.work_item_id),
        })
        .await
        .unwrap();
    assert_eq!(
        remediation.addresses_audit_run_id,
        Some(first_audit.worker_run_id)
    );
    assert!(matches!(
        store
            .create_work_item(CreateWorkItem {
                parent_instance_id: connected.instance.instance_id,
                feature_id: feature.feature_id,
                parent_work_item_id: None,
                owner_repository_id: connected.repository.repository_id,
                title: "Duplicate remediation".to_owned(),
                summary: "Must be rejected".to_owned(),
                kind: WorkItemKind::Remediation,
                audit_scope_work_item_ids: Vec::new(),
                addresses_audit_run_id: Some(first_audit.worker_run_id),
                dependency_ids: Vec::new(),
                priority: 0,
                max_audit_iterations: 1,
            })
            .await,
        Err(StoreError::Conflict(_))
    ));

    assert!(matches!(
        store
            .create_work_item(CreateWorkItem {
                parent_instance_id: connected.instance.instance_id,
                feature_id: feature.feature_id,
                parent_work_item_id: None,
                owner_repository_id: connected.repository.repository_id,
                title: "Premature re-audit".to_owned(),
                summary: "Cannot audit before remediation finishes".to_owned(),
                kind: WorkItemKind::Audit,
                audit_scope_work_item_ids: vec![first_item, second_item],
                addresses_audit_run_id: None,
                dependency_ids: Vec::new(),
                priority: 0,
                max_audit_iterations: 1,
            })
            .await,
        Err(StoreError::Conflict(_))
    ));
    store
        .activate_worker_run(remediation.worker_run_id)
        .await
        .unwrap();
    let remediation_report = store
        .publish_message(PublishMessage {
            actor: ActorRef::Worker(remediation.worker_run_id),
            kind: MessageKind::Report,
            target: MessageTarget::Broadcast,
            summary: "Remediation report".to_owned(),
            body: Some("Addressed audit findings".to_owned()),
            artifact_kind: ArtifactKind::ImplementationReport,
            artifact_title: None,
            content_type: "text/markdown".to_owned(),
            feature_id: Some(feature.feature_id),
            work_item_id: Some(remediation_item.work_item_id),
            request_id: None,
            thread_id: None,
            correlation_id: None,
            in_reply_to: None,
        })
        .await
        .unwrap()
        .artifact
        .unwrap();
    store
        .complete_worker_run(remediation.worker_run_id, "Remediation complete")
        .await
        .unwrap();
    let current_remediation = store
        .get_work_item(remediation_item.work_item_id)
        .await
        .unwrap();
    store
        .update_work_item(UpdateWorkItem {
            parent_instance_id: connected.instance.instance_id,
            work_item_id: remediation_item.work_item_id,
            expected_revision: current_remediation.revision,
            title: None,
            summary: None,
            owner_repository_id: None,
            parent_work_item_id: None,
            dependency_ids: None,
            priority: None,
            status: Some(WorkItemStatus::Completed),
            implementation_artifact_id: Some(remediation_report.artifact_id),
        })
        .await
        .unwrap();

    let second_audit_item = create_claimed_audit_item(
        &store,
        &connected,
        feature.feature_id,
        vec![first_item, second_item],
        "remediation",
    )
    .await;
    let second_audit =
        create_auditor_run(&store, &connected, &second_audit_item, "Audit remediation").await;
    assert!(
        second_audit
            .audit_scope
            .iter()
            .all(|item| item.iteration == 2)
    );
    assert!(matches!(
        store
            .create_work_item(CreateWorkItem {
                parent_instance_id: connected.instance.instance_id,
                feature_id: feature.feature_id,
                parent_work_item_id: None,
                owner_repository_id: connected.repository.repository_id,
                title: "Third audit".to_owned(),
                summary: "Depth is exhausted".to_owned(),
                kind: WorkItemKind::Audit,
                audit_scope_work_item_ids: vec![first_item, second_item],
                addresses_audit_run_id: None,
                dependency_ids: Vec::new(),
                priority: 0,
                max_audit_iterations: 1,
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
            kind: WorkItemKind::Implementation,
            audit_scope_work_item_ids: Vec::new(),
            addresses_audit_run_id: None,
            max_audit_iterations: 2,
        })
        .await
        .unwrap();
    sqlx::query("UPDATE work_items SET status = 'completed' WHERE work_item_id = ?")
        .bind(item.work_item_id.to_string())
        .execute(&store.pool)
        .await
        .unwrap();
    let first_audit_item = create_claimed_audit_item(
        &store,
        &connected,
        feature.feature_id,
        vec![item.work_item_id],
        "completed item",
    )
    .await;
    let first = create_auditor_run(
        &store,
        &connected,
        &first_audit_item,
        "Audit completed item",
    )
    .await;
    store
        .activate_worker_run(first.worker_run_id)
        .await
        .unwrap();
    store
        .publish_message_with_audit_outcomes(
            PublishMessage {
                actor: ActorRef::Worker(first.worker_run_id),
                kind: MessageKind::Report,
                target: MessageTarget::Broadcast,
                summary: "Audit blocked".to_owned(),
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
            },
            &[audit_outcome(item.work_item_id, AuditVerdict::Blocked)],
        )
        .await
        .unwrap();
    store
        .complete_worker_run(first.worker_run_id, "Audited")
        .await
        .unwrap();
    complete_audit_work_item(&store, &connected, first_audit_item.work_item_id).await;
    store
        .create_work_item(CreateWorkItem {
            parent_instance_id: connected.instance.instance_id,
            feature_id: feature.feature_id,
            parent_work_item_id: None,
            owner_repository_id: connected.repository.repository_id,
            title: "Second audit".to_owned(),
            summary: "Later iteration".to_owned(),
            kind: WorkItemKind::Audit,
            audit_scope_work_item_ids: vec![item.work_item_id],
            addresses_audit_run_id: None,
            dependency_ids: Vec::new(),
            priority: 0,
            max_audit_iterations: 1,
        })
        .await
        .unwrap();

    assert!(matches!(
        store
            .create_work_item(CreateWorkItem {
                parent_instance_id: connected.instance.instance_id,
                feature_id: feature.feature_id,
                parent_work_item_id: None,
                owner_repository_id: connected.repository.repository_id,
                title: "Late remediation".to_owned(),
                summary: "Must be rejected".to_owned(),
                kind: WorkItemKind::Remediation,
                audit_scope_work_item_ids: Vec::new(),
                addresses_audit_run_id: Some(first.worker_run_id),
                dependency_ids: Vec::new(),
                priority: 0,
                max_audit_iterations: 1,
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
            kind: WorkItemKind::Implementation,
            audit_scope_work_item_ids: Vec::new(),
            addresses_audit_run_id: None,
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
            .create_work_item(CreateWorkItem {
                parent_instance_id: first.instance.instance_id,
                feature_id: foreign_feature.feature_id,
                parent_work_item_id: None,
                owner_repository_id: first.repository.repository_id,
                title: "Cross-project audit".to_owned(),
                summary: "Must be rejected".to_owned(),
                kind: WorkItemKind::Audit,
                audit_scope_work_item_ids: vec![foreign_item.work_item_id],
                addresses_audit_run_id: None,
                dependency_ids: Vec::new(),
                priority: 0,
                max_audit_iterations: 1,
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
async fn worker_role_must_match_the_claimed_work_item_kind() {
    let (store, path) = test_store(90).await;
    let connected = store
        .connect_orchestrator(connect_command("project", "backend", "C:/project/backend"))
        .await
        .unwrap();
    let feature = store
        .create_feature(CreateFeature {
            parent_instance_id: connected.instance.instance_id,
            title: "Role boundary".to_owned(),
            summary: "Audit cards require auditors".to_owned(),
        })
        .await
        .unwrap();
    let implementation = store
        .create_work_item(CreateWorkItem {
            parent_instance_id: connected.instance.instance_id,
            feature_id: feature.feature_id,
            parent_work_item_id: None,
            owner_repository_id: connected.repository.repository_id,
            title: "Completed implementation".to_owned(),
            summary: "Audit target".to_owned(),
            kind: WorkItemKind::Implementation,
            audit_scope_work_item_ids: Vec::new(),
            addresses_audit_run_id: None,
            dependency_ids: Vec::new(),
            priority: 0,
            max_audit_iterations: 1,
        })
        .await
        .unwrap();
    sqlx::query("UPDATE work_items SET status = 'completed' WHERE work_item_id = ?")
        .bind(implementation.work_item_id.to_string())
        .execute(&store.pool)
        .await
        .unwrap();
    let audit_item = create_claimed_audit_item(
        &store,
        &connected,
        feature.feature_id,
        vec![implementation.work_item_id],
        "role boundary",
    )
    .await;

    assert!(matches!(
        store
            .create_worker_run(CreateWorkerRun {
                parent_instance_id: connected.instance.instance_id,
                role: WorkerRole::Executor,
                task_summary: "Executor cannot claim audit work".to_owned(),
                context_refs: json!([]),
                addresses_audit_run_id: None,
                request_id: None,
                work_item_id: Some(audit_item.work_item_id),
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
                addresses_audit_run_id: None,
                request_id: None,
                work_item_id: None,
            })
            .await,
        Err(StoreError::Conflict(_))
    ));

    store.pool.close().await;
    remove_database(&path);
}

#[tokio::test]
async fn audit_and_remediation_owner_repository_is_immutable() {
    let (store, path) = test_store(90).await;
    let backend = store
        .connect_orchestrator(connect_command("owner-lock", "backend", "C:/owner/backend"))
        .await
        .unwrap();
    let web = store
        .connect_orchestrator(connect_command("owner-lock", "web", "C:/owner/web"))
        .await
        .unwrap();

    let (audited_feature, _, audit_run) =
        feature_with_completed_item_and_audit(&store, &backend, "remediation owner").await;
    let remediation = store
        .create_work_item(CreateWorkItem {
            parent_instance_id: backend.instance.instance_id,
            feature_id: audited_feature.feature_id,
            parent_work_item_id: None,
            owner_repository_id: backend.repository.repository_id,
            title: "Repository-local remediation".to_owned(),
            summary: "Must remain in backend".to_owned(),
            kind: WorkItemKind::Remediation,
            audit_scope_work_item_ids: Vec::new(),
            addresses_audit_run_id: Some(audit_run.worker_run_id),
            dependency_ids: Vec::new(),
            priority: 0,
            max_audit_iterations: 1,
        })
        .await
        .unwrap();
    assert!(matches!(
        store
            .update_work_item(UpdateWorkItem {
                parent_instance_id: backend.instance.instance_id,
                work_item_id: remediation.work_item_id,
                expected_revision: remediation.revision,
                title: None,
                summary: None,
                owner_repository_id: Some(web.repository.repository_id),
                parent_work_item_id: None,
                dependency_ids: None,
                priority: None,
                status: None,
                implementation_artifact_id: None,
            })
            .await,
        Err(StoreError::Conflict(_))
    ));
    assert!(
        sqlx::query("UPDATE work_items SET owner_repository_id = ? WHERE work_item_id = ?")
            .bind(web.repository.repository_id.to_string())
            .bind(remediation.work_item_id.to_string())
            .execute(&store.pool)
            .await
            .is_err()
    );

    let feature = store
        .create_feature(CreateFeature {
            parent_instance_id: backend.instance.instance_id,
            title: "Audit owner".to_owned(),
            summary: "Audit must stay beside its code".to_owned(),
        })
        .await
        .unwrap();
    let implementation = store
        .create_work_item(CreateWorkItem {
            parent_instance_id: backend.instance.instance_id,
            feature_id: feature.feature_id,
            parent_work_item_id: None,
            owner_repository_id: backend.repository.repository_id,
            title: "Backend implementation".to_owned(),
            summary: "Completed target".to_owned(),
            kind: WorkItemKind::Implementation,
            audit_scope_work_item_ids: Vec::new(),
            addresses_audit_run_id: None,
            dependency_ids: Vec::new(),
            priority: 0,
            max_audit_iterations: 1,
        })
        .await
        .unwrap();
    sqlx::query("UPDATE work_items SET status = 'completed' WHERE work_item_id = ?")
        .bind(implementation.work_item_id.to_string())
        .execute(&store.pool)
        .await
        .unwrap();
    let audit_item = store
        .create_work_item(CreateWorkItem {
            parent_instance_id: backend.instance.instance_id,
            feature_id: feature.feature_id,
            parent_work_item_id: None,
            owner_repository_id: backend.repository.repository_id,
            title: "Backend audit".to_owned(),
            summary: "Repository-local audit".to_owned(),
            kind: WorkItemKind::Audit,
            audit_scope_work_item_ids: vec![implementation.work_item_id],
            addresses_audit_run_id: None,
            dependency_ids: Vec::new(),
            priority: 0,
            max_audit_iterations: 1,
        })
        .await
        .unwrap();
    assert!(matches!(
        store
            .update_work_item(UpdateWorkItem {
                parent_instance_id: backend.instance.instance_id,
                work_item_id: audit_item.work_item_id,
                expected_revision: audit_item.revision,
                title: None,
                summary: None,
                owner_repository_id: Some(web.repository.repository_id),
                parent_work_item_id: None,
                dependency_ids: None,
                priority: None,
                status: None,
                implementation_artifact_id: None,
            })
            .await,
        Err(StoreError::Conflict(_))
    ));
    assert!(
        sqlx::query("UPDATE work_items SET owner_repository_id = ? WHERE work_item_id = ?")
            .bind(web.repository.repository_id.to_string())
            .bind(audit_item.work_item_id.to_string())
            .execute(&store.pool)
            .await
            .is_err()
    );

    store.pool.close().await;
    remove_database(&path);
}

#[tokio::test]
async fn abandoned_auditor_can_be_retried_on_the_same_audit_card() {
    let (store, path) = test_store(90).await;
    let connected = store
        .connect_orchestrator(connect_command(
            "audit-retry",
            "backend",
            "C:/retry/backend",
        ))
        .await
        .unwrap();
    let feature = store
        .create_feature(CreateFeature {
            parent_instance_id: connected.instance.instance_id,
            title: "Retry audit".to_owned(),
            summary: "Transient auditor failure".to_owned(),
        })
        .await
        .unwrap();
    let implementation = store
        .create_work_item(CreateWorkItem {
            parent_instance_id: connected.instance.instance_id,
            feature_id: feature.feature_id,
            parent_work_item_id: None,
            owner_repository_id: connected.repository.repository_id,
            title: "Completed implementation".to_owned(),
            summary: "Audit target".to_owned(),
            kind: WorkItemKind::Implementation,
            audit_scope_work_item_ids: Vec::new(),
            addresses_audit_run_id: None,
            dependency_ids: Vec::new(),
            priority: 0,
            max_audit_iterations: 1,
        })
        .await
        .unwrap();
    sqlx::query("UPDATE work_items SET status = 'completed' WHERE work_item_id = ?")
        .bind(implementation.work_item_id.to_string())
        .execute(&store.pool)
        .await
        .unwrap();
    let audit_item = create_claimed_audit_item(
        &store,
        &connected,
        feature.feature_id,
        vec![implementation.work_item_id],
        "retry target",
    )
    .await;
    let first = create_auditor_run(&store, &connected, &audit_item, "First attempt").await;
    store
        .activate_worker_run(first.worker_run_id)
        .await
        .unwrap();
    sqlx::query("UPDATE worker_runs SET lease_expires_at = ? WHERE worker_run_id = ?")
        .bind("1970-01-01T00:00:00Z")
        .bind(first.worker_run_id.to_string())
        .execute(&store.pool)
        .await
        .unwrap();
    sqlx::query("UPDATE work_items SET claim_expires_at = ? WHERE work_item_id = ?")
        .bind("1970-01-01T00:00:00Z")
        .bind(audit_item.work_item_id.to_string())
        .execute(&store.pool)
        .await
        .unwrap();
    store.run_lease_maintenance().await.unwrap();
    assert_eq!(
        store
            .get_worker_run(first.worker_run_id)
            .await
            .unwrap()
            .status,
        WorkerRunStatus::Abandoned
    );
    let expired_item = store.get_work_item(audit_item.work_item_id).await.unwrap();
    let retried_item = store
        .claim_work_item(
            connected.instance.instance_id,
            audit_item.work_item_id,
            expired_item.revision,
        )
        .await
        .unwrap();
    let second = create_auditor_run(&store, &connected, &retried_item, "Second attempt").await;
    assert_ne!(first.worker_run_id, second.worker_run_id);
    assert!(matches!(
        store
            .create_worker_run(CreateWorkerRun {
                parent_instance_id: connected.instance.instance_id,
                role: WorkerRole::Auditor,
                task_summary: "Overlapping attempt".to_owned(),
                context_refs: json!([]),
                addresses_audit_run_id: None,
                request_id: None,
                work_item_id: Some(audit_item.work_item_id),
            })
            .await,
        Err(StoreError::Conflict(_))
    ));

    store.pool.close().await;
    remove_database(&path);
}

#[tokio::test]
async fn failed_remediation_executor_can_be_retried_on_the_same_card() {
    let (store, path) = test_store(90).await;
    let connected = store
        .connect_orchestrator(connect_command(
            "remediation-retry",
            "backend",
            "C:/retry/remediation",
        ))
        .await
        .unwrap();
    let (feature, _, audit_run) =
        feature_with_completed_item_and_audit(&store, &connected, "retry remediation").await;
    let remediation = store
        .create_work_item(CreateWorkItem {
            parent_instance_id: connected.instance.instance_id,
            feature_id: feature.feature_id,
            parent_work_item_id: None,
            owner_repository_id: connected.repository.repository_id,
            title: "Retry remediation".to_owned(),
            summary: "Transient executor failure".to_owned(),
            kind: WorkItemKind::Remediation,
            audit_scope_work_item_ids: Vec::new(),
            addresses_audit_run_id: Some(audit_run.worker_run_id),
            dependency_ids: Vec::new(),
            priority: 0,
            max_audit_iterations: 1,
        })
        .await
        .unwrap();
    let remediation = store
        .claim_work_item(
            connected.instance.instance_id,
            remediation.work_item_id,
            remediation.revision,
        )
        .await
        .unwrap();
    let worker_command = || CreateWorkerRun {
        parent_instance_id: connected.instance.instance_id,
        role: WorkerRole::Executor,
        task_summary: "Remediation attempt".to_owned(),
        context_refs: json!([]),
        addresses_audit_run_id: Some(audit_run.worker_run_id),
        request_id: None,
        work_item_id: Some(remediation.work_item_id),
    };
    let first = store.create_worker_run(worker_command()).await.unwrap();
    store
        .activate_worker_run(first.worker_run_id)
        .await
        .unwrap();
    store
        .fail_worker_run(first.worker_run_id, "Transient failure")
        .await
        .unwrap();
    fail_reopen_and_claim_work_item(&store, &connected, remediation.work_item_id).await;
    let second = store.create_worker_run(worker_command()).await.unwrap();
    assert_ne!(first.worker_run_id, second.worker_run_id);
    assert!(matches!(
        store.create_worker_run(worker_command()).await,
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
            Duration::ZERO,
            20,
        )
        .await
        .unwrap();
    assert_eq!(filtered.outcome, WaitOutcome::Deadline);
    assert_eq!(filtered.next_cursor, 0);

    let batch = store
        .wait_for_events(
            ActorRef::Orchestrator(web.instance.instance_id),
            0,
            &[EventKind::MessageAvailable],
            Duration::ZERO,
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
async fn event_headers_expose_routing_fields_and_support_feature_owner_filters() {
    let (store, path) = test_store(90).await;
    let backend = store
        .connect_orchestrator(connect_command(
            "event-headers",
            "backend",
            "C:/event-headers/backend",
        ))
        .await
        .unwrap();
    let android = store
        .connect_orchestrator(connect_command(
            "event-headers",
            "android",
            "C:/event-headers/android",
        ))
        .await
        .unwrap();
    let web = store
        .connect_orchestrator(connect_command(
            "event-headers",
            "web",
            "C:/event-headers/web",
        ))
        .await
        .unwrap();

    let feature = store
        .create_feature(CreateFeature {
            parent_instance_id: backend.instance.instance_id,
            title: "Indexed event headers".to_owned(),
            summary: "Route without loading every entity".to_owned(),
        })
        .await
        .unwrap();
    assert_exact_recipients(
        &event_recipients(
            &store,
            EventKind::FeatureCreated,
            EventEntityType::Feature,
            &feature.feature_id.to_string(),
        )
        .await,
        &[
            backend.repository.repository_id,
            android.repository.repository_id,
            web.repository.repository_id,
        ],
    );
    let feature_events = store
        .wait_for_events_filtered(
            ActorRef::Orchestrator(backend.instance.instance_id),
            EventWaitOptions {
                cursor: Some(0),
                consumer_key: None,
                kinds: &[EventKind::FeatureCreated],
                feature_id: Some(feature.feature_id),
                owner_repository_id: Some(backend.repository.repository_id),
                timeout: Duration::ZERO,
                limit: 20,
            },
        )
        .await
        .unwrap();
    assert_eq!(feature_events.events.len(), 1);
    let feature_event = &feature_events.events[0];
    assert_eq!(feature_event.feature_id, Some(feature.feature_id));
    assert_eq!(
        feature_event.owner_repository_id,
        Some(backend.repository.repository_id)
    );
    assert_eq!(feature_event.entity_status.as_deref(), Some("active"));
    assert_eq!(feature_event.entity_revision, Some(1));
    assert_eq!(feature_event.work_item_id, None);
    assert_eq!(feature_event.request_id, None);

    let item = store
        .create_work_item(CreateWorkItem {
            parent_instance_id: backend.instance.instance_id,
            feature_id: feature.feature_id,
            parent_work_item_id: None,
            owner_repository_id: android.repository.repository_id,
            title: "Android event owner".to_owned(),
            summary: "Expose owner and revision in the event header".to_owned(),
            kind: WorkItemKind::Implementation,
            audit_scope_work_item_ids: Vec::new(),
            addresses_audit_run_id: None,
            dependency_ids: Vec::new(),
            priority: 0,
            max_audit_iterations: 1,
        })
        .await
        .unwrap();
    assert_exact_recipients(
        &event_recipients(
            &store,
            EventKind::WorkItemCreated,
            EventEntityType::WorkItem,
            &item.work_item_id.to_string(),
        )
        .await,
        &[
            backend.repository.repository_id,
            android.repository.repository_id,
        ],
    );
    let item_events = store
        .wait_for_events_filtered(
            ActorRef::Orchestrator(backend.instance.instance_id),
            EventWaitOptions {
                cursor: Some(feature_events.next_cursor),
                consumer_key: None,
                kinds: &[EventKind::WorkItemCreated],
                feature_id: Some(feature.feature_id),
                owner_repository_id: Some(android.repository.repository_id),
                timeout: Duration::ZERO,
                limit: 20,
            },
        )
        .await
        .unwrap();
    assert_eq!(item_events.events.len(), 1);
    let item_event = &item_events.events[0];
    assert_eq!(item_event.feature_id, Some(feature.feature_id));
    assert_eq!(
        item_event.owner_repository_id,
        Some(android.repository.repository_id)
    );
    assert_eq!(item_event.entity_status.as_deref(), Some("ready"));
    assert_eq!(item_event.entity_revision, Some(item.revision));
    assert_eq!(item_event.work_item_id, Some(item.work_item_id));

    let unrelated_owner = store
        .wait_for_events_filtered(
            ActorRef::Orchestrator(backend.instance.instance_id),
            EventWaitOptions {
                cursor: Some(feature_events.next_cursor),
                consumer_key: None,
                kinds: &[EventKind::WorkItemCreated],
                feature_id: Some(feature.feature_id),
                owner_repository_id: Some(web.repository.repository_id),
                timeout: Duration::ZERO,
                limit: 20,
            },
        )
        .await
        .unwrap();
    assert_eq!(unrelated_owner.outcome, WaitOutcome::Deadline);
    assert!(unrelated_owner.events.is_empty());

    let claimed = store
        .claim_work_item(
            android.instance.instance_id,
            item.work_item_id,
            item.revision,
        )
        .await
        .unwrap();
    let worker = store
        .create_worker_run(CreateWorkerRun {
            parent_instance_id: android.instance.instance_id,
            role: WorkerRole::Executor,
            task_summary: "Verify enriched worker event".to_owned(),
            context_refs: json!([]),
            addresses_audit_run_id: None,
            work_item_id: Some(claimed.work_item_id),
            request_id: None,
        })
        .await
        .unwrap();
    assert_exact_recipients(
        &event_recipients(
            &store,
            EventKind::WorkerRunCreated,
            EventEntityType::WorkerRun,
            &worker.worker_run_id.to_string(),
        )
        .await,
        &[
            backend.repository.repository_id,
            android.repository.repository_id,
        ],
    );
    let worker_events = store
        .wait_for_events_filtered(
            ActorRef::Orchestrator(android.instance.instance_id),
            EventWaitOptions {
                cursor: Some(0),
                consumer_key: None,
                kinds: &[EventKind::WorkerRunCreated],
                feature_id: Some(feature.feature_id),
                owner_repository_id: Some(android.repository.repository_id),
                timeout: Duration::ZERO,
                limit: 20,
            },
        )
        .await
        .unwrap();
    assert_eq!(worker_events.events.len(), 1);
    let worker_event = &worker_events.events[0];
    assert_eq!(worker_event.entity_id, worker.worker_run_id.to_string());
    assert_eq!(worker_event.feature_id, Some(feature.feature_id));
    assert_eq!(
        worker_event.owner_repository_id,
        Some(android.repository.repository_id)
    );
    assert_eq!(worker_event.entity_status.as_deref(), Some("created"));
    assert_eq!(worker_event.entity_revision, None);
    assert_eq!(worker_event.work_item_id, Some(item.work_item_id));
    assert_eq!(worker_event.request_id, None);

    let request = store
        .ask_repository(AskRepository {
            actor: ActorRef::Orchestrator(backend.instance.instance_id),
            target_repository_id: android.repository.repository_id,
            feature_id: Some(feature.feature_id),
            work_item_id: Some(item.work_item_id),
            question_summary: "Inspect the Android contract".to_owned(),
            body: None,
            context_refs: json!([]),
            handler_requirement: HandlerRequirement::ButlerRequired,
            answer_requirement: AnswerRequirement::SummaryAllowed,
        })
        .await
        .unwrap();
    assert_exact_recipients(
        &event_recipients(
            &store,
            EventKind::RequestAvailable,
            EventEntityType::Request,
            &request.request_id.to_string(),
        )
        .await,
        &[
            backend.repository.repository_id,
            android.repository.repository_id,
        ],
    );
    let request_events = store
        .wait_for_events_filtered(
            ActorRef::Orchestrator(android.instance.instance_id),
            EventWaitOptions {
                cursor: Some(worker_events.next_cursor),
                consumer_key: None,
                kinds: &[EventKind::RequestAvailable],
                feature_id: Some(feature.feature_id),
                owner_repository_id: Some(android.repository.repository_id),
                timeout: Duration::ZERO,
                limit: 20,
            },
        )
        .await
        .unwrap();
    assert_eq!(request_events.events.len(), 1);
    let request_event = &request_events.events[0];
    assert_eq!(request_event.entity_status.as_deref(), Some("pending"));
    assert_eq!(request_event.work_item_id, Some(item.work_item_id));
    assert_eq!(request_event.request_id, Some(request.request_id));

    let butler = store
        .create_worker_run(CreateWorkerRun {
            parent_instance_id: android.instance.instance_id,
            role: WorkerRole::Butler,
            task_summary: "Inspect the requested contract".to_owned(),
            context_refs: json!([]),
            addresses_audit_run_id: None,
            work_item_id: None,
            request_id: Some(request.request_id),
        })
        .await
        .unwrap();
    assert_exact_recipients(
        &event_recipients(
            &store,
            EventKind::WorkerRunCreated,
            EventEntityType::WorkerRun,
            &butler.worker_run_id.to_string(),
        )
        .await,
        &[
            backend.repository.repository_id,
            android.repository.repository_id,
        ],
    );
    let butler_events = store
        .wait_for_events_filtered(
            ActorRef::Orchestrator(android.instance.instance_id),
            EventWaitOptions {
                cursor: Some(request_events.next_cursor),
                consumer_key: None,
                kinds: &[EventKind::WorkerRunCreated],
                feature_id: Some(feature.feature_id),
                owner_repository_id: Some(android.repository.repository_id),
                timeout: Duration::ZERO,
                limit: 20,
            },
        )
        .await
        .unwrap();
    assert_eq!(butler_events.events.len(), 1);
    let butler_event = &butler_events.events[0];
    assert_eq!(butler_event.entity_id, butler.worker_run_id.to_string());
    assert_eq!(butler_event.work_item_id, None);
    assert_eq!(butler_event.request_id, Some(request.request_id));

    let message = store
        .publish_message(publish_command(
            ActorRef::Orchestrator(backend.instance.instance_id),
            MessageTarget::Repositories(vec![android.repository.repository_id]),
            "Explicit delivery only",
            None,
        ))
        .await
        .unwrap();
    assert_exact_recipients(
        &event_recipients(
            &store,
            EventKind::MessageAvailable,
            EventEntityType::Message,
            &message.message.message_id.to_string(),
        )
        .await,
        &[android.repository.repository_id],
    );

    store.pool.close().await;
    remove_database(&path);
}

#[tokio::test]
async fn snapshot_cursor_and_named_consumers_recover_without_shared_progress() {
    let (store, path) = test_store(90).await;
    let backend = store
        .connect_orchestrator(connect_command(
            "checkpoint-recovery",
            "backend",
            "C:/checkpoint/backend",
        ))
        .await
        .unwrap();
    let web = store
        .connect_orchestrator(connect_command(
            "checkpoint-recovery",
            "web",
            "C:/checkpoint/web",
        ))
        .await
        .unwrap();
    store
        .publish_message(publish_command(
            ActorRef::Orchestrator(backend.instance.instance_id),
            MessageTarget::Repositories(vec![web.repository.repository_id]),
            "First checkpoint event",
            None,
        ))
        .await
        .unwrap();
    let first_batch = store
        .wait_for_events(
            ActorRef::Orchestrator(web.instance.instance_id),
            0,
            &[EventKind::MessageAvailable],
            Duration::ZERO,
            20,
        )
        .await
        .unwrap();
    let first_event_id = first_batch.events[0].event_id;

    let replacement = store
        .connect_orchestrator(connect_command(
            "checkpoint-recovery",
            "web",
            "C:/checkpoint/web",
        ))
        .await
        .unwrap();
    assert_eq!(replacement.pending_inbox_count, 1);
    assert!(replacement.snapshot_event_cursor >= first_event_id);

    store
        .publish_message(publish_command(
            ActorRef::Orchestrator(backend.instance.instance_id),
            MessageTarget::Repositories(vec![web.repository.repository_id]),
            "Second checkpoint event",
            None,
        ))
        .await
        .unwrap();
    let after_snapshot = store
        .wait_for_events_from(
            ActorRef::Orchestrator(replacement.instance.instance_id),
            Some(replacement.snapshot_event_cursor),
            None,
            &[EventKind::MessageAvailable],
            Duration::ZERO,
            20,
        )
        .await
        .unwrap();
    assert_eq!(after_snapshot.events.len(), 1);
    let second_event_id = after_snapshot.events[0].event_id;
    assert!(second_event_id > replacement.snapshot_event_cursor);

    let alpha = store
        .get_event_checkpoint(
            ActorRef::Orchestrator(replacement.instance.instance_id),
            "alpha",
        )
        .await
        .unwrap();
    assert_eq!(alpha.cursor, 0);
    assert!(alpha.created_at.is_none());
    store
        .commit_event_cursor(
            ActorRef::Orchestrator(replacement.instance.instance_id),
            "alpha",
            first_event_id,
        )
        .await
        .unwrap();
    let alpha_page = store
        .wait_for_events_from(
            ActorRef::Orchestrator(replacement.instance.instance_id),
            None,
            Some("alpha"),
            &[EventKind::MessageAvailable],
            Duration::ZERO,
            20,
        )
        .await
        .unwrap();
    assert_eq!(alpha_page.events.len(), 1);
    assert_eq!(alpha_page.events[0].event_id, second_event_id);
    assert_eq!(
        store
            .get_event_checkpoint(
                ActorRef::Orchestrator(replacement.instance.instance_id),
                "alpha",
            )
            .await
            .unwrap()
            .cursor,
        first_event_id,
        "returning a page must not auto-commit it"
    );

    let beta_page = store
        .wait_for_events_from(
            ActorRef::Orchestrator(replacement.instance.instance_id),
            None,
            Some("beta"),
            &[EventKind::MessageAvailable],
            Duration::ZERO,
            20,
        )
        .await
        .unwrap();
    assert_eq!(beta_page.events.len(), 2);
    assert!(matches!(
        store
            .commit_event_cursor(
                ActorRef::Orchestrator(replacement.instance.instance_id),
                "alpha",
                first_event_id - 1,
            )
            .await,
        Err(StoreError::Conflict(_))
    ));
    assert!(matches!(
        store
            .commit_event_cursor(
                ActorRef::Orchestrator(replacement.instance.instance_id),
                "alpha",
                second_event_id + 1,
            )
            .await,
        Err(StoreError::Conflict(_))
    ));
    store
        .commit_event_cursor(
            ActorRef::Orchestrator(replacement.instance.instance_id),
            "alpha",
            second_event_id,
        )
        .await
        .unwrap();
    assert_eq!(
        store
            .get_event_checkpoint(
                ActorRef::Orchestrator(backend.instance.instance_id),
                "alpha",
            )
            .await
            .unwrap()
            .cursor,
        0,
        "the same consumer key in another repository has independent progress"
    );

    store.pool.close().await;
    drop(store);
    let reopened = SqliteStore::open(&path, 2, 90, 3600, 600, 3600)
        .await
        .unwrap();
    let reconnected = reopened
        .connect_orchestrator(connect_command(
            "checkpoint-recovery",
            "web",
            "C:/checkpoint/web",
        ))
        .await
        .unwrap();
    assert_eq!(
        reopened
            .get_event_checkpoint(
                ActorRef::Orchestrator(reconnected.instance.instance_id),
                "alpha",
            )
            .await
            .unwrap()
            .cursor,
        second_event_id
    );

    reopened.pool.close().await;
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
                Duration::from_secs(2),
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
                    Duration::from_secs(3),
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
            Duration::from_millis(1_500),
            20,
        )
        .await
        .unwrap();
    let renewed = store
        .get_instance(connected.instance.instance_id)
        .await
        .unwrap();

    assert_eq!(batch.outcome, WaitOutcome::Deadline);
    assert_eq!(batch.next_cursor, 42);
    assert!(batch.events.is_empty());
    assert!(renewed.last_heartbeat_at > initial_heartbeat);
    assert!(renewed.lease_expires_at > utc_now());

    store.pool.close().await;
    remove_database(&path);
}

#[tokio::test]
async fn wait_deadline_includes_initial_write_lock_contention() {
    let (store, path) = test_store(90).await;
    let connected = store
        .connect_orchestrator(connect_command(
            "wait-contention",
            "backend",
            "C:/wait-contention/backend",
        ))
        .await
        .unwrap();
    let write_guard = store.write_lock.lock().await;
    let started_at = tokio::time::Instant::now();
    let batch = tokio::time::timeout(
        Duration::from_millis(500),
        store.wait_for_events(
            ActorRef::Orchestrator(connected.instance.instance_id),
            77,
            &[],
            Duration::from_millis(100),
            20,
        ),
    )
    .await
    .expect("wait_for_events exceeded its full-call deadline")
    .unwrap();
    assert_eq!(batch.outcome, WaitOutcome::Deadline);
    assert_eq!(batch.next_cursor, 77);
    assert!(batch.events.is_empty());
    assert!(started_at.elapsed() < Duration::from_millis(300));
    drop(write_guard);

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
            feature_id: None,
            work_item_id: None,
            question_summary: "Operational request".to_owned(),
            body: None,
            context_refs: json!([]),
            handler_requirement: HandlerRequirement::OrchestratorAllowed,
            answer_requirement: AnswerRequirement::SummaryAllowed,
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
            "EXPLAIN QUERY PLAN SELECT work_item_id FROM work_items WHERE implementation_artifact_id = 'artifact'",
            "work_items_implementation_artifact_idx",
        ),
        (
            "EXPLAIN QUERY PLAN SELECT work_item_id FROM work_items WHERE latest_audit_artifact_id = 'artifact'",
            "work_items_latest_audit_artifact_idx",
        ),
        (
            "EXPLAIN QUERY PLAN SELECT event_id FROM events WHERE recipient_repository_id = 'repository' AND kind = 'message_available' AND event_id > 0 ORDER BY event_id",
            "events_repository_kind_cursor_idx",
        ),
        (
            "EXPLAIN QUERY PLAN SELECT event_id FROM events WHERE recipient_repository_id = 'repository' AND feature_id = 'feature' AND event_id > 0 ORDER BY event_id",
            "events_repository_feature_cursor_idx",
        ),
        (
            "EXPLAIN QUERY PLAN SELECT event_id FROM events WHERE recipient_repository_id = 'repository' AND owner_repository_id = 'owner' AND event_id > 0 ORDER BY event_id",
            "events_repository_owner_cursor_idx",
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
    let shared_artifact_plan = sqlx::query(
        "EXPLAIN QUERY PLAN
         SELECT a.artifact_id FROM artifacts a
         WHERE a.project_id = 'project' AND (
             EXISTS (
                 SELECT 1 FROM work_items shared_implementation
                 JOIN features shared_feature
                   ON shared_feature.feature_id = shared_implementation.feature_id
                 WHERE shared_implementation.implementation_artifact_id = a.artifact_id
                   AND shared_feature.project_id = a.project_id
             ) OR EXISTS (
                 SELECT 1 FROM audit_outcomes shared_audit
                 JOIN work_items shared_audit_item
                   ON shared_audit_item.work_item_id = shared_audit.audit_work_item_id
                 JOIN features shared_audit_feature
                   ON shared_audit_feature.feature_id = shared_audit_item.feature_id
                 WHERE shared_audit.audit_artifact_id = a.artifact_id
                   AND shared_audit_feature.project_id = a.project_id
             )
         ) ORDER BY a.created_at, a.artifact_id LIMIT 51",
    )
    .fetch_all(&store.pool)
    .await
    .unwrap()
    .iter()
    .map(|row| row.try_get::<String, _>("detail").unwrap())
    .collect::<Vec<_>>()
    .join(" ");
    assert!(
        shared_artifact_plan.contains("work_items_implementation_artifact_idx"),
        "unexpected shared-artifact query plan: {shared_artifact_plan}"
    );
    assert!(
        shared_artifact_plan.contains("sqlite_autoindex_audit_outcomes"),
        "unexpected shared-artifact query plan: {shared_artifact_plan}"
    );

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
            feature_id: None,
            work_item_id: None,
            question_summary: "Which response fields are guaranteed?".to_owned(),
            body: Some("Need the complete API contract.".to_owned()),
            context_refs: json!([]),
            handler_requirement: HandlerRequirement::OrchestratorAllowed,
            answer_requirement: AnswerRequirement::SummaryAllowed,
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
            feature_id: None,
            work_item_id: None,
            question_summary: "Can this request be retried?".to_owned(),
            body: None,
            context_refs: json!([]),
            handler_requirement: HandlerRequirement::OrchestratorAllowed,
            answer_requirement: AnswerRequirement::SummaryAllowed,
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
            feature_id: None,
            work_item_id: None,
            question_summary: "Return the contract".to_owned(),
            body: None,
            context_refs: json!([]),
            handler_requirement: HandlerRequirement::OrchestratorAllowed,
            answer_requirement: AnswerRequirement::SummaryAllowed,
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
            feature_id: None,
            work_item_id: None,
            question_summary: "Resolve ownership rules".to_owned(),
            body: None,
            context_refs: json!([]),
            handler_requirement: HandlerRequirement::OrchestratorAllowed,
            answer_requirement: AnswerRequirement::SummaryAllowed,
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
            Duration::ZERO,
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
        kind: WorkItemKind::Implementation,
        audit_scope_work_item_ids: Vec::new(),
        addresses_audit_run_id: None,
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
        addresses_audit_run_id: None,
        request_id: None,
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
        .publish_message_idempotent(message_command.clone(), &[], Some("message-key"))
        .await
        .unwrap();
    let replayed_message = store
        .publish_message_idempotent(message_command, &[], Some("message-key"))
        .await
        .unwrap();
    assert_eq!(
        first_message.message.message_id,
        replayed_message.message.message_id
    );

    let request_command = AskRepository {
        actor: ActorRef::Orchestrator(web.instance.instance_id),
        target_repository_id: backend.repository.repository_id,
        feature_id: None,
        work_item_id: None,
        question_summary: "Idempotent question".to_owned(),
        body: None,
        context_refs: json!([]),
        handler_requirement: HandlerRequirement::OrchestratorAllowed,
        answer_requirement: AnswerRequirement::SummaryAllowed,
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
            feature_id: None,
            work_item_id: None,
            question_summary: "Renew me".to_owned(),
            body: None,
            context_refs: json!([]),
            handler_requirement: HandlerRequirement::OrchestratorAllowed,
            answer_requirement: AnswerRequirement::SummaryAllowed,
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
                    feature_id: None,
                    work_item_id: None,
                    question_summary: format!("Question {index}"),
                    body: None,
                    context_refs: json!([]),
                    handler_requirement: HandlerRequirement::OrchestratorAllowed,
                    answer_requirement: AnswerRequirement::SummaryAllowed,
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
                addresses_audit_run_id: None,
                request_id: Some(requests[index].request_id),
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
async fn request_handler_policy_enforces_butler_scope_and_records_provenance() {
    let (store, path) = test_store(90).await;
    let requester = store
        .connect_orchestrator(connect_command("handler-policy", "web", "C:/handler/web"))
        .await
        .unwrap();
    let target = store
        .connect_orchestrator(connect_command(
            "handler-policy",
            "backend",
            "C:/handler/backend",
        ))
        .await
        .unwrap();

    let status_request = store
        .ask_repository(AskRepository {
            actor: ActorRef::Orchestrator(requester.instance.instance_id),
            target_repository_id: target.repository.repository_id,
            feature_id: None,
            work_item_id: None,
            question_summary: "Is backend work still in progress?".to_owned(),
            body: None,
            context_refs: json!([]),
            handler_requirement: HandlerRequirement::OrchestratorAllowed,
            answer_requirement: AnswerRequirement::SummaryAllowed,
        })
        .await
        .unwrap();
    store
        .claim_request(
            ActorRef::Orchestrator(target.instance.instance_id),
            status_request.request_id,
        )
        .await
        .unwrap();
    let status_answer = store
        .answer_request(AnswerRequest {
            actor: ActorRef::Orchestrator(target.instance.instance_id),
            request_id: status_request.request_id,
            answer_summary: "Backend work is active".to_owned(),
            body: None,
            content_type: "text/plain".to_owned(),
        })
        .await
        .unwrap();
    assert_eq!(
        serde_json::to_value(status_answer.request.answer_provenance).unwrap(),
        json!("orchestrator")
    );
    assert_eq!(
        status_answer.request.answered_by_instance_id,
        Some(target.instance.instance_id)
    );
    assert_eq!(status_answer.request.answered_by_worker_run_id, None);

    let technical_request = store
        .ask_repository(AskRepository {
            actor: ActorRef::Orchestrator(requester.instance.instance_id),
            target_repository_id: target.repository.repository_id,
            feature_id: None,
            work_item_id: None,
            question_summary: "Trace the authentication contract".to_owned(),
            body: Some("Inspect the current code and return the exact flow.".to_owned()),
            context_refs: json!([]),
            handler_requirement: HandlerRequirement::ButlerRequired,
            answer_requirement: AnswerRequirement::ArtifactRequired,
        })
        .await
        .unwrap();
    assert!(matches!(
        store
            .claim_request(
                ActorRef::Orchestrator(target.instance.instance_id),
                technical_request.request_id,
            )
            .await,
        Err(StoreError::Conflict(_))
    ));

    let butler = store
        .create_worker_run(CreateWorkerRun {
            parent_instance_id: target.instance.instance_id,
            role: WorkerRole::Butler,
            task_summary: "Inspect the authentication contract".to_owned(),
            context_refs: json!([]),
            addresses_audit_run_id: None,
            request_id: Some(technical_request.request_id),
            work_item_id: None,
        })
        .await
        .unwrap();
    let butler = store
        .activate_worker_run(butler.worker_run_id)
        .await
        .unwrap();
    assert!(matches!(
        store
            .create_worker_run(CreateWorkerRun {
                parent_instance_id: target.instance.instance_id,
                role: WorkerRole::Butler,
                task_summary: "Overlapping butler".to_owned(),
                context_refs: json!([]),
                addresses_audit_run_id: None,
                request_id: Some(technical_request.request_id),
                work_item_id: None,
            })
            .await,
        Err(StoreError::Conflict(_))
    ));

    let unrelated_request = store
        .ask_repository(AskRepository {
            actor: ActorRef::Orchestrator(requester.instance.instance_id),
            target_repository_id: target.repository.repository_id,
            feature_id: None,
            work_item_id: None,
            question_summary: "Is the deployment ready?".to_owned(),
            body: None,
            context_refs: json!([]),
            handler_requirement: HandlerRequirement::OrchestratorAllowed,
            answer_requirement: AnswerRequirement::SummaryAllowed,
        })
        .await
        .unwrap();
    assert!(matches!(
        store
            .claim_request(
                ActorRef::Worker(butler.worker_run_id),
                unrelated_request.request_id,
            )
            .await,
        Err(StoreError::Conflict(_))
    ));

    store
        .claim_request(
            ActorRef::Worker(butler.worker_run_id),
            technical_request.request_id,
        )
        .await
        .unwrap();
    assert!(matches!(
        store
            .answer_request(AnswerRequest {
                actor: ActorRef::Worker(butler.worker_run_id),
                request_id: technical_request.request_id,
                answer_summary: "Authentication flow traced".to_owned(),
                body: None,
                content_type: "text/markdown".to_owned(),
            })
            .await,
        Err(StoreError::Conflict(_))
    ));
    assert_eq!(
        store
            .get_request(technical_request.request_id)
            .await
            .unwrap()
            .status,
        RequestStatus::Claimed
    );
    let technical_answer = store
        .answer_request(AnswerRequest {
            actor: ActorRef::Worker(butler.worker_run_id),
            request_id: technical_request.request_id,
            answer_summary: "Authentication flow traced".to_owned(),
            body: Some("The verified contract and code references.".to_owned()),
            content_type: "text/markdown".to_owned(),
        })
        .await
        .unwrap();
    assert_eq!(
        serde_json::to_value(technical_answer.request.answer_provenance).unwrap(),
        json!("butler")
    );
    assert_eq!(
        technical_answer.request.answered_by_worker_run_id,
        Some(butler.worker_run_id)
    );
    assert!(technical_answer.artifact.is_some());

    store
        .complete_worker_run(butler.worker_run_id, "Request answered")
        .await
        .unwrap();
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
            kind: WorkItemKind::Implementation,
            audit_scope_work_item_ids: Vec::new(),
            addresses_audit_run_id: None,
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
            kind: WorkItemKind::Implementation,
            audit_scope_work_item_ids: Vec::new(),
            addresses_audit_run_id: None,
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
            addresses_audit_run_id: None,
            request_id: None,
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
            kind: WorkItemKind::Implementation,
            audit_scope_work_item_ids: Vec::new(),
            addresses_audit_run_id: None,
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
            kind: WorkItemKind::Implementation,
            audit_scope_work_item_ids: Vec::new(),
            addresses_audit_run_id: None,
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
                    kind: WorkItemKind::Implementation,
                    audit_scope_work_item_ids: Vec::new(),
                    addresses_audit_run_id: None,
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

#[tokio::test]
async fn discovery_lists_reconstruct_actor_visible_state_with_stable_pages() {
    let (store, path) = test_store(90).await;
    let backend = store
        .connect_orchestrator(connect_command(
            "discovery-recovery",
            "backend",
            "C:/discovery/backend",
        ))
        .await
        .unwrap();
    let android = store
        .connect_orchestrator(connect_command(
            "discovery-recovery",
            "android",
            "C:/discovery/android",
        ))
        .await
        .unwrap();
    let first_feature = store
        .create_feature(CreateFeature {
            parent_instance_id: backend.instance.instance_id,
            title: "Discovery one".to_owned(),
            summary: "First page".to_owned(),
        })
        .await
        .unwrap();
    let second_feature = store
        .create_feature(CreateFeature {
            parent_instance_id: backend.instance.instance_id,
            title: "Discovery two".to_owned(),
            summary: "Second page".to_owned(),
        })
        .await
        .unwrap();
    let first_page = store
        .list_features(
            backend.project.project_id,
            Some(FeatureStatus::Active),
            None,
            1,
        )
        .await
        .unwrap();
    assert_eq!(first_page.items.len(), 1);
    assert!(first_page.has_more);
    let second_page = store
        .list_features(
            backend.project.project_id,
            Some(FeatureStatus::Active),
            first_page.next_cursor,
            1,
        )
        .await
        .unwrap();
    assert_eq!(second_page.items.len(), 1);
    assert!(!second_page.has_more);
    assert_ne!(
        first_page.items[0].feature_id,
        second_page.items[0].feature_id
    );
    assert!(
        [first_feature.feature_id, second_feature.feature_id]
            .contains(&second_page.items[0].feature_id)
    );

    let item = store
        .create_work_item(CreateWorkItem {
            parent_instance_id: backend.instance.instance_id,
            feature_id: first_feature.feature_id,
            parent_work_item_id: None,
            owner_repository_id: backend.repository.repository_id,
            title: "Recover this work".to_owned(),
            summary: "Expose the linked worker".to_owned(),
            kind: WorkItemKind::Implementation,
            audit_scope_work_item_ids: Vec::new(),
            addresses_audit_run_id: None,
            dependency_ids: Vec::new(),
            priority: 0,
            max_audit_iterations: 1,
        })
        .await
        .unwrap();
    let claimed = store
        .claim_work_item(
            backend.instance.instance_id,
            item.work_item_id,
            item.revision,
        )
        .await
        .unwrap();
    let worker = store
        .create_worker_run(CreateWorkerRun {
            parent_instance_id: backend.instance.instance_id,
            role: WorkerRole::Executor,
            task_summary: "Implement recovery probe".to_owned(),
            context_refs: json!([]),
            addresses_audit_run_id: None,
            work_item_id: Some(item.work_item_id),
            request_id: None,
        })
        .await
        .unwrap();
    let projection = store
        .get_work_item(claimed.work_item_id)
        .await
        .unwrap()
        .worker_projection
        .unwrap();
    assert_eq!(projection.worker_run_id, worker.worker_run_id);
    assert_eq!(projection.status, WorkerRunStatus::Created);
    let listed_projection = store
        .list_work_items(first_feature.feature_id, None, 20)
        .await
        .unwrap()
        .items
        .into_iter()
        .find(|listed| listed.work_item_id == item.work_item_id)
        .unwrap()
        .worker_projection
        .unwrap();
    assert_eq!(listed_projection.worker_run_id, worker.worker_run_id);

    let request = store
        .ask_repository(AskRepository {
            actor: ActorRef::Orchestrator(backend.instance.instance_id),
            target_repository_id: android.repository.repository_id,
            feature_id: Some(first_feature.feature_id),
            work_item_id: Some(item.work_item_id),
            question_summary: "Which Android DTO is affected?".to_owned(),
            body: None,
            context_refs: json!([]),
            handler_requirement: HandlerRequirement::OrchestratorAllowed,
            answer_requirement: AnswerRequirement::ArtifactRequired,
        })
        .await
        .unwrap();
    let published = store
        .publish_message(PublishMessage {
            actor: ActorRef::Orchestrator(backend.instance.instance_id),
            kind: MessageKind::Report,
            target: MessageTarget::Repositories(vec![android.repository.repository_id]),
            summary: "Recovery marker".to_owned(),
            body: Some("BODY_MUST_NOT_APPEAR_IN_METADATA".to_owned()),
            artifact_kind: ArtifactKind::Note,
            artifact_title: Some("Recovery note".to_owned()),
            content_type: "text/markdown".to_owned(),
            feature_id: Some(first_feature.feature_id),
            work_item_id: Some(item.work_item_id),
            request_id: None,
            thread_id: None,
            correlation_id: None,
            in_reply_to: None,
        })
        .await
        .unwrap();
    store
        .ack_message(
            ActorRef::Orchestrator(android.instance.instance_id),
            published.message.message_id,
        )
        .await
        .unwrap();
    let second_published = store
        .publish_message(PublishMessage {
            actor: ActorRef::Orchestrator(backend.instance.instance_id),
            kind: MessageKind::Report,
            target: MessageTarget::Repositories(vec![android.repository.repository_id]),
            summary: "Second recovery marker".to_owned(),
            body: Some("SECOND_BODY".to_owned()),
            artifact_kind: ArtifactKind::Note,
            artifact_title: Some("Second recovery note".to_owned()),
            content_type: "text/markdown".to_owned(),
            feature_id: Some(first_feature.feature_id),
            work_item_id: Some(item.work_item_id),
            request_id: None,
            thread_id: None,
            correlation_id: None,
            in_reply_to: None,
        })
        .await
        .unwrap();

    let workers = store
        .list_worker_runs(
            ActorRef::Orchestrator(android.instance.instance_id),
            WorkerRunListFilter {
                feature_id: Some(first_feature.feature_id),
                ..Default::default()
            },
            None,
            20,
        )
        .await
        .unwrap();
    assert_eq!(workers.items[0].worker_run_id, worker.worker_run_id);
    let requests = store
        .list_requests(
            ActorRef::Orchestrator(android.instance.instance_id),
            RequestListFilter {
                target_repository_id: Some(android.repository.repository_id),
                feature_id: Some(first_feature.feature_id),
                ..Default::default()
            },
            None,
            20,
        )
        .await
        .unwrap();
    assert_eq!(requests.items[0].request_id, request.request_id);
    assert_eq!(
        requests.items[0].answer_requirement,
        AnswerRequirement::ArtifactRequired
    );
    let deliveries = store
        .list_deliveries(
            ActorRef::Orchestrator(android.instance.instance_id),
            DeliveryListStatus::Acknowledged,
            None,
            20,
        )
        .await
        .unwrap();
    assert_eq!(deliveries.items.len(), 1);
    assert_eq!(deliveries.items[0].status, "acknowledged");
    let message_page_one = store
        .list_messages(
            ActorRef::Orchestrator(android.instance.instance_id),
            MessageListFilter {
                work_item_id: Some(item.work_item_id),
                ..Default::default()
            },
            None,
            1,
        )
        .await
        .unwrap();
    assert!(message_page_one.has_more);
    let message_page_two = store
        .list_messages(
            ActorRef::Orchestrator(android.instance.instance_id),
            MessageListFilter {
                work_item_id: Some(item.work_item_id),
                ..Default::default()
            },
            message_page_one.next_cursor,
            1,
        )
        .await
        .unwrap();
    assert!(!message_page_two.has_more);
    assert_ne!(
        message_page_one.items[0].message_id,
        message_page_two.items[0].message_id
    );
    assert!(
        [
            published.message.message_id,
            second_published.message.message_id
        ]
        .contains(&message_page_two.items[0].message_id)
    );
    let artifact_page_one = store
        .list_artifacts(
            ActorRef::Orchestrator(android.instance.instance_id),
            ArtifactListFilter {
                feature_id: Some(first_feature.feature_id),
                ..Default::default()
            },
            None,
            1,
        )
        .await
        .unwrap();
    assert!(artifact_page_one.has_more);
    let artifact_page_two = store
        .list_artifacts(
            ActorRef::Orchestrator(android.instance.instance_id),
            ArtifactListFilter {
                feature_id: Some(first_feature.feature_id),
                ..Default::default()
            },
            artifact_page_one.next_cursor,
            1,
        )
        .await
        .unwrap();
    assert!(!artifact_page_two.has_more);
    assert_ne!(
        artifact_page_one.items[0].artifact_id,
        artifact_page_two.items[0].artifact_id
    );
    assert_eq!(
        artifact_page_one.items[0].work_item_id,
        Some(item.work_item_id)
    );
    assert!(
        serde_json::to_value(&artifact_page_one.items[0])
            .unwrap()
            .get("content")
            .is_none()
    );

    store.pool.close().await;
    remove_database(&path);
}

#[tokio::test]
async fn orchestrators_share_linked_artifacts_without_exposing_private_or_request_content() {
    let (store, path) = test_store(90).await;
    let backend = store
        .connect_orchestrator(connect_command(
            "shared-artifacts",
            "backend",
            "C:/shared/backend",
        ))
        .await
        .unwrap();
    let android = store
        .connect_orchestrator(connect_command(
            "shared-artifacts",
            "android",
            "C:/shared/android",
        ))
        .await
        .unwrap();
    let web = store
        .connect_orchestrator(connect_command("shared-artifacts", "web", "C:/shared/web"))
        .await
        .unwrap();
    let unrelated = store
        .connect_orchestrator(connect_command(
            "other-artifacts",
            "backend",
            "C:/other/backend",
        ))
        .await
        .unwrap();
    let feature = store
        .create_feature(CreateFeature {
            parent_instance_id: backend.instance.instance_id,
            title: "Shared reports".to_owned(),
            summary: "Recover repository implementation artifacts".to_owned(),
        })
        .await
        .unwrap();
    let android_item = store
        .create_work_item(CreateWorkItem {
            parent_instance_id: backend.instance.instance_id,
            feature_id: feature.feature_id,
            parent_work_item_id: None,
            owner_repository_id: android.repository.repository_id,
            title: "Android implementation".to_owned(),
            summary: "Produce a linked report".to_owned(),
            kind: WorkItemKind::Implementation,
            audit_scope_work_item_ids: Vec::new(),
            addresses_audit_run_id: None,
            dependency_ids: Vec::new(),
            priority: 0,
            max_audit_iterations: 1,
        })
        .await
        .unwrap();
    let claimed = store
        .claim_work_item(
            android.instance.instance_id,
            android_item.work_item_id,
            android_item.revision,
        )
        .await
        .unwrap();
    let executor = store
        .create_worker_run(CreateWorkerRun {
            parent_instance_id: android.instance.instance_id,
            role: WorkerRole::Executor,
            task_summary: "Implement Android change".to_owned(),
            context_refs: json!([]),
            addresses_audit_run_id: None,
            work_item_id: Some(android_item.work_item_id),
            request_id: None,
        })
        .await
        .unwrap();
    store
        .activate_worker_run(executor.worker_run_id)
        .await
        .unwrap();
    let report = store
        .publish_message(PublishMessage {
            actor: ActorRef::Worker(executor.worker_run_id),
            kind: MessageKind::Report,
            target: MessageTarget::Repositories(vec![android.repository.repository_id]),
            summary: "Android implementation complete".to_owned(),
            body: Some("ANDROID_IMPLEMENTATION_REPORT".to_owned()),
            artifact_kind: ArtifactKind::ImplementationReport,
            artifact_title: Some("Android report".to_owned()),
            content_type: "text/markdown".to_owned(),
            feature_id: None,
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
    assert!(
        store
            .read_artifact(
                ActorRef::Orchestrator(backend.instance.instance_id),
                report.artifact_id,
            )
            .await
            .is_err(),
        "an unlinked report remains private before the WorkItem accepts it"
    );
    store
        .complete_worker_run(executor.worker_run_id, "Android implementation complete")
        .await
        .unwrap();
    let current = store
        .get_work_item(android_item.work_item_id)
        .await
        .unwrap();
    assert!(current.revision > claimed.revision);
    store
        .update_work_item(UpdateWorkItem {
            parent_instance_id: android.instance.instance_id,
            work_item_id: android_item.work_item_id,
            expected_revision: current.revision,
            title: None,
            summary: None,
            owner_repository_id: None,
            parent_work_item_id: None,
            dependency_ids: None,
            priority: None,
            status: Some(WorkItemStatus::Completed),
            implementation_artifact_id: Some(report.artifact_id),
        })
        .await
        .unwrap();

    for instance_id in [backend.instance.instance_id, web.instance.instance_id] {
        assert_eq!(
            store
                .read_artifact(ActorRef::Orchestrator(instance_id), report.artifact_id)
                .await
                .unwrap()
                .content,
            "ANDROID_IMPLEMENTATION_REPORT"
        );
    }
    assert!(
        store
            .read_artifact(
                ActorRef::Orchestrator(unrelated.instance.instance_id),
                report.artifact_id,
            )
            .await
            .is_err()
    );
    assert_eq!(
        store
            .list_artifacts(
                ActorRef::Orchestrator(web.instance.instance_id),
                ArtifactListFilter {
                    work_item_id: Some(android_item.work_item_id),
                    ..Default::default()
                },
                None,
                20,
            )
            .await
            .unwrap()
            .items[0]
            .artifact_id,
        report.artifact_id
    );

    let audit_item = create_claimed_audit_item(
        &store,
        &android,
        feature.feature_id,
        vec![android_item.work_item_id],
        "shared artifact access",
    )
    .await;
    let auditor = create_auditor_run(&store, &android, &audit_item, "Audit shared report").await;
    store
        .activate_worker_run(auditor.worker_run_id)
        .await
        .unwrap();
    let audit_report = store
        .publish_message_with_audit_outcomes(
            PublishMessage {
                actor: ActorRef::Worker(auditor.worker_run_id),
                kind: MessageKind::Report,
                target: MessageTarget::Repositories(vec![android.repository.repository_id]),
                summary: "Android audit passed".to_owned(),
                body: Some("ANDROID_AUDIT_REPORT".to_owned()),
                artifact_kind: ArtifactKind::AuditReport,
                artifact_title: Some("Android audit".to_owned()),
                content_type: "text/markdown".to_owned(),
                feature_id: None,
                work_item_id: None,
                request_id: None,
                thread_id: None,
                correlation_id: None,
                in_reply_to: None,
            },
            &[audit_outcome(
                android_item.work_item_id,
                AuditVerdict::Passed,
            )],
        )
        .await
        .unwrap()
        .artifact
        .unwrap();
    assert_eq!(
        store
            .read_artifact(
                ActorRef::Orchestrator(backend.instance.instance_id),
                audit_report.artifact_id,
            )
            .await
            .unwrap()
            .content,
        "ANDROID_AUDIT_REPORT"
    );
    store
        .complete_worker_run(auditor.worker_run_id, "Audit passed")
        .await
        .unwrap();
    complete_audit_work_item(&store, &android, audit_item.work_item_id).await;

    let web_item = store
        .create_work_item(CreateWorkItem {
            parent_instance_id: backend.instance.instance_id,
            feature_id: feature.feature_id,
            parent_work_item_id: None,
            owner_repository_id: web.repository.repository_id,
            title: "Worker isolation".to_owned(),
            summary: "Keep worker access repository-scoped".to_owned(),
            kind: WorkItemKind::Implementation,
            audit_scope_work_item_ids: Vec::new(),
            addresses_audit_run_id: None,
            dependency_ids: Vec::new(),
            priority: 0,
            max_audit_iterations: 1,
        })
        .await
        .unwrap();
    let web_claim = store
        .claim_work_item(
            web.instance.instance_id,
            web_item.work_item_id,
            web_item.revision,
        )
        .await
        .unwrap();
    let web_worker = store
        .create_worker_run(CreateWorkerRun {
            parent_instance_id: web.instance.instance_id,
            role: WorkerRole::Executor,
            task_summary: "Verify worker isolation".to_owned(),
            context_refs: json!([]),
            addresses_audit_run_id: None,
            work_item_id: Some(web_claim.work_item_id),
            request_id: None,
        })
        .await
        .unwrap();
    store
        .activate_worker_run(web_worker.worker_run_id)
        .await
        .unwrap();
    assert!(
        store
            .read_artifact(
                ActorRef::Worker(web_worker.worker_run_id),
                report.artifact_id
            )
            .await
            .is_err()
    );

    let private = store
        .publish_message(PublishMessage {
            actor: ActorRef::Orchestrator(android.instance.instance_id),
            kind: MessageKind::Report,
            target: MessageTarget::Repositories(vec![android.repository.repository_id]),
            summary: "Private repository note".to_owned(),
            body: Some("PRIVATE_ANDROID_NOTE".to_owned()),
            artifact_kind: ArtifactKind::Note,
            artifact_title: None,
            content_type: "text/plain".to_owned(),
            feature_id: None,
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
    assert!(
        store
            .read_artifact(
                ActorRef::Orchestrator(backend.instance.instance_id),
                private.artifact_id,
            )
            .await
            .is_err()
    );

    let shared_note = store
        .publish_message(PublishMessage {
            actor: ActorRef::Orchestrator(android.instance.instance_id),
            kind: MessageKind::Report,
            target: MessageTarget::Repositories(vec![android.repository.repository_id]),
            summary: "Feature coordination note".to_owned(),
            body: Some("FEATURE_SHARED_NOTE".to_owned()),
            artifact_kind: ArtifactKind::Note,
            artifact_title: None,
            content_type: "text/plain".to_owned(),
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
    assert_eq!(
        store
            .read_artifact(
                ActorRef::Orchestrator(backend.instance.instance_id),
                shared_note.artifact_id,
            )
            .await
            .unwrap()
            .content,
        "FEATURE_SHARED_NOTE"
    );

    let request = store
        .ask_repository(AskRepository {
            actor: ActorRef::Orchestrator(android.instance.instance_id),
            target_repository_id: backend.repository.repository_id,
            feature_id: Some(feature.feature_id),
            work_item_id: Some(android_item.work_item_id),
            question_summary: "Private linked request".to_owned(),
            body: Some("PRIVATE_REQUEST_BODY".to_owned()),
            context_refs: json!([]),
            handler_requirement: HandlerRequirement::OrchestratorAllowed,
            answer_requirement: AnswerRequirement::ArtifactRequired,
        })
        .await
        .unwrap();
    let question_artifact_id = request.question_artifact_id.unwrap();
    assert!(
        store
            .read_artifact(
                ActorRef::Orchestrator(web.instance.instance_id),
                question_artifact_id,
            )
            .await
            .is_err()
    );
    assert!(
        store
            .read_artifact(
                ActorRef::Orchestrator(backend.instance.instance_id),
                question_artifact_id,
            )
            .await
            .is_ok()
    );
    store
        .claim_request(
            ActorRef::Orchestrator(backend.instance.instance_id),
            request.request_id,
        )
        .await
        .unwrap();
    let answer = store
        .answer_request(AnswerRequest {
            actor: ActorRef::Orchestrator(backend.instance.instance_id),
            request_id: request.request_id,
            answer_summary: "Private linked answer".to_owned(),
            body: Some("PRIVATE_ANSWER_BODY".to_owned()),
            content_type: "text/plain".to_owned(),
        })
        .await
        .unwrap()
        .artifact
        .unwrap();
    assert!(
        store
            .read_artifact(
                ActorRef::Orchestrator(web.instance.instance_id),
                answer.artifact_id,
            )
            .await
            .is_err()
    );
    assert!(
        store
            .read_artifact(
                ActorRef::Orchestrator(android.instance.instance_id),
                answer.artifact_id,
            )
            .await
            .is_ok()
    );
    let web_artifacts = store
        .list_artifacts(
            ActorRef::Orchestrator(web.instance.instance_id),
            ArtifactListFilter::default(),
            None,
            20,
        )
        .await
        .unwrap();
    let web_artifact_ids = web_artifacts
        .items
        .iter()
        .map(|artifact| artifact.artifact_id)
        .collect::<std::collections::HashSet<_>>();
    assert!(web_artifact_ids.contains(&report.artifact_id));
    assert!(web_artifact_ids.contains(&audit_report.artifact_id));
    assert!(web_artifact_ids.contains(&shared_note.artifact_id));
    assert!(!web_artifact_ids.contains(&private.artifact_id));
    assert!(!web_artifact_ids.contains(&question_artifact_id));
    assert!(!web_artifact_ids.contains(&answer.artifact_id));

    store.pool.close().await;
    remove_database(&path);
}

#[tokio::test]
async fn batch_audit_artifact_metadata_preserves_card_and_complete_scope() {
    let (store, path) = test_store(90).await;
    let connected = store
        .connect_orchestrator(connect_command(
            "batch-audit-metadata",
            "backend",
            "C:/batch-audit/backend",
        ))
        .await
        .unwrap();
    let feature = store
        .create_feature(CreateFeature {
            parent_instance_id: connected.instance.instance_id,
            title: "Batch audit metadata".to_owned(),
            summary: "Recover every audited WorkItem deterministically".to_owned(),
        })
        .await
        .unwrap();
    let mut implementation_ids = Vec::new();
    for label in ["API", "storage"] {
        let item = store
            .create_work_item(CreateWorkItem {
                parent_instance_id: connected.instance.instance_id,
                feature_id: feature.feature_id,
                parent_work_item_id: None,
                owner_repository_id: connected.repository.repository_id,
                title: format!("Implement {label}"),
                summary: format!("Implementation covered by the {label} audit scope"),
                kind: WorkItemKind::Implementation,
                audit_scope_work_item_ids: Vec::new(),
                addresses_audit_run_id: None,
                dependency_ids: Vec::new(),
                priority: 0,
                max_audit_iterations: 1,
            })
            .await
            .unwrap();
        complete_implementation_work(&store, &connected, feature.feature_id, &item).await;
        implementation_ids.push(item.work_item_id);
    }
    implementation_ids.sort_by_key(ToString::to_string);

    let audit_item = create_claimed_audit_item(
        &store,
        &connected,
        feature.feature_id,
        implementation_ids.clone(),
        "batch metadata",
    )
    .await;
    let auditor = create_auditor_run(&store, &connected, &audit_item, "Audit both items").await;
    store
        .activate_worker_run(auditor.worker_run_id)
        .await
        .unwrap();
    let audit_artifact = store
        .publish_message_with_audit_outcomes(
            PublishMessage {
                actor: ActorRef::Worker(auditor.worker_run_id),
                kind: MessageKind::Report,
                target: MessageTarget::Broadcast,
                summary: "Batch audit passed".to_owned(),
                body: Some("BATCH_AUDIT_REPORT".to_owned()),
                artifact_kind: ArtifactKind::AuditReport,
                artifact_title: None,
                content_type: "text/markdown".to_owned(),
                feature_id: Some(feature.feature_id),
                work_item_id: None,
                request_id: None,
                thread_id: None,
                correlation_id: None,
                in_reply_to: None,
            },
            &implementation_ids
                .iter()
                .map(|work_item_id| audit_outcome(*work_item_id, AuditVerdict::Passed))
                .collect::<Vec<_>>(),
        )
        .await
        .unwrap()
        .artifact
        .unwrap();

    for scoped_work_item_id in &implementation_ids {
        let page = store
            .list_artifacts(
                ActorRef::Orchestrator(connected.instance.instance_id),
                ArtifactListFilter {
                    work_item_id: Some(*scoped_work_item_id),
                    ..Default::default()
                },
                None,
                20,
            )
            .await
            .unwrap();
        let metadata = page
            .items
            .iter()
            .find(|metadata| metadata.artifact_id == audit_artifact.artifact_id)
            .unwrap();
        assert_eq!(metadata.work_item_id, None);
        assert_eq!(metadata.audit_work_item_id, Some(audit_item.work_item_id));
        assert_eq!(metadata.scoped_work_item_ids, implementation_ids);
    }

    store.pool.close().await;
    remove_database(&path);
}

#[tokio::test]
async fn verification_requires_remediation_and_passing_reaudit_before_feature_completion() {
    let (store, path) = test_store(90).await;
    let connected = store
        .connect_orchestrator(connect_command(
            "verification-loop",
            "backend",
            "C:/verification/backend",
        ))
        .await
        .unwrap();
    let feature = store
        .create_feature(CreateFeature {
            parent_instance_id: connected.instance.instance_id,
            title: "Verified feature".to_owned(),
            summary: "Exercise remediation and re-audit".to_owned(),
        })
        .await
        .unwrap();
    let item = store
        .create_work_item(CreateWorkItem {
            parent_instance_id: connected.instance.instance_id,
            feature_id: feature.feature_id,
            parent_work_item_id: None,
            owner_repository_id: connected.repository.repository_id,
            title: "Implement endpoint".to_owned(),
            summary: "Implementation requiring independent review".to_owned(),
            kind: WorkItemKind::Implementation,
            audit_scope_work_item_ids: Vec::new(),
            addresses_audit_run_id: None,
            dependency_ids: Vec::new(),
            priority: 0,
            max_audit_iterations: 2,
        })
        .await
        .unwrap();
    let implemented =
        complete_implementation_work(&store, &connected, feature.feature_id, &item).await;
    assert_eq!(
        implemented.verification_status,
        Some(VerificationStatus::PendingAudit)
    );
    assert!(matches!(
        store
            .complete_feature(CompleteFeature {
                parent_instance_id: connected.instance.instance_id,
                feature_id: feature.feature_id,
                expected_revision: feature.revision,
            })
            .await,
        Err(StoreError::Conflict(_))
    ));

    let first_audit = complete_audit_with_outcomes(
        &store,
        &connected,
        feature.feature_id,
        vec![item.work_item_id],
        &[audit_outcome(
            item.work_item_id,
            AuditVerdict::ChangesRequired,
        )],
    )
    .await;
    assert_eq!(
        store
            .get_work_item(item.work_item_id)
            .await
            .unwrap()
            .verification_status,
        Some(VerificationStatus::RemediationRequired)
    );

    let remediation = store
        .create_work_item(CreateWorkItem {
            parent_instance_id: connected.instance.instance_id,
            feature_id: feature.feature_id,
            parent_work_item_id: None,
            owner_repository_id: connected.repository.repository_id,
            title: "Address audit".to_owned(),
            summary: "Apply actionable audit findings".to_owned(),
            kind: WorkItemKind::Remediation,
            audit_scope_work_item_ids: Vec::new(),
            addresses_audit_run_id: Some(first_audit.worker_run_id),
            dependency_ids: Vec::new(),
            priority: 0,
            max_audit_iterations: 1,
        })
        .await
        .unwrap();
    let remediation = store
        .claim_work_item(
            connected.instance.instance_id,
            remediation.work_item_id,
            remediation.revision,
        )
        .await
        .unwrap();
    let remediation_run = store
        .create_worker_run(CreateWorkerRun {
            parent_instance_id: connected.instance.instance_id,
            role: WorkerRole::Executor,
            task_summary: "Address audit".to_owned(),
            context_refs: json!([]),
            addresses_audit_run_id: Some(first_audit.worker_run_id),
            request_id: None,
            work_item_id: Some(remediation.work_item_id),
        })
        .await
        .unwrap();
    assert_eq!(
        store
            .get_work_item(item.work_item_id)
            .await
            .unwrap()
            .verification_status,
        Some(VerificationStatus::Remediating)
    );
    store
        .activate_worker_run(remediation_run.worker_run_id)
        .await
        .unwrap();
    let report = store
        .publish_message(PublishMessage {
            actor: ActorRef::Worker(remediation_run.worker_run_id),
            kind: MessageKind::Report,
            target: MessageTarget::Broadcast,
            summary: "Audit findings addressed".to_owned(),
            body: Some("Remediation report".to_owned()),
            artifact_kind: ArtifactKind::ImplementationReport,
            artifact_title: None,
            content_type: "text/markdown".to_owned(),
            feature_id: Some(feature.feature_id),
            work_item_id: Some(remediation.work_item_id),
            request_id: None,
            thread_id: None,
            correlation_id: None,
            in_reply_to: None,
        })
        .await
        .unwrap()
        .artifact
        .unwrap();
    store
        .complete_worker_run(remediation_run.worker_run_id, "Remediation complete")
        .await
        .unwrap();
    let current_remediation = store.get_work_item(remediation.work_item_id).await.unwrap();
    store
        .update_work_item(UpdateWorkItem {
            parent_instance_id: connected.instance.instance_id,
            work_item_id: remediation.work_item_id,
            expected_revision: current_remediation.revision,
            title: None,
            summary: None,
            owner_repository_id: None,
            parent_work_item_id: None,
            dependency_ids: None,
            priority: None,
            status: Some(WorkItemStatus::Completed),
            implementation_artifact_id: Some(report.artifact_id),
        })
        .await
        .unwrap();
    assert_eq!(
        store
            .get_work_item(item.work_item_id)
            .await
            .unwrap()
            .verification_status,
        Some(VerificationStatus::PendingReaudit)
    );

    complete_audit_with_outcomes(
        &store,
        &connected,
        feature.feature_id,
        vec![item.work_item_id],
        &[audit_outcome(item.work_item_id, AuditVerdict::Passed)],
    )
    .await;
    assert_eq!(
        store
            .get_work_item(item.work_item_id)
            .await
            .unwrap()
            .verification_status,
        Some(VerificationStatus::Verified)
    );
    let completed = store
        .complete_feature(CompleteFeature {
            parent_instance_id: connected.instance.instance_id,
            feature_id: feature.feature_id,
            expected_revision: feature.revision,
        })
        .await
        .unwrap();
    assert_eq!(completed.status, FeatureStatus::Completed);

    store.pool.close().await;
    remove_database(&path);
}

#[tokio::test]
async fn audit_batch_requires_exact_outcomes_and_depth_reopening_is_revisioned_and_evented() {
    let (store, path) = test_store(90).await;
    let connected = store
        .connect_orchestrator(connect_command(
            "audit-depth",
            "backend",
            "C:/audit-depth/backend",
        ))
        .await
        .unwrap();
    let feature = store
        .create_feature(CreateFeature {
            parent_instance_id: connected.instance.instance_id,
            title: "Batch audit".to_owned(),
            summary: "Exercise exact coverage and depth reopening".to_owned(),
        })
        .await
        .unwrap();
    let mut items = Vec::new();
    for title in ["First", "Second"] {
        let item = store
            .create_work_item(CreateWorkItem {
                parent_instance_id: connected.instance.instance_id,
                feature_id: feature.feature_id,
                parent_work_item_id: None,
                owner_repository_id: connected.repository.repository_id,
                title: title.to_owned(),
                summary: "Audited implementation".to_owned(),
                kind: WorkItemKind::Implementation,
                audit_scope_work_item_ids: Vec::new(),
                addresses_audit_run_id: None,
                dependency_ids: Vec::new(),
                priority: 0,
                max_audit_iterations: 1,
            })
            .await
            .unwrap();
        sqlx::query(
            "UPDATE work_items SET status = 'completed', verification_status = 'pending_audit'
             WHERE work_item_id = ?",
        )
        .bind(item.work_item_id.to_string())
        .execute(&store.pool)
        .await
        .unwrap();
        items.push(item.work_item_id);
    }
    let audit_item = create_claimed_audit_item(
        &store,
        &connected,
        feature.feature_id,
        items.clone(),
        "batch",
    )
    .await;
    let audit = create_auditor_run(&store, &connected, &audit_item, "Audit batch").await;
    store
        .activate_worker_run(audit.worker_run_id)
        .await
        .unwrap();
    let command = PublishMessage {
        actor: ActorRef::Worker(audit.worker_run_id),
        kind: MessageKind::Report,
        target: MessageTarget::Broadcast,
        summary: "Batch verdict".to_owned(),
        body: Some("Batch audit report".to_owned()),
        artifact_kind: ArtifactKind::AuditReport,
        artifact_title: None,
        content_type: "text/markdown".to_owned(),
        feature_id: Some(feature.feature_id),
        work_item_id: Some(audit_item.work_item_id),
        request_id: None,
        thread_id: None,
        correlation_id: None,
        in_reply_to: None,
    };
    assert!(matches!(
        store
            .publish_message_with_audit_outcomes(
                command.clone(),
                &[audit_outcome(items[0], AuditVerdict::Passed)],
            )
            .await,
        Err(StoreError::Conflict(_))
    ));
    store
        .publish_message_with_audit_outcomes(
            command,
            &[
                audit_outcome(items[0], AuditVerdict::Passed),
                audit_outcome(items[1], AuditVerdict::Blocked),
            ],
        )
        .await
        .unwrap();
    store
        .complete_worker_run(audit.worker_run_id, "Batch audit complete")
        .await
        .unwrap();
    complete_audit_work_item(&store, &connected, audit_item.work_item_id).await;
    assert_eq!(
        store
            .get_work_item(items[0])
            .await
            .unwrap()
            .verification_status,
        Some(VerificationStatus::Verified)
    );
    let blocked = store.get_work_item(items[1]).await.unwrap();
    assert_eq!(
        blocked.verification_status,
        Some(VerificationStatus::VerificationBlocked)
    );
    let reopened = store
        .increase_audit_depth(IncreaseAuditDepth {
            parent_instance_id: connected.instance.instance_id,
            work_item_id: items[1],
            expected_revision: blocked.revision,
            max_audit_iterations: 2,
            reason: "User approved one additional independent audit".to_owned(),
        })
        .await
        .unwrap();
    assert_eq!(reopened.max_audit_iterations, 2);
    assert_eq!(
        reopened.verification_status,
        Some(VerificationStatus::PendingReaudit)
    );
    let reason: String =
        sqlx::query_scalar("SELECT reason FROM audit_depth_changes WHERE work_item_id = ?")
            .bind(items[1].to_string())
            .fetch_one(&store.pool)
            .await
            .unwrap();
    assert!(reason.contains("additional independent audit"));
    let events: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM events
         WHERE entity_id = ? AND kind = 'audit_depth_increased'",
    )
    .bind(items[1].to_string())
    .fetch_one(&store.pool)
    .await
    .unwrap();
    assert!(events > 0);
    assert!(matches!(
        store
            .increase_audit_depth(IncreaseAuditDepth {
                parent_instance_id: connected.instance.instance_id,
                work_item_id: items[1],
                expected_revision: blocked.revision,
                max_audit_iterations: 3,
                reason: "Stale retry".to_owned(),
            })
            .await,
        Err(StoreError::RevisionConflict { .. })
    ));
    complete_audit_with_outcomes(
        &store,
        &connected,
        feature.feature_id,
        vec![items[1]],
        &[audit_outcome(items[1], AuditVerdict::Passed)],
    )
    .await;
    assert_eq!(
        store
            .get_work_item(items[1])
            .await
            .unwrap()
            .verification_status,
        Some(VerificationStatus::Verified)
    );

    store.pool.close().await;
    remove_database(&path);
}

#[tokio::test]
async fn batch_remediation_requires_every_actionable_item_to_be_reopened() {
    let (store, path) = test_store(90).await;
    let connected = store
        .connect_orchestrator(connect_command(
            "batch-remediation-depth",
            "backend",
            "C:/batch-remediation/backend",
        ))
        .await
        .unwrap();
    let feature = store
        .create_feature(CreateFeature {
            parent_instance_id: connected.instance.instance_id,
            title: "Batch remediation depth".to_owned(),
            summary: "Keep per-item audit depth intact".to_owned(),
        })
        .await
        .unwrap();
    let mut item_ids = Vec::new();
    for title in ["First", "Second"] {
        let item = store
            .create_work_item(CreateWorkItem {
                parent_instance_id: connected.instance.instance_id,
                feature_id: feature.feature_id,
                parent_work_item_id: None,
                owner_repository_id: connected.repository.repository_id,
                title: title.to_owned(),
                summary: "Final-depth implementation".to_owned(),
                kind: WorkItemKind::Implementation,
                audit_scope_work_item_ids: Vec::new(),
                addresses_audit_run_id: None,
                dependency_ids: Vec::new(),
                priority: 0,
                max_audit_iterations: 1,
            })
            .await
            .unwrap();
        sqlx::query(
            "UPDATE work_items SET status = 'completed', verification_status = 'pending_audit'
             WHERE work_item_id = ?",
        )
        .bind(item.work_item_id.to_string())
        .execute(&store.pool)
        .await
        .unwrap();
        item_ids.push(item.work_item_id);
    }
    let audit = complete_audit_with_outcomes(
        &store,
        &connected,
        feature.feature_id,
        item_ids.clone(),
        &[
            audit_outcome(item_ids[0], AuditVerdict::ChangesRequired),
            audit_outcome(item_ids[1], AuditVerdict::ChangesRequired),
        ],
    )
    .await;
    let first = store.get_work_item(item_ids[0]).await.unwrap();
    let second = store.get_work_item(item_ids[1]).await.unwrap();
    assert_eq!(
        first.verification_status,
        Some(VerificationStatus::VerificationBlocked)
    );
    assert_eq!(
        second.verification_status,
        Some(VerificationStatus::VerificationBlocked)
    );
    store
        .increase_audit_depth(IncreaseAuditDepth {
            parent_instance_id: connected.instance.instance_id,
            work_item_id: first.work_item_id,
            expected_revision: first.revision,
            max_audit_iterations: 2,
            reason: "Reopen the first finding".to_owned(),
        })
        .await
        .unwrap();
    let remediation = CreateWorkItem {
        parent_instance_id: connected.instance.instance_id,
        feature_id: feature.feature_id,
        parent_work_item_id: None,
        owner_repository_id: connected.repository.repository_id,
        title: "Address batch findings".to_owned(),
        summary: "One remediation for the complete audit report".to_owned(),
        kind: WorkItemKind::Remediation,
        audit_scope_work_item_ids: Vec::new(),
        addresses_audit_run_id: Some(audit.worker_run_id),
        dependency_ids: Vec::new(),
        priority: 0,
        max_audit_iterations: 1,
    };
    assert!(matches!(
        store.create_work_item(remediation.clone()).await,
        Err(StoreError::Conflict(_))
    ));
    let still_blocked = store.get_work_item(item_ids[1]).await.unwrap();
    assert_eq!(
        still_blocked.verification_status,
        Some(VerificationStatus::VerificationBlocked)
    );
    store
        .increase_audit_depth(IncreaseAuditDepth {
            parent_instance_id: connected.instance.instance_id,
            work_item_id: still_blocked.work_item_id,
            expected_revision: still_blocked.revision,
            max_audit_iterations: 2,
            reason: "Reopen the second finding".to_owned(),
        })
        .await
        .unwrap();
    assert!(store.create_work_item(remediation).await.is_ok());

    store.pool.close().await;
    remove_database(&path);
}

#[tokio::test]
async fn completed_worker_reports_survive_orchestrator_claim_takeover() {
    let (store, path) = test_store(90).await;
    let first = store
        .connect_orchestrator(connect_command(
            "report-takeover",
            "backend",
            "C:/report-takeover/backend",
        ))
        .await
        .unwrap();
    let second = store
        .connect_orchestrator(connect_command(
            "report-takeover",
            "backend",
            "C:/report-takeover/backend",
        ))
        .await
        .unwrap();
    let feature = store
        .create_feature(CreateFeature {
            parent_instance_id: first.instance.instance_id,
            title: "Recover completed reports".to_owned(),
            summary: "Transfer card ownership without redoing finished work".to_owned(),
        })
        .await
        .unwrap();
    let item = store
        .create_work_item(CreateWorkItem {
            parent_instance_id: first.instance.instance_id,
            feature_id: feature.feature_id,
            parent_work_item_id: None,
            owner_repository_id: first.repository.repository_id,
            title: "Implementation".to_owned(),
            summary: "Completed before orchestrator disappears".to_owned(),
            kind: WorkItemKind::Implementation,
            audit_scope_work_item_ids: Vec::new(),
            addresses_audit_run_id: None,
            dependency_ids: Vec::new(),
            priority: 0,
            max_audit_iterations: 1,
        })
        .await
        .unwrap();
    let item = store
        .claim_work_item(first.instance.instance_id, item.work_item_id, item.revision)
        .await
        .unwrap();
    let executor = store
        .create_worker_run(CreateWorkerRun {
            parent_instance_id: first.instance.instance_id,
            role: WorkerRole::Executor,
            task_summary: "Implement".to_owned(),
            context_refs: json!([]),
            addresses_audit_run_id: None,
            request_id: None,
            work_item_id: Some(item.work_item_id),
        })
        .await
        .unwrap();
    store
        .activate_worker_run(executor.worker_run_id)
        .await
        .unwrap();
    let report = store
        .publish_message(PublishMessage {
            actor: ActorRef::Worker(executor.worker_run_id),
            kind: MessageKind::Report,
            target: MessageTarget::Broadcast,
            summary: "Implementation complete".to_owned(),
            body: Some("Implementation report".to_owned()),
            artifact_kind: ArtifactKind::ImplementationReport,
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
        .unwrap()
        .artifact
        .unwrap();
    store
        .complete_worker_run(executor.worker_run_id, "Implementation complete")
        .await
        .unwrap();
    let current = store.get_work_item(item.work_item_id).await.unwrap();
    sqlx::query("UPDATE work_items SET claim_expires_at = ? WHERE work_item_id = ?")
        .bind("2000-01-01T00:00:00.000000Z")
        .bind(item.work_item_id.to_string())
        .execute(&store.pool)
        .await
        .unwrap();
    let taken_over = store
        .claim_work_item(
            second.instance.instance_id,
            item.work_item_id,
            current.revision,
        )
        .await
        .unwrap();
    let completed = store
        .update_work_item(UpdateWorkItem {
            parent_instance_id: second.instance.instance_id,
            work_item_id: item.work_item_id,
            expected_revision: taken_over.revision,
            title: None,
            summary: None,
            owner_repository_id: None,
            parent_work_item_id: None,
            dependency_ids: None,
            priority: None,
            status: Some(WorkItemStatus::Completed),
            implementation_artifact_id: Some(report.artifact_id),
        })
        .await
        .unwrap();
    assert_eq!(completed.status, WorkItemStatus::Completed);

    let audit_item = create_claimed_audit_item(
        &store,
        &second,
        feature.feature_id,
        vec![item.work_item_id],
        "taken-over implementation",
    )
    .await;
    let auditor = create_auditor_run(&store, &second, &audit_item, "Audit implementation").await;
    store
        .activate_worker_run(auditor.worker_run_id)
        .await
        .unwrap();
    store
        .publish_message_with_audit_outcomes(
            PublishMessage {
                actor: ActorRef::Worker(auditor.worker_run_id),
                kind: MessageKind::Report,
                target: MessageTarget::Broadcast,
                summary: "Audit passed".to_owned(),
                body: Some("Audit report".to_owned()),
                artifact_kind: ArtifactKind::AuditReport,
                artifact_title: None,
                content_type: "text/markdown".to_owned(),
                feature_id: Some(feature.feature_id),
                work_item_id: Some(audit_item.work_item_id),
                request_id: None,
                thread_id: None,
                correlation_id: None,
                in_reply_to: None,
            },
            &[audit_outcome(item.work_item_id, AuditVerdict::Passed)],
        )
        .await
        .unwrap();
    store
        .complete_worker_run(auditor.worker_run_id, "Audit complete")
        .await
        .unwrap();
    let current_audit = store.get_work_item(audit_item.work_item_id).await.unwrap();
    sqlx::query("UPDATE work_items SET claim_expires_at = ? WHERE work_item_id = ?")
        .bind("2000-01-01T00:00:00.000000Z")
        .bind(audit_item.work_item_id.to_string())
        .execute(&store.pool)
        .await
        .unwrap();
    let audit_takeover = store
        .claim_work_item(
            first.instance.instance_id,
            audit_item.work_item_id,
            current_audit.revision,
        )
        .await
        .unwrap();
    let completed_audit = store
        .update_work_item(UpdateWorkItem {
            parent_instance_id: first.instance.instance_id,
            work_item_id: audit_item.work_item_id,
            expected_revision: audit_takeover.revision,
            title: None,
            summary: None,
            owner_repository_id: None,
            parent_work_item_id: None,
            dependency_ids: None,
            priority: None,
            status: Some(WorkItemStatus::Completed),
            implementation_artifact_id: None,
        })
        .await
        .unwrap();
    assert_eq!(completed_audit.status, WorkItemStatus::Completed);

    store.pool.close().await;
    remove_database(&path);
}
