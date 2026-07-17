mod events;
mod history;
mod ids;
mod messaging;
mod operations;
mod presence;
mod project;
mod requests;
mod work;
mod worker;

pub const MAX_CONTEXT_REFS: usize = 50;
pub const MAX_AUDIT_WORK_ITEMS: usize = 50;
pub const MAX_WORK_ITEM_DEPENDENCIES: usize = 100;
pub const MAX_MESSAGE_RECIPIENTS: usize = 100;

pub use events::{EventBatch, EventCheckpoint, EventEntityType, EventKind, HubEvent, WaitOutcome};
pub use history::{
    ArtifactListFilter, ArtifactMetadata, ArtifactPage, DeliveryHistoryItem, DeliveryListStatus,
    DeliveryPage, FeaturePage, MessageListFilter, MessagePage, RequestListFilter, RequestPage,
    RequestSummary, WorkItemWorkerProjection, WorkerRunListFilter, WorkerRunPage, WorkerRunSummary,
};
pub use ids::{
    ArtifactId, FeatureId, InstanceId, MessageId, ProjectId, RepositoryId, RequestId, ThreadId,
    WorkItemId, WorkerRunId,
};
pub use messaging::{
    ActorRef, Artifact, ArtifactKind, AuditOutcomeInput, AuditVerdict, DeliveryAck, InboxItem,
    InboxPage, Message, MessageKind, MessageTarget, PublishMessage, PublishResult,
};
pub use operations::{LeaseMaintenanceReport, OperationalMetrics, RetentionReport};
pub use presence::{
    AgentInstance, ConnectOrchestrator, ConnectResult, PresenceStatus, RepositoryPresenceFilter,
};
pub use project::{Project, ProjectOverview, Repository, RepositoryPresence};
pub use requests::{
    AnswerRequest, AnswerRequestResult, AnswerRequirement, AskRepository, CoordinationRequest,
    FailRequest, HandlerRequirement, RequestStatus,
};
pub use work::{
    CancelFeature, CompleteFeature, CreateFeature, CreateWorkItem, Feature, FeatureStatus,
    IncreaseAuditDepth, UpdateWorkItem, VerificationStatus, WorkItem, WorkItemKind, WorkItemPage,
    WorkItemStatus,
};
pub use worker::{AuditScopeItem, CreateWorkerRun, WorkerRole, WorkerRun, WorkerRunStatus};

pub(crate) fn validate_key(field_name: &str, value: &str) -> Result<String, String> {
    let value = value.trim();
    if value.is_empty() {
        return Err(format!("{field_name} must not be empty"));
    }
    if value.len() > 64 {
        return Err(format!("{field_name} must not exceed 64 bytes"));
    }
    if !value
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
    {
        return Err(format!(
            "{field_name} may contain only ASCII letters, digits, '.', '_' and '-'"
        ));
    }
    Ok(value.to_owned())
}

#[cfg(test)]
mod tests {
    use super::validate_key;

    #[test]
    fn validates_user_defined_keys() {
        assert_eq!(
            validate_key("repository_key", " android-app ").unwrap(),
            "android-app"
        );
        assert!(validate_key("repository_key", "android app").is_err());
        assert!(validate_key("repository_key", "").is_err());
    }
}
