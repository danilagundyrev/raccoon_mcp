mod events;
mod ids;
mod messaging;
mod operations;
mod presence;
mod project;
mod requests;
mod work;
mod worker;

pub use events::{EventBatch, EventEntityType, EventKind, HubEvent, WaitOutcome};
pub use ids::{
    ArtifactId, FeatureId, InstanceId, MessageId, ProjectId, RepositoryId, RequestId, ThreadId,
    WorkItemId, WorkerRunId,
};
pub use messaging::{
    ActorRef, Artifact, ArtifactKind, DeliveryAck, InboxItem, InboxPage, Message, MessageKind,
    MessageTarget, PublishMessage, PublishResult,
};
pub use operations::{LeaseMaintenanceReport, OperationalMetrics, RetentionReport};
pub use presence::{AgentInstance, ConnectOrchestrator, ConnectResult, PresenceStatus};
pub use project::{Project, ProjectOverview, Repository, RepositoryPresence};
pub use requests::{
    AnswerRequest, AnswerRequestResult, AskRepository, CoordinationRequest, FailRequest,
    RequestStatus,
};
pub use work::{CreateFeature, CreateWorkItem, Feature, UpdateWorkItem, WorkItem, WorkItemStatus};
pub use worker::{
    AuditScopeItem, AuditWorkItemInput, CreateWorkerRun, WorkerRole, WorkerRun, WorkerRunStatus,
};

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
