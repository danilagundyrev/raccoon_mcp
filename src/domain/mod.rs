mod events;
mod ids;
mod messaging;
mod presence;
mod project;
mod worker;

pub use events::{EventBatch, EventEntityType, EventKind, HubEvent, WaitOutcome};
pub use ids::{
    ArtifactId, InstanceId, MessageId, ProjectId, RepositoryId, ThreadId, WorkItemId, WorkerRunId,
};
pub use messaging::{
    ActorRef, Artifact, ArtifactKind, DeliveryAck, InboxItem, InboxPage, Message, MessageKind,
    MessageTarget, PublishMessage, PublishResult,
};
pub use presence::{AgentInstance, ConnectOrchestrator, ConnectResult, PresenceStatus};
pub use project::{Project, ProjectOverview, Repository, RepositoryPresence};
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
