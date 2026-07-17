use std::str::FromStr;

use serde::{Deserialize, Serialize};

use super::{FeatureId, ProjectId, RepositoryId, RequestId, WorkItemId};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EventCheckpoint {
    pub repository_id: RepositoryId,
    pub consumer_key: String,
    pub cursor: i64,
    pub created_at: Option<String>,
    pub updated_at: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EventKind {
    MessageAvailable,
    WorkerRunCreated,
    WorkerRunActive,
    WorkerRunCompleted,
    WorkerRunFailed,
    RequestAvailable,
    RequestClaimed,
    RequestAnswered,
    RequestFailed,
    FeatureCreated,
    FeatureUpdated,
    FeatureCompleted,
    FeatureCancelled,
    WorkItemCreated,
    WorkItemUpdated,
    AuditDepthIncreased,
}

impl EventKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::MessageAvailable => "message_available",
            Self::WorkerRunCreated => "worker_run_created",
            Self::WorkerRunActive => "worker_run_active",
            Self::WorkerRunCompleted => "worker_run_completed",
            Self::WorkerRunFailed => "worker_run_failed",
            Self::RequestAvailable => "request_available",
            Self::RequestClaimed => "request_claimed",
            Self::RequestAnswered => "request_answered",
            Self::RequestFailed => "request_failed",
            Self::FeatureCreated => "feature_created",
            Self::FeatureUpdated => "feature_updated",
            Self::FeatureCompleted => "feature_completed",
            Self::FeatureCancelled => "feature_cancelled",
            Self::WorkItemCreated => "work_item_created",
            Self::WorkItemUpdated => "work_item_updated",
            Self::AuditDepthIncreased => "audit_depth_increased",
        }
    }
}

impl FromStr for EventKind {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "message_available" => Ok(Self::MessageAvailable),
            "worker_run_created" => Ok(Self::WorkerRunCreated),
            "worker_run_active" => Ok(Self::WorkerRunActive),
            "worker_run_completed" => Ok(Self::WorkerRunCompleted),
            "worker_run_failed" => Ok(Self::WorkerRunFailed),
            "request_available" => Ok(Self::RequestAvailable),
            "request_claimed" => Ok(Self::RequestClaimed),
            "request_answered" => Ok(Self::RequestAnswered),
            "request_failed" => Ok(Self::RequestFailed),
            "feature_created" => Ok(Self::FeatureCreated),
            "feature_updated" => Ok(Self::FeatureUpdated),
            "feature_completed" => Ok(Self::FeatureCompleted),
            "feature_cancelled" => Ok(Self::FeatureCancelled),
            "work_item_created" => Ok(Self::WorkItemCreated),
            "work_item_updated" => Ok(Self::WorkItemUpdated),
            "audit_depth_increased" => Ok(Self::AuditDepthIncreased),
            _ => Err(format!("unknown event kind '{value}'")),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EventEntityType {
    Message,
    WorkerRun,
    Request,
    Feature,
    WorkItem,
}

impl EventEntityType {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Message => "message",
            Self::WorkerRun => "worker_run",
            Self::Request => "request",
            Self::Feature => "feature",
            Self::WorkItem => "work_item",
        }
    }
}

impl FromStr for EventEntityType {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "message" => Ok(Self::Message),
            "worker_run" => Ok(Self::WorkerRun),
            "request" => Ok(Self::Request),
            "feature" => Ok(Self::Feature),
            "work_item" => Ok(Self::WorkItem),
            _ => Err(format!("unknown event entity type '{value}'")),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HubEvent {
    pub event_id: i64,
    pub project_id: ProjectId,
    pub recipient_repository_id: RepositoryId,
    pub kind: EventKind,
    pub entity_type: EventEntityType,
    pub entity_id: String,
    pub feature_id: Option<FeatureId>,
    pub owner_repository_id: Option<RepositoryId>,
    pub entity_status: Option<String>,
    pub entity_revision: Option<u64>,
    pub work_item_id: Option<WorkItemId>,
    pub request_id: Option<RequestId>,
    pub created_at: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WaitOutcome {
    Events,
    Deadline,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EventBatch {
    pub outcome: WaitOutcome,
    pub events: Vec<HubEvent>,
    pub next_cursor: i64,
    pub has_more: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_supported_event_kinds() {
        assert_eq!("message_available".parse(), Ok(EventKind::MessageAvailable));
        assert_eq!("feature_completed".parse(), Ok(EventKind::FeatureCompleted));
        assert!("handoff".parse::<EventKind>().is_err());
    }
}
