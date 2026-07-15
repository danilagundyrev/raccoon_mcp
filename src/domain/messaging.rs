use std::str::FromStr;

use serde::{Deserialize, Serialize};

use super::{
    ArtifactId, FeatureId, InstanceId, MessageId, ProjectId, RepositoryId, RequestId, ThreadId,
    WorkItemId, WorkerRunId,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ActorRef {
    Orchestrator(InstanceId),
    Worker(WorkerRunId),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MessageKind {
    Report,
    Announcement,
    Command,
}

impl MessageKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Report => "report",
            Self::Announcement => "announcement",
            Self::Command => "command",
        }
    }
}

impl FromStr for MessageKind {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "report" => Ok(Self::Report),
            "announcement" => Ok(Self::Announcement),
            "command" => Ok(Self::Command),
            _ => Err(format!(
                "unknown message kind '{value}'; expected report, announcement or command"
            )),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ArtifactKind {
    ImplementationReport,
    ApiContract,
    Answer,
    AuditReport,
    Note,
}

impl ArtifactKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ImplementationReport => "implementation_report",
            Self::ApiContract => "api_contract",
            Self::Answer => "answer",
            Self::AuditReport => "audit_report",
            Self::Note => "note",
        }
    }
}

impl FromStr for ArtifactKind {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "implementation_report" => Ok(Self::ImplementationReport),
            "api_contract" => Ok(Self::ApiContract),
            "answer" => Ok(Self::Answer),
            "audit_report" => Ok(Self::AuditReport),
            "note" => Ok(Self::Note),
            _ => Err(format!(
                "unknown artifact kind '{value}'; expected implementation_report, api_contract, answer, audit_report or note"
            )),
        }
    }
}

#[derive(Clone, Debug)]
pub enum MessageTarget {
    Repositories(Vec<RepositoryId>),
    Broadcast,
}

#[derive(Clone, Debug)]
pub struct PublishMessage {
    pub actor: ActorRef,
    pub kind: MessageKind,
    pub target: MessageTarget,
    pub summary: String,
    pub body: Option<String>,
    pub artifact_kind: ArtifactKind,
    pub artifact_title: Option<String>,
    pub content_type: String,
    pub feature_id: Option<FeatureId>,
    pub work_item_id: Option<WorkItemId>,
    pub request_id: Option<RequestId>,
    pub thread_id: Option<ThreadId>,
    pub correlation_id: Option<String>,
    pub in_reply_to: Option<MessageId>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Artifact {
    pub artifact_id: ArtifactId,
    pub project_id: ProjectId,
    pub producer_repository_id: RepositoryId,
    pub producer_instance_id: Option<InstanceId>,
    pub producer_worker_run_id: Option<WorkerRunId>,
    pub kind: ArtifactKind,
    pub title: String,
    pub content_type: String,
    pub content: String,
    pub created_at: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Message {
    pub message_id: MessageId,
    pub project_id: ProjectId,
    pub sender_repository_id: RepositoryId,
    pub sender_instance_id: Option<InstanceId>,
    pub sender_worker_run_id: Option<WorkerRunId>,
    pub kind: MessageKind,
    pub feature_id: Option<FeatureId>,
    pub work_item_id: Option<WorkItemId>,
    pub request_id: Option<RequestId>,
    pub thread_id: ThreadId,
    pub correlation_id: Option<String>,
    pub in_reply_to: Option<MessageId>,
    pub summary: String,
    pub artifact_id: Option<ArtifactId>,
    pub created_at: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PublishResult {
    pub message: Message,
    pub artifact: Option<Artifact>,
    pub recipient_count: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct InboxItem {
    pub delivery_id: i64,
    pub message: Message,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct InboxPage {
    pub items: Vec<InboxItem>,
    pub next_cursor: Option<i64>,
    pub has_more: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DeliveryAck {
    pub delivery_id: i64,
    pub message_id: MessageId,
    pub recipient_repository_id: RepositoryId,
    pub status: String,
    pub acked_at: String,
    pub acked_by_instance_id: Option<InstanceId>,
    pub acked_by_worker_run_id: Option<WorkerRunId>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_message_and_artifact_kinds() {
        assert_eq!("report".parse(), Ok(MessageKind::Report));
        assert_eq!("api_contract".parse(), Ok(ArtifactKind::ApiContract));
        assert!("handoff".parse::<MessageKind>().is_err());
    }
}
