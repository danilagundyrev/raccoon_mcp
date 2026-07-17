use serde::{Deserialize, Serialize};

use super::{
    ArtifactId, ArtifactKind, Feature, FeatureId, HandlerRequirement, InstanceId, Message,
    MessageId, ProjectId, RepositoryId, RequestId, RequestStatus, ThreadId, WorkItemId, WorkerRole,
    WorkerRunId, WorkerRunStatus,
};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CursorPage<T, C> {
    pub items: Vec<T>,
    pub next_cursor: Option<C>,
    pub has_more: bool,
}

pub type FeaturePage = CursorPage<Feature, FeatureId>;
pub type WorkerRunPage = CursorPage<WorkerRunSummary, WorkerRunId>;
pub type RequestPage = CursorPage<RequestSummary, RequestId>;
pub type DeliveryPage = CursorPage<DeliveryHistoryItem, i64>;
pub type MessagePage = CursorPage<Message, MessageId>;
pub type ArtifactPage = CursorPage<ArtifactMetadata, ArtifactId>;

#[derive(Clone, Debug, Default)]
pub struct WorkerRunListFilter {
    pub repository_id: Option<RepositoryId>,
    pub feature_id: Option<FeatureId>,
    pub work_item_id: Option<WorkItemId>,
    pub role: Option<WorkerRole>,
    pub status: Option<WorkerRunStatus>,
    pub addresses_audit_run_id: Option<WorkerRunId>,
}

#[derive(Clone, Debug, Default)]
pub struct RequestListFilter {
    pub requester_repository_id: Option<RepositoryId>,
    pub target_repository_id: Option<RepositoryId>,
    pub feature_id: Option<FeatureId>,
    pub work_item_id: Option<WorkItemId>,
    pub status: Option<RequestStatus>,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum DeliveryListStatus {
    Pending,
    Acknowledged,
    #[default]
    All,
}

#[derive(Clone, Debug, Default)]
pub struct MessageListFilter {
    pub feature_id: Option<FeatureId>,
    pub work_item_id: Option<WorkItemId>,
    pub request_id: Option<RequestId>,
    pub thread_id: Option<ThreadId>,
    pub kind: Option<super::MessageKind>,
    pub producer_repository_id: Option<RepositoryId>,
    pub recipient_repository_id: Option<RepositoryId>,
}

#[derive(Clone, Debug, Default)]
pub struct ArtifactListFilter {
    pub kind: Option<ArtifactKind>,
    pub feature_id: Option<FeatureId>,
    pub work_item_id: Option<WorkItemId>,
    pub request_id: Option<RequestId>,
    pub producer_repository_id: Option<RepositoryId>,
    pub producer_worker_run_id: Option<WorkerRunId>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WorkerRunSummary {
    pub worker_run_id: WorkerRunId,
    pub repository_id: RepositoryId,
    pub parent_instance_id: InstanceId,
    pub role: WorkerRole,
    pub status: WorkerRunStatus,
    pub task_summary: String,
    pub result_summary: Option<String>,
    pub error_summary: Option<String>,
    pub addresses_audit_run_id: Option<WorkerRunId>,
    pub work_item_id: Option<WorkItemId>,
    pub request_id: Option<RequestId>,
    pub created_at: String,
    pub last_heartbeat_at: String,
    pub lease_expires_at: String,
    pub finished_at: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RequestSummary {
    pub request_id: RequestId,
    pub project_id: ProjectId,
    pub requester_repository_id: RepositoryId,
    pub target_repository_id: RepositoryId,
    pub feature_id: Option<FeatureId>,
    pub work_item_id: Option<WorkItemId>,
    pub status: RequestStatus,
    pub handler_requirement: HandlerRequirement,
    pub question_summary: String,
    pub answer_message_id: Option<MessageId>,
    pub created_at: String,
    pub updated_at: String,
    pub finished_at: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DeliveryHistoryItem {
    pub delivery_id: i64,
    pub message: Message,
    pub status: String,
    pub delivered_at: String,
    pub acked_at: Option<String>,
    pub acked_by_instance_id: Option<InstanceId>,
    pub acked_by_worker_run_id: Option<WorkerRunId>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ArtifactMetadata {
    pub artifact_id: ArtifactId,
    pub project_id: ProjectId,
    pub producer_repository_id: RepositoryId,
    pub producer_instance_id: Option<InstanceId>,
    pub producer_worker_run_id: Option<WorkerRunId>,
    pub kind: ArtifactKind,
    pub title: String,
    pub content_type: String,
    pub feature_id: Option<FeatureId>,
    pub work_item_id: Option<WorkItemId>,
    pub request_id: Option<RequestId>,
    pub message_id: Option<MessageId>,
    pub created_at: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WorkItemWorkerProjection {
    pub worker_run_id: WorkerRunId,
    pub role: WorkerRole,
    pub status: WorkerRunStatus,
    pub last_heartbeat_at: String,
    pub lease_expires_at: String,
}
