use std::str::FromStr;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::{
    ActorRef, Artifact, ArtifactId, FeatureId, InstanceId, Message, MessageId, ProjectId,
    RepositoryId, RequestId, WorkItemId, WorkerRunId,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RequestStatus {
    Pending,
    Claimed,
    Answered,
    Failed,
}

impl RequestStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Claimed => "claimed",
            Self::Answered => "answered",
            Self::Failed => "failed",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HandlerRequirement {
    OrchestratorAllowed,
    ButlerRequired,
}

impl HandlerRequirement {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::OrchestratorAllowed => "orchestrator_allowed",
            Self::ButlerRequired => "butler_required",
        }
    }
}

impl FromStr for HandlerRequirement {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "orchestrator_allowed" => Ok(Self::OrchestratorAllowed),
            "butler_required" => Ok(Self::ButlerRequired),
            _ => Err(format!("unknown handler requirement '{value}'")),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AnswerRequirement {
    SummaryAllowed,
    ArtifactRequired,
}

impl AnswerRequirement {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::SummaryAllowed => "summary_allowed",
            Self::ArtifactRequired => "artifact_required",
        }
    }
}

impl FromStr for AnswerRequirement {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "summary_allowed" => Ok(Self::SummaryAllowed),
            "artifact_required" => Ok(Self::ArtifactRequired),
            _ => Err(format!("unknown answer requirement '{value}'")),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AnswerProvenance {
    Orchestrator,
    Butler,
}

impl FromStr for AnswerProvenance {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "orchestrator" => Ok(Self::Orchestrator),
            "butler" => Ok(Self::Butler),
            _ => Err(format!("unknown answer provenance '{value}'")),
        }
    }
}

impl FromStr for RequestStatus {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "pending" => Ok(Self::Pending),
            "claimed" => Ok(Self::Claimed),
            "answered" => Ok(Self::Answered),
            "failed" => Ok(Self::Failed),
            _ => Err(format!("unknown request status '{value}'")),
        }
    }
}

#[derive(Clone, Debug)]
pub struct AskRepository {
    pub actor: ActorRef,
    pub target_repository_id: RepositoryId,
    pub feature_id: Option<FeatureId>,
    pub work_item_id: Option<WorkItemId>,
    pub question_summary: String,
    pub body: Option<String>,
    pub context_refs: Value,
    pub handler_requirement: HandlerRequirement,
    pub answer_requirement: AnswerRequirement,
}

#[derive(Clone, Debug)]
pub struct AnswerRequest {
    pub actor: ActorRef,
    pub request_id: RequestId,
    pub answer_summary: String,
    pub body: Option<String>,
    pub content_type: String,
}

#[derive(Clone, Debug)]
pub struct FailRequest {
    pub actor: ActorRef,
    pub request_id: RequestId,
    pub retryable: bool,
    pub summary: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CoordinationRequest {
    pub request_id: RequestId,
    pub project_id: ProjectId,
    pub requester_repository_id: RepositoryId,
    pub target_repository_id: RepositoryId,
    pub feature_id: Option<FeatureId>,
    pub work_item_id: Option<WorkItemId>,
    pub created_by_instance_id: Option<InstanceId>,
    pub created_by_worker_run_id: Option<WorkerRunId>,
    pub status: RequestStatus,
    pub handler_requirement: HandlerRequirement,
    pub answer_requirement: AnswerRequirement,
    pub question_summary: String,
    pub question_artifact_id: Option<ArtifactId>,
    pub context_refs: Value,
    pub claimed_by_instance_id: Option<InstanceId>,
    pub claimed_by_worker_run_id: Option<WorkerRunId>,
    pub claim_expires_at: Option<String>,
    pub failure_summary: Option<String>,
    pub failure_retryable: Option<bool>,
    pub answer_message_id: Option<MessageId>,
    pub answer_provenance: Option<AnswerProvenance>,
    pub answered_by_instance_id: Option<InstanceId>,
    pub answered_by_worker_run_id: Option<WorkerRunId>,
    pub created_at: String,
    pub updated_at: String,
    pub finished_at: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AnswerRequestResult {
    pub request: CoordinationRequest,
    pub message: Message,
    pub artifact: Option<Artifact>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_request_statuses() {
        assert_eq!("claimed".parse(), Ok(RequestStatus::Claimed));
        assert!("cancelled".parse::<RequestStatus>().is_err());
        assert_eq!(
            "butler_required".parse(),
            Ok(HandlerRequirement::ButlerRequired)
        );
        assert_eq!(
            "artifact_required".parse(),
            Ok(AnswerRequirement::ArtifactRequired)
        );
        assert_eq!("butler".parse(), Ok(AnswerProvenance::Butler));
    }
}
