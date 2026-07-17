use std::str::FromStr;

use serde::{Deserialize, Serialize};

use super::{ArtifactId, FeatureId, InstanceId, ProjectId, RepositoryId, WorkItemId, WorkerRunId};

#[derive(Clone, Debug)]
pub struct CreateFeature {
    pub parent_instance_id: InstanceId,
    pub title: String,
    pub summary: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FeatureStatus {
    Active,
    Completed,
    Cancelled,
}

impl FeatureStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Completed => "completed",
            Self::Cancelled => "cancelled",
        }
    }
}

impl FromStr for FeatureStatus {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "active" => Ok(Self::Active),
            "completed" => Ok(Self::Completed),
            "cancelled" => Ok(Self::Cancelled),
            _ => Err(format!("unknown feature status '{value}'")),
        }
    }
}

#[derive(Clone, Debug)]
pub struct CompleteFeature {
    pub parent_instance_id: InstanceId,
    pub feature_id: FeatureId,
    pub expected_revision: u64,
}

#[derive(Clone, Debug)]
pub struct CancelFeature {
    pub parent_instance_id: InstanceId,
    pub feature_id: FeatureId,
    pub expected_revision: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Feature {
    pub feature_id: FeatureId,
    pub project_id: ProjectId,
    pub created_by_instance_id: InstanceId,
    pub coordinator_repository_id: RepositoryId,
    pub title: String,
    pub summary: String,
    pub status: FeatureStatus,
    pub revision: u64,
    pub created_at: String,
    pub updated_at: String,
    pub completed_at: Option<String>,
    pub cancelled_at: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkItemStatus {
    Planned,
    Ready,
    Claimed,
    InProgress,
    Blocked,
    Completed,
    Failed,
    Cancelled,
}

impl WorkItemStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Planned => "planned",
            Self::Ready => "ready",
            Self::Claimed => "claimed",
            Self::InProgress => "in_progress",
            Self::Blocked => "blocked",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }
}

impl FromStr for WorkItemStatus {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "planned" => Ok(Self::Planned),
            "ready" => Ok(Self::Ready),
            "claimed" => Ok(Self::Claimed),
            "in_progress" => Ok(Self::InProgress),
            "blocked" => Ok(Self::Blocked),
            "completed" => Ok(Self::Completed),
            "failed" => Ok(Self::Failed),
            "cancelled" => Ok(Self::Cancelled),
            _ => Err(format!("unknown work-item status '{value}'")),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkItemKind {
    Implementation,
    Audit,
    Remediation,
}

impl WorkItemKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Implementation => "implementation",
            Self::Audit => "audit",
            Self::Remediation => "remediation",
        }
    }
}

impl FromStr for WorkItemKind {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "implementation" => Ok(Self::Implementation),
            "audit" => Ok(Self::Audit),
            "remediation" => Ok(Self::Remediation),
            _ => Err(format!("unknown work-item kind '{value}'")),
        }
    }
}

#[derive(Clone, Debug)]
pub struct CreateWorkItem {
    pub parent_instance_id: InstanceId,
    pub feature_id: FeatureId,
    pub parent_work_item_id: Option<WorkItemId>,
    pub owner_repository_id: RepositoryId,
    pub title: String,
    pub summary: String,
    pub kind: WorkItemKind,
    pub audit_scope_work_item_ids: Vec<WorkItemId>,
    pub addresses_audit_run_id: Option<WorkerRunId>,
    pub dependency_ids: Vec<WorkItemId>,
    pub priority: i16,
    pub max_audit_iterations: u8,
}

#[derive(Clone, Debug)]
pub struct UpdateWorkItem {
    pub parent_instance_id: InstanceId,
    pub work_item_id: WorkItemId,
    pub expected_revision: u64,
    pub title: Option<String>,
    pub summary: Option<String>,
    pub owner_repository_id: Option<RepositoryId>,
    pub parent_work_item_id: Option<Option<WorkItemId>>,
    pub dependency_ids: Option<Vec<WorkItemId>>,
    pub priority: Option<i16>,
    pub status: Option<WorkItemStatus>,
    pub implementation_artifact_id: Option<ArtifactId>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WorkItem {
    pub work_item_id: WorkItemId,
    pub feature_id: FeatureId,
    pub parent_work_item_id: Option<WorkItemId>,
    pub owner_repository_id: RepositoryId,
    pub assignee_instance_id: Option<InstanceId>,
    pub title: String,
    pub summary: String,
    pub kind: WorkItemKind,
    pub audit_scope_work_item_ids: Vec<WorkItemId>,
    pub addresses_audit_run_id: Option<WorkerRunId>,
    pub dependency_ids: Vec<WorkItemId>,
    pub priority: i16,
    pub status: WorkItemStatus,
    pub revision: u64,
    pub max_audit_iterations: u8,
    pub claim_expires_at: Option<String>,
    pub implementation_artifact_id: Option<ArtifactId>,
    pub latest_audit_artifact_id: Option<ArtifactId>,
    pub created_at: String,
    pub updated_at: String,
    pub finished_at: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WorkItemPage {
    pub items: Vec<WorkItem>,
    pub next_cursor: Option<WorkItemId>,
    pub has_more: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_work_item_statuses() {
        assert_eq!("in_progress".parse(), Ok(WorkItemStatus::InProgress));
        assert!("open".parse::<WorkItemStatus>().is_err());
    }

    #[test]
    fn parses_work_item_kinds() {
        assert_eq!("implementation".parse(), Ok(WorkItemKind::Implementation));
        assert_eq!("audit".parse(), Ok(WorkItemKind::Audit));
        assert_eq!("remediation".parse(), Ok(WorkItemKind::Remediation));
        assert!("review".parse::<WorkItemKind>().is_err());
    }

    #[test]
    fn parses_feature_statuses() {
        assert_eq!("active".parse(), Ok(FeatureStatus::Active));
        assert_eq!("completed".parse(), Ok(FeatureStatus::Completed));
        assert!("open".parse::<FeatureStatus>().is_err());
    }
}
