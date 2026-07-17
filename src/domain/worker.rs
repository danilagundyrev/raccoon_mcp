use std::{fmt, str::FromStr};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::{InstanceId, RepositoryId, WorkItemId, WorkerRunId};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkerRole {
    Executor,
    Butler,
    Auditor,
}

impl WorkerRole {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Executor => "executor",
            Self::Butler => "butler",
            Self::Auditor => "auditor",
        }
    }
}

impl fmt::Display for WorkerRole {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for WorkerRole {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "executor" => Ok(Self::Executor),
            "butler" => Ok(Self::Butler),
            "auditor" => Ok(Self::Auditor),
            _ => Err(format!(
                "unknown worker role '{value}'; expected 'executor', 'butler' or 'auditor'"
            )),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkerRunStatus {
    Created,
    Active,
    Completed,
    Failed,
    Abandoned,
}

impl WorkerRunStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Created => "created",
            Self::Active => "active",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Abandoned => "abandoned",
        }
    }
}

impl FromStr for WorkerRunStatus {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "created" => Ok(Self::Created),
            "active" => Ok(Self::Active),
            "completed" => Ok(Self::Completed),
            "failed" => Ok(Self::Failed),
            "abandoned" => Ok(Self::Abandoned),
            _ => Err(format!("unknown worker-run status '{value}'")),
        }
    }
}

#[derive(Clone, Debug)]
pub struct CreateWorkerRun {
    pub parent_instance_id: InstanceId,
    pub role: WorkerRole,
    pub task_summary: String,
    pub context_refs: Value,
    pub addresses_audit_run_id: Option<WorkerRunId>,
    pub work_item_id: Option<WorkItemId>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AuditScopeItem {
    pub work_item_id: WorkItemId,
    pub iteration: u8,
    pub max_iterations: u8,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WorkerRun {
    pub worker_run_id: WorkerRunId,
    pub repository_id: RepositoryId,
    pub parent_instance_id: InstanceId,
    pub role: WorkerRole,
    pub status: WorkerRunStatus,
    pub task_summary: String,
    pub context_refs: Value,
    pub result_summary: Option<String>,
    pub error_summary: Option<String>,
    pub audit_scope: Vec<AuditScopeItem>,
    pub addresses_audit_run_id: Option<WorkerRunId>,
    pub work_item_id: Option<WorkItemId>,
    pub created_at: String,
    pub activated_at: Option<String>,
    pub last_heartbeat_at: String,
    pub lease_expires_at: String,
    pub finished_at: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_supported_worker_roles() {
        assert_eq!("executor".parse(), Ok(WorkerRole::Executor));
        assert_eq!("butler".parse(), Ok(WorkerRole::Butler));
        assert_eq!("auditor".parse(), Ok(WorkerRole::Auditor));
        assert!("orchestrator".parse::<WorkerRole>().is_err());
    }
}
