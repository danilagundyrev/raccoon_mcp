use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::{InstanceId, Project, Repository, RepositoryPresence};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PresenceStatus {
    Online,
    Offline,
    Disconnected,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AgentInstance {
    pub instance_id: InstanceId,
    pub repository_id: super::RepositoryId,
    pub kind: String,
    pub status: PresenceStatus,
    pub capabilities: Value,
    pub started_at: String,
    pub last_heartbeat_at: String,
    pub lease_expires_at: String,
    pub disconnected_at: Option<String>,
}

#[derive(Clone, Debug)]
pub struct ConnectOrchestrator {
    pub project_key: String,
    pub project_display_name: Option<String>,
    pub repository_key: String,
    pub repository_display_name: Option<String>,
    pub canonical_path: String,
    pub instance_id: Option<InstanceId>,
    pub capabilities: Value,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ConnectResult {
    pub project: Project,
    pub repository: Repository,
    pub instance: AgentInstance,
    pub repositories: Vec<RepositoryPresence>,
    pub warnings: Vec<String>,
    pub next_actions: Vec<String>,
}
