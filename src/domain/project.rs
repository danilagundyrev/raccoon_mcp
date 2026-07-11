use serde::{Deserialize, Serialize};

use super::{ProjectId, RepositoryId};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Project {
    pub project_id: ProjectId,
    pub project_key: String,
    pub display_name: String,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Repository {
    pub repository_id: RepositoryId,
    pub project_id: ProjectId,
    pub repository_key: String,
    pub display_name: String,
    pub canonical_path: String,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RepositoryPresence {
    #[serde(flatten)]
    pub repository: Repository,
    pub online_instances: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ProjectOverview {
    pub project: Project,
    pub repositories: Vec<RepositoryPresence>,
}
