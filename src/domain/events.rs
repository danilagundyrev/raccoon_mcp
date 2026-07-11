use std::str::FromStr;

use serde::{Deserialize, Serialize};

use super::{ProjectId, RepositoryId};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EventKind {
    MessageAvailable,
    WorkerRunCreated,
    WorkerRunActive,
    WorkerRunCompleted,
    WorkerRunFailed,
}

impl EventKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::MessageAvailable => "message_available",
            Self::WorkerRunCreated => "worker_run_created",
            Self::WorkerRunActive => "worker_run_active",
            Self::WorkerRunCompleted => "worker_run_completed",
            Self::WorkerRunFailed => "worker_run_failed",
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
            _ => Err(format!("unknown event kind '{value}'")),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EventEntityType {
    Message,
    WorkerRun,
}

impl EventEntityType {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Message => "message",
            Self::WorkerRun => "worker_run",
        }
    }
}

impl FromStr for EventEntityType {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "message" => Ok(Self::Message),
            "worker_run" => Ok(Self::WorkerRun),
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
    pub created_at: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WaitOutcome {
    Events,
    Timeout,
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
        assert!("handoff".parse::<EventKind>().is_err());
    }
}
