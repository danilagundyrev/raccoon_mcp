use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OperationalMetrics {
    pub online_orchestrators: u64,
    pub online_repositories: u64,
    pub active_workers: u64,
    pub pending_deliveries: u64,
    pub open_requests: u64,
    pub expired_request_claims: u64,
    pub actively_claimed_work_items: u64,
    pub oldest_pending_delivery_at: Option<String>,
    pub oldest_pending_delivery_age_seconds: Option<u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LeaseMaintenanceReport {
    pub disconnected_orchestrators: u64,
    pub abandoned_workers: u64,
    pub requeued_requests: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RetentionReport {
    pub deleted_events: u64,
    pub deleted_idempotency_records: u64,
}
