use std::{
    collections::HashSet,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use anyhow::Context;
use axum::{
    Router,
    body::{Body, Bytes},
    extract::{DefaultBodyLimit, State},
    http::{
        HeaderMap, StatusCode,
        header::{ALLOW, CACHE_CONTROL, CONTENT_LENGTH, CONTENT_TYPE, ORIGIN},
    },
    response::Response,
    routing::get,
};
use serde_json::{Map as JsonMap, Value, json};
use tokio::{net::TcpListener, sync::watch, time::MissedTickBehavior};
use tracing::Instrument;

use crate::{
    domain::{
        ActorRef, AnswerRequest, AnswerRequirement, ArtifactId, ArtifactKind, ArtifactListFilter,
        AskRepository, AuditOutcomeInput, AuditVerdict, CancelFeature, CompleteFeature,
        ConnectOrchestrator, CreateFeature, CreateWorkItem, CreateWorkerRun, DeliveryListStatus,
        EventKind, FailRequest, FeatureId, FeatureStatus, HandlerRequirement, IncreaseAuditDepth,
        InstanceId, MAX_AUDIT_WORK_ITEMS, MAX_CONTEXT_REFS, MAX_MESSAGE_RECIPIENTS,
        MAX_WORK_ITEM_DEPENDENCIES, MessageId, MessageKind, MessageListFilter, MessageTarget,
        ProjectId, PublishMessage, RepositoryId, RepositoryPresenceFilter, RequestId,
        RequestListFilter, RequestStatus, ThreadId, UpdateWorkItem, WaitOutcome, WorkItemId,
        WorkItemKind, WorkItemStatus, WorkerRole, WorkerRunId, WorkerRunListFilter,
        WorkerRunStatus, validate_key,
    },
    store::{EventWaitOptions, SqliteStore, StoreError},
};

mod dispatch;
mod schemas;
mod session;
mod validation;

use dispatch::dispatch;
use schemas::*;
use session::{SessionAccessError, SessionCreateError, SessionRegistry};
use validation::*;

const PROTOCOL_VERSION: &str = "2025-11-25";
const SERVER_NAME: &str = "raccoon-mcp";
const SERVER_TITLE: &str = "Agent Coordination Hub";
const SERVER_VERSION: &str = env!("CARGO_PKG_VERSION");

pub struct RuntimeConfig {
    pub max_summary_bytes: u16,
    pub max_artifact_bytes: u32,
    pub max_wait_seconds: u16,
    pub maintenance_interval: Duration,
    pub retention_interval: Option<Duration>,
    pub retention: Duration,
    pub enable_metrics: bool,
    pub session_idle_timeout: Duration,
    pub max_sessions: usize,
}

#[derive(Clone)]
struct AppState {
    store: SqliteStore,
    allowed_hosts: Arc<HashSet<String>>,
    database_path: Arc<PathBuf>,
    max_summary_bytes: u16,
    max_artifact_bytes: u32,
    max_wait_seconds: u16,
    sessions: SessionRegistry,
    metrics_enabled: bool,
}

#[derive(Debug)]
struct RpcError {
    code: i64,
    message: String,
    http_status: StatusCode,
    data: Option<Value>,
}

#[derive(Debug, Eq, PartialEq)]
struct RpcTraceFields {
    session_id: String,
    rpc_request_id: String,
    method: String,
    tool_name: String,
    raw_project_id: String,
    raw_repository_id: String,
    owner_repository_id: String,
    target_repository_id: String,
    instance_id: String,
    worker_run_id: String,
    coordination_request_id: String,
    feature_id: String,
    work_item_id: String,
    parent_work_item_id: String,
    addresses_audit_run_id: String,
    message_id: String,
    artifact_id: String,
    thread_id: String,
    in_reply_to: String,
    correlation_id: String,
    cancelled_rpc_request_id: String,
    resource_uri: String,
    page_cursor_field: String,
    page_cursor: String,
    dependency_count: usize,
    audit_scope_count: usize,
    target_repository_count: usize,
    is_notification: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct RpcResponseTrace {
    error_code: Option<String>,
    tool_error: bool,
}

impl RpcResponseTrace {
    fn from_body(body: &Value) -> Self {
        if let Some(error_code) = body.pointer("/error/data/code").and_then(Value::as_str) {
            return Self {
                error_code: Some(error_code.to_owned()),
                tool_error: false,
            };
        }
        Self {
            error_code: body
                .pointer("/result/structuredContent/error/code")
                .and_then(Value::as_str)
                .map(str::to_owned),
            tool_error: true,
        }
    }
}

impl RpcTraceFields {
    fn from_payload(session_id: Option<&str>, payload: &Value) -> Self {
        let request = payload.as_object();
        let params = request
            .and_then(|request| request.get("params"))
            .and_then(Value::as_object);
        let arguments = params
            .and_then(|params| params.get("arguments"))
            .and_then(Value::as_object);
        let field = |name: &str| {
            arguments
                .and_then(|arguments| arguments.get(name))
                .or_else(|| params.and_then(|params| params.get(name)))
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned()
        };
        let collection_count = |name: &str| {
            arguments
                .and_then(|arguments| arguments.get(name))
                .or_else(|| params.and_then(|params| params.get(name)))
                .and_then(Value::as_array)
                .map_or(0, Vec::len)
        };
        let (page_cursor_field, page_cursor) = [
            "after_feature_id",
            "after_work_item_id",
            "after_worker_run_id",
            "after_request_id",
            "after_delivery_id",
            "after_message_id",
            "after_artifact_id",
        ]
        .into_iter()
        .find_map(|name| {
            arguments
                .and_then(|arguments| arguments.get(name))
                .or_else(|| params.and_then(|params| params.get(name)))
                .map(json_rpc_id_text)
                .filter(|value| !value.is_empty())
                .map(|value| (name.to_owned(), value))
        })
        .unwrap_or_default();
        let rpc_request_id = request
            .and_then(|request| request.get("id"))
            .map(json_rpc_id_text)
            .unwrap_or_default();
        Self {
            session_id: session_id.unwrap_or_default().to_owned(),
            rpc_request_id,
            method: request
                .and_then(|request| request.get("method"))
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned(),
            tool_name: params
                .and_then(|params| params.get("name"))
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned(),
            raw_project_id: field("project_id"),
            raw_repository_id: field("repository_id"),
            owner_repository_id: field("owner_repository_id"),
            target_repository_id: field("target_repository_id"),
            instance_id: field("instance_id"),
            worker_run_id: field("worker_run_id"),
            coordination_request_id: field("request_id"),
            feature_id: field("feature_id"),
            work_item_id: field("work_item_id"),
            parent_work_item_id: field("parent_work_item_id"),
            addresses_audit_run_id: field("addresses_audit_run_id"),
            message_id: field("message_id"),
            artifact_id: field("artifact_id"),
            thread_id: field("thread_id"),
            in_reply_to: field("in_reply_to"),
            correlation_id: field("correlation_id"),
            cancelled_rpc_request_id: params
                .and_then(|params| params.get("requestId"))
                .map(json_rpc_id_text)
                .unwrap_or_default(),
            resource_uri: field("uri"),
            page_cursor_field,
            page_cursor,
            dependency_count: collection_count("dependency_ids"),
            audit_scope_count: collection_count("audit_scope_work_item_ids"),
            target_repository_count: collection_count("target_repository_ids"),
            is_notification: request.is_some_and(|request| !request.contains_key("id")),
        }
    }

    fn span(&self) -> tracing::Span {
        tracing::info_span!(
            "mcp_request",
            session_id = %self.session_id,
            rpc_request_id = %self.rpc_request_id,
            method = %self.method,
            tool_name = %self.tool_name,
            raw_project_id = %self.raw_project_id,
            raw_repository_id = %self.raw_repository_id,
            owner_repository_id = %self.owner_repository_id,
            target_repository_id = %self.target_repository_id,
            instance_id = %self.instance_id,
            worker_run_id = %self.worker_run_id,
            coordination_request_id = %self.coordination_request_id,
            feature_id = %self.feature_id,
            work_item_id = %self.work_item_id,
            parent_work_item_id = %self.parent_work_item_id,
            addresses_audit_run_id = %self.addresses_audit_run_id,
            message_id = %self.message_id,
            artifact_id = %self.artifact_id,
            thread_id = %self.thread_id,
            in_reply_to = %self.in_reply_to,
            correlation_id = %self.correlation_id,
            cancelled_rpc_request_id = %self.cancelled_rpc_request_id,
            resource_uri = %self.resource_uri,
            page_cursor_field = %self.page_cursor_field,
            page_cursor = %self.page_cursor,
            dependency_count = self.dependency_count,
            audit_scope_count = self.audit_scope_count,
            target_repository_count = self.target_repository_count,
            project_id = tracing::field::Empty,
            repository_id = tracing::field::Empty,
            duration_ms = tracing::field::Empty,
            outcome = tracing::field::Empty,
            error_code = tracing::field::Empty,
            wait_requested_timeout_seconds = tracing::field::Empty,
            wait_effective_timeout_seconds = tracing::field::Empty,
            wait_start_cursor = tracing::field::Empty,
            wait_end_cursor = tracing::field::Empty,
            wait_event_count = tracing::field::Empty,
            wait_elapsed_ms = tracing::field::Empty,
            wait_exit_reason = tracing::field::Empty,
        )
    }
}

fn json_rpc_id_text(value: &Value) -> String {
    match value {
        Value::String(value) => value.clone(),
        Value::Number(value) => value.to_string(),
        _ => String::new(),
    }
}

impl RpcError {
    fn new(code: i64, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            http_status: StatusCode::BAD_REQUEST,
            data: None,
        }
    }

    fn with_data(mut self, data: impl Into<Value>) -> Self {
        self.data = Some(data.into());
        self
    }

    fn with_status(mut self, status: StatusCode) -> Self {
        self.http_status = status;
        self
    }
}

pub async fn run_server(
    host: String,
    port: u16,
    store: SqliteStore,
    database_path: PathBuf,
    config: RuntimeConfig,
    allow_hosts: Vec<String>,
) -> anyhow::Result<()> {
    let mut allowed_hosts = HashSet::from(["127.0.0.1".to_owned(), "localhost".to_owned()]);
    allowed_hosts.extend(allow_hosts.into_iter().map(|host| host.to_lowercase()));

    let sessions = SessionRegistry::new(config.max_sessions, config.session_idle_timeout);
    let app = router(
        store.clone(),
        database_path.clone(),
        &config,
        allowed_hosts,
        sessions.clone(),
    );
    let listener = TcpListener::bind((host.as_str(), port))
        .await
        .with_context(|| format!("failed to bind {host}:{port}"))?;

    tracing::info!(
        "Serving MCP coordination hub on http://{}:{}/mcp using {}",
        host,
        listener.local_addr()?.port(),
        database_path.display()
    );

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let maintenance = tokio::spawn(maintenance_loop(
        store.clone(),
        config.maintenance_interval,
        config.retention_interval,
        config.retention,
        sessions.clone(),
        shutdown_rx,
    ));
    let serve_result = axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal(sessions, shutdown_tx.clone()))
        .await;
    let _ = shutdown_tx.send(true);
    maintenance.await?;
    store.expire_local_leases().await?;
    serve_result?;
    Ok(())
}

fn router(
    store: SqliteStore,
    database_path: PathBuf,
    config: &RuntimeConfig,
    allowed_hosts: HashSet<String>,
    sessions: SessionRegistry,
) -> Router {
    let mut router = Router::new()
        .route("/", get(root))
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .route("/mcp", get(mcp_get).post(mcp_post).delete(mcp_delete))
        .fallback(not_found)
        .layer(DefaultBodyLimit::max(2 * 1024 * 1024));
    if config.enable_metrics {
        router = router.route("/metrics", get(metrics));
    }
    router.with_state(AppState {
        store,
        allowed_hosts: Arc::new(allowed_hosts),
        database_path: Arc::new(database_path),
        max_summary_bytes: config.max_summary_bytes,
        max_artifact_bytes: config.max_artifact_bytes,
        max_wait_seconds: config.max_wait_seconds,
        sessions,
        metrics_enabled: config.enable_metrics,
    })
}

async fn shutdown_signal(sessions: SessionRegistry, shutdown: watch::Sender<bool>) {
    let _ = tokio::signal::ctrl_c().await;
    tracing::info!("shutdown signal received; cancelling waiters and stopping maintenance");
    let cancelled_sessions = sessions.terminate_all();
    let _ = shutdown.send(true);
    tracing::info!(cancelled_sessions, "graceful shutdown signal propagated");
}

#[cfg(test)]
async fn prepare_shutdown(
    store: &SqliteStore,
    sessions: &SessionRegistry,
) -> Result<(), StoreError> {
    let cancelled_sessions = sessions.terminate_all();
    store.expire_local_leases().await?;
    tracing::info!(cancelled_sessions, "sessions cancelled during shutdown");
    Ok(())
}

async fn maintenance_loop(
    store: SqliteStore,
    lease_interval: Duration,
    retention_interval: Option<Duration>,
    retention: Duration,
    sessions: SessionRegistry,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut lease_ticker =
        tokio::time::interval_at(tokio::time::Instant::now() + lease_interval, lease_interval);
    lease_ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut retention_ticker = retention_interval.map(|retention_interval| {
        let mut ticker = tokio::time::interval_at(
            tokio::time::Instant::now() + retention_interval,
            retention_interval,
        );
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        ticker
    });
    loop {
        tokio::select! {
            _ = lease_ticker.tick() => {
                let reaped_sessions = sessions.reap_idle();
                match store.run_lease_maintenance().await {
                    Ok(report) if reaped_sessions != 0
                        || report.disconnected_orchestrators != 0
                        || report.abandoned_workers != 0
                        || report.requeued_requests != 0 =>
                    {
                        tracing::info!(
                            reaped_sessions,
                            disconnected_orchestrators = report.disconnected_orchestrators,
                            abandoned_workers = report.abandoned_workers,
                            requeued_requests = report.requeued_requests,
                            "lease maintenance reaped expired state"
                        )
                    }
                    Ok(report) => tracing::debug!(
                        reaped_sessions,
                        disconnected_orchestrators = report.disconnected_orchestrators,
                        abandoned_workers = report.abandoned_workers,
                        requeued_requests = report.requeued_requests,
                        "lease maintenance pass completed without changes"
                    ),
                    Err(error) => tracing::error!(error = %error, "lease maintenance pass failed"),
                }
            }
            _ = optional_interval_tick(&mut retention_ticker) => {
                match store.run_retention(retention, 1000).await {
                    Ok(report) => tracing::info!(
                        deleted_events = report.deleted_events,
                        deleted_idempotency_records = report.deleted_idempotency_records,
                        "bounded retention pass completed"
                    ),
                    Err(error) => tracing::error!(error = %error, "retention pass failed"),
                }
            }
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    return;
                }
            }
        }
    }
}

async fn optional_interval_tick(interval: &mut Option<tokio::time::Interval>) {
    match interval {
        Some(interval) => {
            interval.tick().await;
        }
        None => std::future::pending::<()>().await,
    }
}

async fn root(State(state): State<AppState>) -> Response {
    json_response(
        StatusCode::OK,
        json!({
            "server": SERVER_NAME,
            "title": SERVER_TITLE,
            "version": SERVER_VERSION,
            "protocol_version": PROTOCOL_VERSION,
            "database": state.database_path.to_string_lossy(),
            "max_summary_bytes": state.max_summary_bytes,
            "max_artifact_bytes": state.max_artifact_bytes,
            "mcp_endpoint": "/mcp",
            "health_endpoint": "/healthz",
            "readiness_endpoint": "/readyz",
            "metrics_endpoint": state.metrics_enabled.then_some("/metrics"),
        }),
    )
}

async fn healthz() -> Response {
    json_response(
        StatusCode::OK,
        json!({
            "status": "ok",
            "server": SERVER_NAME,
            "version": SERVER_VERSION,
        }),
    )
}

async fn readyz(State(state): State<AppState>) -> Response {
    match state.store.readiness_check().await {
        Ok(()) => json_response(
            StatusCode::OK,
            json!({
                "status": "ok",
                "server": SERVER_NAME,
                "version": SERVER_VERSION,
                "database": state.database_path.to_string_lossy(),
            }),
        ),
        Err(error) => json_response(
            StatusCode::SERVICE_UNAVAILABLE,
            json!({
                "status": "unavailable",
                "server": SERVER_NAME,
                "error": error.to_string(),
            }),
        ),
    }
}

async fn metrics(State(state): State<AppState>) -> Response {
    match state.store.operational_metrics().await {
        Ok(metrics) => json_response(StatusCode::OK, json!(metrics)),
        Err(error) => json_response(
            StatusCode::SERVICE_UNAVAILABLE,
            json!({ "error": { "code": "metrics_unavailable", "message": error.to_string() } }),
        ),
    }
}

async fn mcp_get() -> Response {
    let mut response = json_response(
        StatusCode::METHOD_NOT_ALLOWED,
        json!({ "error": "This server only supports POST /mcp." }),
    );
    response
        .headers_mut()
        .insert(ALLOW, "POST".parse().unwrap());
    response
}

async fn not_found() -> Response {
    json_response(StatusCode::NOT_FOUND, json!({ "error": "Not found." }))
}

async fn mcp_post(State(state): State<AppState>, headers: HeaderMap, body: Bytes) -> Response {
    if let Some(origin) = headers.get(ORIGIN).and_then(|value| value.to_str().ok()) {
        if !origin_allowed(origin, &state.allowed_hosts) {
            return json_response(
                StatusCode::FORBIDDEN,
                json!({ "error": "Origin is not allowed." }),
            );
        }
    }

    if let Some(content_length) = headers.get(CONTENT_LENGTH) {
        if content_length
            .to_str()
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .is_none()
        {
            return json_response(
                StatusCode::BAD_REQUEST,
                json!({ "error": "Invalid Content-Length header." }),
            );
        }
    }

    let payload = match serde_json::from_slice::<Value>(&body) {
        Ok(payload) => payload,
        Err(error) => {
            return json_response(
                StatusCode::BAD_REQUEST,
                json_rpc_error_response(
                    Value::Null,
                    -32700,
                    "Parse error",
                    Some(machine_error_data("parse_error", error.to_string())),
                ),
            );
        }
    };

    let trace = RpcTraceFields::from_payload(
        headers
            .get("mcp-session-id")
            .and_then(|value| value.to_str().ok()),
        &payload,
    );
    let span = trace.span();
    let started_at = tokio::time::Instant::now();
    let response = async {
        let is_initialize = payload.as_object().is_some_and(|request| {
            request.get("method").and_then(Value::as_str) == Some("initialize")
        });
        let response_id = payload.get("id").cloned().unwrap_or(Value::Null);
        let session_header = headers.get("mcp-session-id");
        let (_, in_flight) = if is_initialize {
            if session_header.is_some() {
                return transport_error_response(
                    StatusCode::BAD_REQUEST,
                    payload.get("id").cloned().unwrap_or(Value::Null),
                    "session_not_allowed",
                    "initialize must not include Mcp-Session-Id",
                );
            }
            (None, None)
        } else {
            let session_id = match session_header {
                None => {
                    return transport_error_response(
                        StatusCode::BAD_REQUEST,
                        payload.get("id").cloned().unwrap_or(Value::Null),
                        "session_required",
                        "Mcp-Session-Id is required after initialize",
                    );
                }
                Some(value) if value.to_str().is_err() => {
                    return transport_error_response(
                        StatusCode::BAD_REQUEST,
                        payload.get("id").cloned().unwrap_or(Value::Null),
                        "invalid_session",
                        "Mcp-Session-Id must contain visible ASCII characters",
                    );
                }
                Some(value) => value.to_str().expect("checked above").to_owned(),
            };
            let Some(supplied_version) = headers
                .get("mcp-protocol-version")
                .and_then(|value| value.to_str().ok())
            else {
                return transport_error_response(
                    StatusCode::BAD_REQUEST,
                    payload.get("id").cloned().unwrap_or(Value::Null),
                    "unsupported_protocol_version",
                    "MCP-Protocol-Version is required after initialize",
                );
            };
            let is_cancellation = payload.as_object().is_some_and(|request| {
                request.get("method").and_then(Value::as_str) == Some("notifications/cancelled")
                    && !request.contains_key("id")
            });
            let request_id = payload.get("id");
            let access = if is_cancellation {
                let cancelled_request_id = payload
                    .get("params")
                    .and_then(Value::as_object)
                    .and_then(|params| params.get("requestId"));
                match cancelled_request_id {
                    Some(request_id) if is_valid_request_id(request_id) => state
                        .sessions
                        .cancel(&session_id, supplied_version, request_id)
                        .map(|()| None),
                    _ => state
                        .sessions
                        .validate(&session_id, supplied_version)
                        .map(|()| None),
                }
            } else if request_id.is_some_and(is_valid_request_id) {
                state
                    .sessions
                    .begin_request(
                        &session_id,
                        supplied_version,
                        request_id.expect("checked above"),
                    )
                    .map(Some)
            } else {
                state
                    .sessions
                    .validate(&session_id, supplied_version)
                    .map(|()| None)
            };
            let in_flight = match access {
                Ok(in_flight) => in_flight,
                Err(error) => {
                    return session_access_error_response(
                        error,
                        payload.get("id").cloned().unwrap_or(Value::Null),
                    );
                }
            };
            (Some(session_id), in_flight)
        };

        let (status, response_body) = handle_rpc_payload(
            &state.store,
            state.max_summary_bytes,
            state.max_artifact_bytes,
            state.max_wait_seconds,
            payload,
            in_flight.as_ref().map(|request| request.cancellation()),
        )
        .await;
        let mut response = match response_body {
            Some(body) => json_response(status, body),
            None => no_content_response(status),
        };
        if is_initialize && status == StatusCode::OK {
            let session_id = match state.sessions.create(PROTOCOL_VERSION) {
                Ok(session_id) => session_id,
                Err(SessionCreateError::CapacityExceeded) => {
                    return transport_error_response(
                        StatusCode::SERVICE_UNAVAILABLE,
                        response_id,
                        "session_capacity_exceeded",
                        "MCP session capacity is exhausted; retry after an idle session expires",
                    );
                }
            };
            response.headers_mut().insert(
                "mcp-session-id",
                session_id.parse().expect("UUID is a valid header value"),
            );
        }
        response
    }
    .instrument(span.clone())
    .await;
    let duration_ms = u64::try_from(started_at.elapsed().as_millis()).unwrap_or(u64::MAX);
    let response_trace = response.extensions().get::<RpcResponseTrace>();
    let (outcome, error_code) =
        rpc_trace_outcome(response.status(), response_trace, trace.is_notification);
    span.record("duration_ms", duration_ms);
    span.record("outcome", outcome);
    if let Some(error_code) = error_code {
        span.record("error_code", error_code);
    }
    if outcome == "cancelled" && trace.tool_name == "wait_for_events" {
        span.record("wait_elapsed_ms", duration_ms);
        span.record("wait_exit_reason", "cancelled");
    }
    tracing::info!(parent: &span, "MCP request completed");
    response
}

async fn mcp_delete(State(state): State<AppState>, headers: HeaderMap) -> Response {
    if let Some(origin) = headers.get(ORIGIN).and_then(|value| value.to_str().ok()) {
        if !origin_allowed(origin, &state.allowed_hosts) {
            return json_response(
                StatusCode::FORBIDDEN,
                json!({ "error": "Origin is not allowed." }),
            );
        }
    }
    let Some(session_id) = headers
        .get("mcp-session-id")
        .and_then(|value| value.to_str().ok())
    else {
        return transport_error_response(
            StatusCode::BAD_REQUEST,
            Value::Null,
            "session_required",
            "Mcp-Session-Id is required",
        );
    };
    let Some(protocol_version) = headers
        .get("mcp-protocol-version")
        .and_then(|value| value.to_str().ok())
    else {
        return transport_error_response(
            StatusCode::BAD_REQUEST,
            Value::Null,
            "unsupported_protocol_version",
            "MCP-Protocol-Version is required",
        );
    };
    match state.sessions.terminate(session_id, protocol_version) {
        Ok(()) => no_content_response(StatusCode::NO_CONTENT),
        Err(error) => session_access_error_response(error, Value::Null),
    }
}

async fn handle_rpc_payload(
    store: &SqliteStore,
    max_summary_bytes: u16,
    max_artifact_bytes: u32,
    max_wait_seconds: u16,
    payload: Value,
    cancellation: Option<Arc<tokio::sync::Notify>>,
) -> (StatusCode, Option<Value>) {
    if payload.is_array() {
        return (
            StatusCode::BAD_REQUEST,
            Some(json_rpc_error_response(
                Value::Null,
                -32600,
                "Invalid Request",
                Some(machine_error_data(
                    "batch_not_supported",
                    "JSON-RPC batching is not supported by MCP.",
                )),
            )),
        );
    }
    handle_single_request(
        store,
        max_summary_bytes,
        max_artifact_bytes,
        max_wait_seconds,
        payload,
        cancellation,
    )
    .await
}

async fn handle_single_request(
    store: &SqliteStore,
    max_summary_bytes: u16,
    max_artifact_bytes: u32,
    max_wait_seconds: u16,
    request: Value,
    cancellation: Option<Arc<tokio::sync::Notify>>,
) -> (StatusCode, Option<Value>) {
    let Some(request) = request.as_object() else {
        return (
            StatusCode::BAD_REQUEST,
            Some(json_rpc_error_response(
                Value::Null,
                -32600,
                "Invalid Request",
                None,
            )),
        );
    };

    let is_notification = !request.contains_key("id");
    if request.get("id").is_some_and(|id| !is_valid_request_id(id)) {
        return (
            StatusCode::BAD_REQUEST,
            Some(json_rpc_error_response(
                Value::Null,
                -32600,
                "Invalid Request",
                Some(machine_error_data(
                    "invalid_request_id",
                    "request id must be a string or number",
                )),
            )),
        );
    }
    let request_id = request.get("id").cloned().unwrap_or(Value::Null);
    if request.get("jsonrpc").and_then(Value::as_str) != Some("2.0") {
        return rpc_request_error(
            request_id,
            is_notification,
            RpcError::new(-32600, "Invalid Request").with_data(machine_error_data(
                "invalid_request",
                "Expected jsonrpc='2.0'.",
            )),
        );
    }
    let Some(method) = request
        .get("method")
        .and_then(Value::as_str)
        .filter(|method| !method.is_empty())
    else {
        return rpc_request_error(
            request_id,
            is_notification,
            RpcError::new(-32600, "Invalid Request")
                .with_data(machine_error_data("invalid_request", "Missing method.")),
        );
    };
    if is_notification
        && !matches!(
            method,
            "notifications/initialized" | "notifications/cancelled"
        )
    {
        tracing::warn!(method, "unsupported notification ignored");
        return (StatusCode::ACCEPTED, None);
    }
    let params = match request.get("params") {
        None | Some(Value::Null) => JsonMap::new(),
        Some(Value::Object(params)) => params.clone(),
        Some(_) => {
            return rpc_request_error(
                request_id,
                is_notification,
                RpcError::new(-32602, "Invalid params").with_data(machine_error_data(
                    "invalid_params",
                    "Expected params to be an object.",
                )),
            );
        }
    };
    let dispatched = dispatch(
        store,
        max_summary_bytes,
        max_artifact_bytes,
        max_wait_seconds,
        method,
        &params,
    );
    let result = if let Some(cancellation) = cancellation {
        tokio::select! {
            result = dispatched => Some(result),
            _ = cancellation.notified() => None,
        }
    } else {
        Some(dispatched.await)
    };
    let Some(result) = result else {
        return (StatusCode::ACCEPTED, None);
    };
    match result {
        Ok(_) if is_notification => (StatusCode::ACCEPTED, None),
        Ok(result) => (
            StatusCode::OK,
            Some(json!({ "jsonrpc": "2.0", "id": request_id, "result": result })),
        ),
        Err(error) => rpc_request_error(request_id, is_notification, error),
    }
}

fn rpc_trace_outcome(
    status: StatusCode,
    response_trace: Option<&RpcResponseTrace>,
    is_notification: bool,
) -> (&'static str, Option<&str>) {
    if let Some(trace) = response_trace.filter(|trace| trace.error_code.is_some()) {
        return (
            if trace.tool_error {
                "tool_error"
            } else {
                "error"
            },
            trace.error_code.as_deref(),
        );
    }
    if !status.is_success() {
        return ("error", Some("http_error"));
    }
    if status == StatusCode::ACCEPTED {
        if is_notification {
            ("notification", None)
        } else {
            ("cancelled", Some("cancelled"))
        }
    } else {
        ("success", None)
    }
}

fn transport_error_response(
    status: StatusCode,
    request_id: Value,
    code: &str,
    message: &str,
) -> Response {
    json_response(
        status,
        json_rpc_error_response(
            request_id,
            -32600,
            "Invalid Request",
            Some(machine_error_data(code, message)),
        ),
    )
}

fn session_access_error_response(error: SessionAccessError, request_id: Value) -> Response {
    match error {
        SessionAccessError::NotFound => transport_error_response(
            StatusCode::NOT_FOUND,
            request_id,
            "session_not_found",
            "MCP session does not exist",
        ),
        SessionAccessError::VersionMismatch { expected } => transport_error_response(
            StatusCode::BAD_REQUEST,
            request_id,
            "unsupported_protocol_version",
            &format!("MCP-Protocol-Version must match negotiated version {expected}"),
        ),
        SessionAccessError::InvalidRequestId => transport_error_response(
            StatusCode::BAD_REQUEST,
            Value::Null,
            "invalid_request_id",
            "request id must be a string or number",
        ),
        SessionAccessError::DuplicateRequestId => transport_error_response(
            StatusCode::CONFLICT,
            request_id,
            "duplicate_request_id",
            "request id is already in flight in this MCP session",
        ),
    }
}

fn is_valid_request_id(request_id: &Value) -> bool {
    matches!(request_id, Value::String(_) | Value::Number(_))
}

fn rpc_request_error(
    request_id: Value,
    is_notification: bool,
    error: RpcError,
) -> (StatusCode, Option<Value>) {
    if is_notification {
        (StatusCode::ACCEPTED, None)
    } else {
        (
            error.http_status,
            Some(json_rpc_error_response(
                request_id,
                error.code,
                &error.message,
                error.data,
            )),
        )
    }
}

fn json_rpc_error_response(
    request_id: Value,
    code: i64,
    message: &str,
    data: Option<Value>,
) -> Value {
    let mut error = JsonMap::new();
    error.insert("code".to_owned(), json!(code));
    error.insert("message".to_owned(), json!(message));
    if let Some(data) = data {
        error.insert("data".to_owned(), data);
    }
    json!({ "jsonrpc": "2.0", "id": request_id, "error": error })
}

fn json_response(status: StatusCode, body: Value) -> Response {
    let trace = RpcResponseTrace::from_body(&body);
    let payload = serde_json::to_vec(&body).unwrap_or_else(|_| b"{}".to_vec());
    Response::builder()
        .status(status)
        .header(CONTENT_TYPE, "application/json; charset=utf-8")
        .header(CACHE_CONTROL, "no-store")
        .header(CONTENT_LENGTH, payload.len().to_string())
        .extension(trace)
        .body(Body::from(payload))
        .unwrap()
}

fn no_content_response(status: StatusCode) -> Response {
    Response::builder()
        .status(status)
        .header(CACHE_CONTROL, "no-store")
        .header(CONTENT_LENGTH, "0")
        .body(Body::empty())
        .unwrap()
}

fn origin_allowed(origin: &str, allowed_hosts: &HashSet<String>) -> bool {
    origin_host(origin).is_some_and(|host| allowed_hosts.contains(&host.to_lowercase()))
}

fn origin_host(origin: &str) -> Option<String> {
    let after_scheme = origin.split_once("://").map_or(origin, |(_, rest)| rest);
    let authority = after_scheme.split('/').next().unwrap_or_default();
    if authority.starts_with('[') {
        return authority
            .split_once(']')
            .map(|(host, _)| host.trim_start_matches('[').to_owned());
    }
    authority
        .rsplit_once(':')
        .map_or(authority, |(host, _)| host)
        .split('@')
        .next_back()
        .filter(|host| !host.is_empty())
        .map(str::to_owned)
}

#[cfg(test)]
mod tests;
