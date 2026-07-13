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
use tokio::net::TcpListener;

use crate::{
    domain::{
        ActorRef, AnswerRequest, ArtifactId, ArtifactKind, AskRepository, AuditWorkItemInput,
        ConnectOrchestrator, CreateFeature, CreateWorkItem, CreateWorkerRun, EventKind,
        FailRequest, FeatureId, InstanceId, MessageId, MessageKind, MessageTarget, ProjectId,
        PublishMessage, RepositoryId, RequestId, ThreadId, UpdateWorkItem, WorkItemId,
        WorkItemStatus, WorkerRole, WorkerRunId, validate_key,
    },
    store::{SqliteStore, StoreError},
};

mod dispatch;
mod schemas;
mod session;
mod validation;

use dispatch::dispatch;
use schemas::*;
use session::SessionRegistry;
use validation::*;

const PROTOCOL_VERSION: &str = "2026-06-18";
const SERVER_NAME: &str = "agent-sync-mcp";
const SERVER_TITLE: &str = "Agent Coordination Hub";
const SERVER_VERSION: &str = env!("CARGO_PKG_VERSION");

#[derive(Clone)]
struct AppState {
    store: SqliteStore,
    allowed_hosts: Arc<HashSet<String>>,
    database_path: Arc<PathBuf>,
    max_summary_bytes: u16,
    max_artifact_bytes: u32,
    sessions: SessionRegistry,
}

#[derive(Debug)]
struct RpcError {
    code: i64,
    message: String,
    http_status: StatusCode,
    data: Option<Value>,
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
    max_summary_bytes: u16,
    max_artifact_bytes: u32,
    allow_hosts: Vec<String>,
) -> anyhow::Result<()> {
    let mut allowed_hosts = HashSet::from(["127.0.0.1".to_owned(), "localhost".to_owned()]);
    allowed_hosts.extend(allow_hosts.into_iter().map(|host| host.to_lowercase()));

    let app = router(
        store,
        database_path.clone(),
        max_summary_bytes,
        max_artifact_bytes,
        allowed_hosts,
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

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    Ok(())
}

fn router(
    store: SqliteStore,
    database_path: PathBuf,
    max_summary_bytes: u16,
    max_artifact_bytes: u32,
    allowed_hosts: HashSet<String>,
) -> Router {
    Router::new()
        .route("/", get(root))
        .route("/healthz", get(healthz))
        .route("/mcp", get(mcp_get).post(mcp_post))
        .fallback(not_found)
        .layer(DefaultBodyLimit::max(2 * 1024 * 1024))
        .with_state(AppState {
            store,
            allowed_hosts: Arc::new(allowed_hosts),
            database_path: Arc::new(database_path),
            max_summary_bytes,
            max_artifact_bytes,
            sessions: SessionRegistry::default(),
        })
}

async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
    tracing::info!("Shutdown signal received");
}

async fn root(State(state): State<AppState>) -> Response {
    json_response(
        StatusCode::OK,
        json!({
            "server": SERVER_NAME,
            "title": SERVER_TITLE,
            "version": SERVER_VERSION,
            "database": state.database_path.to_string_lossy(),
            "max_summary_bytes": state.max_summary_bytes,
            "max_artifact_bytes": state.max_artifact_bytes,
            "mcp_endpoint": "/mcp",
            "health_endpoint": "/healthz",
        }),
    )
}

async fn healthz(State(state): State<AppState>) -> Response {
    match state.store.health_check().await {
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

    let is_initialize = payload
        .as_object()
        .is_some_and(|request| request.get("method").and_then(Value::as_str) == Some("initialize"));
    let session_header = headers.get("mcp-session-id");
    let session_id = if is_initialize {
        if session_header.is_some() {
            return transport_error_response(
                StatusCode::BAD_REQUEST,
                payload.get("id").cloned().unwrap_or(Value::Null),
                "session_not_allowed",
                "initialize must not include Mcp-Session-Id",
            );
        }
        None
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
        if !state.sessions.contains(&session_id) {
            return transport_error_response(
                StatusCode::NOT_FOUND,
                payload.get("id").cloned().unwrap_or(Value::Null),
                "session_not_found",
                "MCP session does not exist",
            );
        }
        Some(session_id)
    };
    if payload.as_object().is_some_and(|request| {
        request.get("method").and_then(Value::as_str) == Some("notifications/cancelled")
            && !request.contains_key("id")
    }) {
        if let (Some(session_id), Some(request_id)) = (
            session_id.as_deref(),
            payload
                .get("params")
                .and_then(Value::as_object)
                .and_then(|params| params.get("requestId")),
        ) {
            state.sessions.cancel(session_id, request_id);
        }
    }
    let in_flight = session_id.as_deref().and_then(|session_id| {
        payload
            .get("id")
            .and_then(|request_id| state.sessions.register(session_id, request_id))
    });

    let (status, response_body) = handle_rpc_payload(
        &state.store,
        state.max_summary_bytes,
        state.max_artifact_bytes,
        payload,
        in_flight.as_ref().map(|request| request.cancellation()),
    )
    .await;
    let mut response = match response_body {
        Some(body) => json_response(status, body),
        None => no_content_response(status),
    };
    if is_initialize && status == StatusCode::OK {
        let session_id = state.sessions.create();
        response.headers_mut().insert(
            "mcp-session-id",
            session_id.parse().expect("UUID is a valid header value"),
        );
    }
    response
}

async fn handle_rpc_payload(
    store: &SqliteStore,
    max_summary_bytes: u16,
    max_artifact_bytes: u32,
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
        payload,
        cancellation,
    )
    .await
}

async fn handle_single_request(
    store: &SqliteStore,
    max_summary_bytes: u16,
    max_artifact_bytes: u32,
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

    let request_id = request.get("id").cloned().unwrap_or(Value::Null);
    let is_notification = !request.contains_key("id");
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
    let payload = serde_json::to_vec(&body).unwrap_or_else(|_| b"{}".to_vec());
    Response::builder()
        .status(status)
        .header(CONTENT_TYPE, "application/json; charset=utf-8")
        .header(CACHE_CONTROL, "no-store")
        .header(CONTENT_LENGTH, payload.len().to_string())
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
