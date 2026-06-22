use std::{
    collections::HashSet,
    path::{Path, PathBuf},
    sync::Arc,
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
use uuid::Uuid;

use crate::store::{StoreError, SyncStore};

const PROTOCOL_VERSION: &str = "2026-06-18";
const SERVER_NAME: &str = "agent-sync-mcp";
const SERVER_TITLE: &str = "Agent Sync MCP";
const SERVER_VERSION: &str = "0.1.0";

#[derive(Clone)]
struct AppState {
    store: SyncStore,
    allowed_hosts: Arc<HashSet<String>>,
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
    store: SyncStore,
    state_path: PathBuf,
    session_id: String,
    allow_hosts: Vec<String>,
) -> anyhow::Result<()> {
    let mut allowed_hosts = HashSet::from(["127.0.0.1".to_owned(), "localhost".to_owned()]);
    allowed_hosts.extend(allow_hosts.into_iter().map(|host| host.to_lowercase()));

    let app = router(store, allowed_hosts);
    let listener = TcpListener::bind((host.as_str(), port))
        .await
        .with_context(|| format!("failed to bind {host}:{port}"))?;

    tracing::info!(
        "Serving MCP sync server on http://{}:{}/mcp using session '{}' at {}",
        host,
        listener.local_addr()?.port(),
        session_id,
        state_path.display()
    );

    axum::serve(listener, app).await?;
    Ok(())
}

pub fn resolve_state_file(
    state_file: Option<&Path>,
    session_id: Option<&str>,
    state_dir: &Path,
) -> anyhow::Result<(PathBuf, String)> {
    if let Some(state_file) = state_file {
        let resolved_path = state_file.to_path_buf();
        let resolved_session_id = match session_id {
            Some(session_id) => slugify_session_id(session_id)?,
            None => resolved_path
                .file_stem()
                .map(|stem| stem.to_string_lossy().into_owned())
                .unwrap_or_else(|| "state".to_owned()),
        };
        return Ok((resolved_path, resolved_session_id));
    }

    let raw_session_id = session_id
        .map(str::to_owned)
        .unwrap_or_else(|| format!("session-{}", &Uuid::new_v4().simple().to_string()[..12]));
    let resolved_session_id = slugify_session_id(&raw_session_id)?;
    Ok((
        state_dir.join(format!("state_{resolved_session_id}.json")),
        resolved_session_id,
    ))
}

fn router(store: SyncStore, allowed_hosts: HashSet<String>) -> Router {
    Router::new()
        .route("/", get(root))
        .route("/healthz", get(healthz))
        .route("/mcp", get(mcp_get).post(mcp_post))
        .fallback(not_found)
        .layer(DefaultBodyLimit::max(64 * 1024 * 1024))
        .with_state(AppState {
            store,
            allowed_hosts: Arc::new(allowed_hosts),
        })
}

async fn root(State(state): State<AppState>) -> Response {
    json_response(
        StatusCode::OK,
        json!({
            "server": SERVER_NAME,
            "title": SERVER_TITLE,
            "version": SERVER_VERSION,
            "session": state.store.get_snapshot()["session"].clone(),
            "mcp_endpoint": "/mcp",
            "health_endpoint": "/healthz",
        }),
    )
}

async fn healthz(State(state): State<AppState>) -> Response {
    json_response(
        StatusCode::OK,
        json!({
            "status": "ok",
            "server": SERVER_NAME,
            "version": SERVER_VERSION,
            "session": state.store.get_snapshot()["session"].clone(),
        }),
    )
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

    let Some(content_length) = headers.get(CONTENT_LENGTH) else {
        return json_response(
            StatusCode::LENGTH_REQUIRED,
            json!({ "error": "Missing Content-Length header." }),
        );
    };
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

    let payload = match serde_json::from_slice::<Value>(&body) {
        Ok(payload) => payload,
        Err(error) => {
            return json_response(
                StatusCode::BAD_REQUEST,
                json_rpc_error_response(
                    Value::Null,
                    -32700,
                    "Parse error",
                    Some(json!(error.to_string())),
                ),
            );
        }
    };

    let (status, response_body) = handle_rpc_payload(&state.store, payload).await;
    match response_body {
        Some(body) => json_response(status, body),
        None => no_content_response(status),
    }
}

async fn handle_rpc_payload(store: &SyncStore, payload: Value) -> (StatusCode, Option<Value>) {
    if let Value::Array(items) = payload {
        if items.is_empty() {
            return (
                StatusCode::BAD_REQUEST,
                Some(json_rpc_error_response(
                    Value::Null,
                    -32600,
                    "Invalid Request",
                    None,
                )),
            );
        }

        let mut responses = Vec::new();
        let mut overall_status = StatusCode::OK;
        for item in items {
            let (item_status, item_response) = handle_single_request(store, item).await;
            if item_status.as_u16() >= 400 {
                overall_status = item_status;
            }
            if let Some(item_response) = item_response {
                responses.push(item_response);
            }
        }

        if responses.is_empty() {
            (StatusCode::ACCEPTED, None)
        } else {
            (overall_status, Some(Value::Array(responses)))
        }
    } else {
        handle_single_request(store, payload).await
    }
}

async fn handle_single_request(store: &SyncStore, request: Value) -> (StatusCode, Option<Value>) {
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
        let response = json_rpc_error_response(
            request_id,
            -32600,
            "Invalid Request",
            Some(json!("Expected jsonrpc='2.0'.")),
        );
        return (
            StatusCode::BAD_REQUEST,
            if is_notification {
                None
            } else {
                Some(response)
            },
        );
    }

    let Some(method) = request.get("method").and_then(Value::as_str) else {
        let response = json_rpc_error_response(
            request_id,
            -32600,
            "Invalid Request",
            Some(json!("Missing method.")),
        );
        return (
            StatusCode::BAD_REQUEST,
            if is_notification {
                None
            } else {
                Some(response)
            },
        );
    };
    if method.is_empty() {
        let response = json_rpc_error_response(
            request_id,
            -32600,
            "Invalid Request",
            Some(json!("Missing method.")),
        );
        return (
            StatusCode::BAD_REQUEST,
            if is_notification {
                None
            } else {
                Some(response)
            },
        );
    }

    let params = match request.get("params") {
        None | Some(Value::Null) => JsonMap::new(),
        Some(Value::Object(params)) => params.clone(),
        Some(_) => {
            let response = json_rpc_error_response(
                request_id,
                -32602,
                "Invalid params",
                Some(json!("Expected params to be an object.")),
            );
            return (
                StatusCode::BAD_REQUEST,
                if is_notification {
                    None
                } else {
                    Some(response)
                },
            );
        }
    };

    match dispatch(store, method, &params).await {
        Ok(result) => {
            if is_notification {
                (StatusCode::ACCEPTED, None)
            } else {
                (
                    StatusCode::OK,
                    Some(json!({
                        "jsonrpc": "2.0",
                        "id": request_id,
                        "result": result.unwrap_or(Value::Null),
                    })),
                )
            }
        }
        Err(error) => {
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
    }
}

async fn dispatch(
    store: &SyncStore,
    method: &str,
    params: &JsonMap<String, Value>,
) -> Result<Option<Value>, RpcError> {
    match method {
        "initialize" => Ok(Some(json!({
            "protocolVersion": PROTOCOL_VERSION,
            "capabilities": {
                "tools": { "listChanged": false },
                "resources": { "listChanged": false },
            },
            "serverInfo": {
                "name": SERVER_NAME,
                "title": SERVER_TITLE,
                "version": SERVER_VERSION,
            },
            "instructions": "Recommended entrypoint: call bootstrap_agent_session when the user tells you your role. The server already includes built-in defaults for backend-agent and client-agent. Use configure_agent_profile only if you need to override those defaults. Then use pull_updates with wait=false to check for already queued handoffs, write any returned handoff into the local file, and call ack_handoff only after that write succeeds. If you expected a startup handoff but got none, inspect sync://agent/{agent_name}/mailbox and sync://agent/{agent_name}/history before assuming there is no inbound work. Later use pull_updates with wait=true to block until a peer reply or task completion. End the loop with mark_task_complete when both repos are done.",
        }))),
        "notifications/initialized" | "notifications/cancelled" => Ok(None),
        "ping" => Ok(Some(json!({}))),
        "tools/list" => Ok(Some(json!({ "tools": tool_definitions() }))),
        "tools/call" => call_tool(store, params).await.map(Some),
        "resources/list" => Ok(Some(json!({ "resources": SyncStore::list_resources() }))),
        "resources/templates/list" => Ok(Some(json!({
            "resourceTemplates": SyncStore::list_resource_templates(),
        }))),
        "resources/read" => {
            let uri = require_string(params, "uri")?;
            match store.read_resource(&uri) {
                Ok(resource) => Ok(Some(resource)),
                Err(StoreError::Invalid(message)) => {
                    Err(RpcError::new(-32602, "Invalid params").with_data(json!(message)))
                }
                Err(error) => Err(internal_error(error)),
            }
        }
        _ => Err(RpcError::new(-32601, "Method not found")
            .with_status(StatusCode::NOT_FOUND)
            .with_data(json!(format!("Unsupported method: {method}")))),
    }
}

async fn call_tool(store: &SyncStore, params: &JsonMap<String, Value>) -> Result<Value, RpcError> {
    let tool_name = require_string(params, "name")?;
    let arguments = match params.get("arguments") {
        None | Some(Value::Null) => JsonMap::new(),
        Some(Value::Object(arguments)) => arguments.clone(),
        Some(_) => {
            return Err(RpcError::new(-32602, "Invalid params")
                .with_data(json!("'arguments' must be an object when provided.")));
        }
    };

    match tool_name.as_str() {
        "register_agent" => {
            let result = store
                .register_agent(
                    &require_string(&arguments, "agent_name")?,
                    optional_string(&arguments, "peer_name")?,
                    optional_string(&arguments, "repo_path")?,
                    optional_string(&arguments, "handoff_path")?,
                    optional_object(&arguments, "metadata")?,
                )
                .map_err(internal_error)?;
            Ok(tool_result(result, false))
        }
        "configure_agent_profile" => {
            let result = store
                .upsert_agent_profile(
                    &require_string(&arguments, "agent_name")?,
                    optional_string(&arguments, "peer_name")?,
                    optional_string(&arguments, "repo_path")?,
                    optional_string(&arguments, "handoff_path")?,
                    optional_string(&arguments, "notes")?,
                    optional_object(&arguments, "metadata")?,
                )
                .map_err(internal_error)?;
            Ok(tool_result(result, false))
        }
        "bootstrap_agent_session" => {
            let register = match arguments.get("register") {
                None => true,
                Some(Value::Bool(register)) => *register,
                Some(_) => {
                    return Err(RpcError::new(-32602, "Invalid params")
                        .with_data(json!("'register' must be a boolean when provided.")));
                }
            };
            let result = store
                .bootstrap_agent_session(
                    &require_string(&arguments, "agent_name")?,
                    register,
                    optional_string(&arguments, "peer_name")?,
                    optional_string(&arguments, "repo_path")?,
                    optional_string(&arguments, "handoff_path")?,
                    optional_object(&arguments, "metadata")?,
                )
                .map_err(internal_error)?;
            Ok(tool_result(result, false))
        }
        "send_handoff" => {
            let result = store.send_handoff(
                &require_string(&arguments, "from_agent")?,
                &require_string(&arguments, "to_agent")?,
                &require_string(&arguments, "content")?,
                optional_string(&arguments, "file_path")?,
                optional_string(&arguments, "summary")?,
                optional_object(&arguments, "metadata")?,
            );
            match result {
                Ok(result) => Ok(tool_result(result, false)),
                Err(StoreError::Invalid(message)) => {
                    Ok(tool_result(json!({ "error": message }), true))
                }
                Err(error) => Err(internal_error(error)),
            }
        }
        "pull_updates" => {
            let agent_name = require_string(&arguments, "agent_name")?;
            let wait = json_truthy(arguments.get("wait"));
            let timeout_seconds = optional_timeout(&arguments, "timeout_seconds")?;
            let result = if wait {
                store
                    .wait_for_event(&agent_name, timeout_seconds)
                    .await
                    .map_err(internal_error)?
            } else {
                match store.poll_event(&agent_name).map_err(internal_error)? {
                    Some(event) => event,
                    None => json!({
                        "event_type": "none",
                        "agent_name": agent_name,
                        "task_status": store.get_task_status().map_err(internal_error)?["status"].clone(),
                    }),
                }
            };
            Ok(tool_result(result, false))
        }
        "ack_handoff" => {
            let result = store.ack_handoff(
                &require_string(&arguments, "agent_name")?,
                require_int(&arguments, "message_id")?,
            );
            match result {
                Ok(result) => Ok(tool_result(result, false)),
                Err(StoreError::Invalid(message)) => {
                    Ok(tool_result(json!({ "error": message }), true))
                }
                Err(error) => Err(internal_error(error)),
            }
        }
        "mark_task_complete" => {
            let task = store
                .mark_task_complete(
                    &require_string(&arguments, "agent_name")?,
                    optional_string(&arguments, "summary")?,
                    optional_object(&arguments, "metadata")?,
                )
                .map_err(internal_error)?;
            Ok(tool_result(json!({ "task": task }), false))
        }
        _ => Err(RpcError::new(-32602, "Invalid params")
            .with_data(json!(format!("Unknown tool: {tool_name}")))),
    }
}

fn require_string(params: &JsonMap<String, Value>, field_name: &str) -> Result<String, RpcError> {
    match params.get(field_name).and_then(Value::as_str) {
        Some(value) if !value.trim().is_empty() => Ok(value.to_owned()),
        _ => Err(RpcError::new(
            -32602,
            format!("'{field_name}' must be a non-empty string."),
        )),
    }
}

fn optional_string(
    params: &JsonMap<String, Value>,
    field_name: &str,
) -> Result<Option<String>, RpcError> {
    match params.get(field_name) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(value)) => Ok(Some(value.clone())),
        Some(_) => Err(RpcError::new(
            -32602,
            format!("'{field_name}' must be a string when provided."),
        )),
    }
}

fn optional_object(
    params: &JsonMap<String, Value>,
    field_name: &str,
) -> Result<Option<JsonMap<String, Value>>, RpcError> {
    match params.get(field_name) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Object(value)) => Ok(Some(value.clone())),
        Some(_) => Err(RpcError::new(
            -32602,
            format!("'{field_name}' must be an object when provided."),
        )),
    }
}

fn optional_timeout(
    params: &JsonMap<String, Value>,
    field_name: &str,
) -> Result<Option<f64>, RpcError> {
    match params.get(field_name) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Number(value)) => {
            let Some(value) = value.as_f64() else {
                return Err(RpcError::new(
                    -32602,
                    format!("'{field_name}' must be a number when provided."),
                ));
            };
            if value < 0.0 {
                return Err(RpcError::new(
                    -32602,
                    format!("'{field_name}' must be zero or greater."),
                ));
            }
            Ok(Some(value))
        }
        Some(_) => Err(RpcError::new(
            -32602,
            format!("'{field_name}' must be a number when provided."),
        )),
    }
}

fn require_int(params: &JsonMap<String, Value>, field_name: &str) -> Result<i64, RpcError> {
    match params.get(field_name).and_then(Value::as_i64) {
        Some(value) => Ok(value),
        None => Err(RpcError::new(
            -32602,
            format!("'{field_name}' must be an integer."),
        )),
    }
}

fn json_truthy(value: Option<&Value>) -> bool {
    match value {
        None | Some(Value::Null) => false,
        Some(Value::Bool(value)) => *value,
        Some(Value::Number(value)) => value.as_f64().is_some_and(|value| value != 0.0),
        Some(Value::String(value)) => !value.is_empty(),
        Some(Value::Array(value)) => !value.is_empty(),
        Some(Value::Object(value)) => !value.is_empty(),
    }
}

fn tool_result(payload: Value, is_error: bool) -> Value {
    json!({
        "content": [
            {
                "type": "text",
                "text": serde_json::to_string_pretty(&payload).unwrap_or_else(|_| "{}".to_owned()),
            }
        ],
        "structuredContent": payload,
        "isError": is_error,
    })
}

fn tool_definitions() -> Value {
    json!([
        {
            "name": "configure_agent_profile",
            "description": "Create or update a persistent override for a role profile such as backend-agent or client-agent. Use this only when you need to change the built-in defaults.",
            "inputSchema": {
                "type": "object",
                "additionalProperties": false,
                "properties": {
                    "agent_name": { "type": "string" },
                    "peer_name": { "type": "string" },
                    "repo_path": { "type": "string" },
                    "handoff_path": { "type": "string" },
                    "notes": { "type": "string" },
                    "metadata": { "type": "object" },
                },
                "required": ["agent_name"],
            },
            "annotations": {
                "readOnlyHint": false,
                "destructiveHint": false,
                "idempotentHint": true,
                "openWorldHint": false,
            },
        },
        {
            "name": "bootstrap_agent_session",
            "description": "Bootstrap the current session from a built-in or overridden role profile. This is the recommended first tool call when the user tells you your role. If you expect the peer to have already sent a startup handoff, follow with pull_updates and the fallback guidance in sync://guide.",
            "inputSchema": {
                "type": "object",
                "additionalProperties": false,
                "properties": {
                    "agent_name": { "type": "string" },
                    "register": { "type": "boolean" },
                    "peer_name": { "type": "string" },
                    "repo_path": { "type": "string" },
                    "handoff_path": { "type": "string" },
                    "metadata": { "type": "object" },
                },
                "required": ["agent_name"],
            },
            "annotations": {
                "readOnlyHint": false,
                "destructiveHint": false,
                "idempotentHint": true,
                "openWorldHint": false,
            },
        },
        {
            "name": "register_agent",
            "description": "Register this Codex instance as an agent. Call once near the start of the session. If the peer already sent a handoff, the result will show pending_messages > 0.",
            "inputSchema": {
                "type": "object",
                "additionalProperties": false,
                "properties": {
                    "agent_name": { "type": "string" },
                    "peer_name": { "type": "string" },
                    "repo_path": { "type": "string" },
                    "handoff_path": { "type": "string" },
                    "metadata": { "type": "object" },
                },
                "required": ["agent_name"],
            },
            "annotations": {
                "readOnlyHint": false,
                "destructiveHint": false,
                "idempotentHint": true,
                "openWorldHint": false,
            },
        },
        {
            "name": "send_handoff",
            "description": "Send the current handoff note to the peer agent. Call this after writing the local handoff file.",
            "inputSchema": {
                "type": "object",
                "additionalProperties": false,
                "properties": {
                    "from_agent": { "type": "string" },
                    "to_agent": { "type": "string" },
                    "content": { "type": "string" },
                    "file_path": { "type": "string" },
                    "summary": { "type": "string" },
                    "metadata": { "type": "object" },
                },
                "required": ["from_agent", "to_agent", "content"],
            },
            "annotations": {
                "readOnlyHint": false,
                "destructiveHint": false,
                "idempotentHint": false,
                "openWorldHint": false,
            },
        },
        {
            "name": "pull_updates",
            "description": "Check for the next inbound event for this agent. By default this is non-blocking. Set wait=true to block until a handoff arrives, the task is completed, or timeout_seconds elapses. Returned handoffs remain pending until ack_handoff succeeds.",
            "inputSchema": {
                "type": "object",
                "additionalProperties": false,
                "properties": {
                    "agent_name": { "type": "string" },
                    "wait": { "type": "boolean" },
                    "timeout_seconds": { "type": "number" },
                },
                "required": ["agent_name"],
            },
            "annotations": {
                "readOnlyHint": true,
                "destructiveHint": false,
                "idempotentHint": false,
                "openWorldHint": false,
            },
        },
        {
            "name": "ack_handoff",
            "description": "Acknowledge an inbound handoff after writing message.content into the local handoff file. If startup recovery was needed, recover the handoff from mailbox/history first, then ack it.",
            "inputSchema": {
                "type": "object",
                "additionalProperties": false,
                "properties": {
                    "agent_name": { "type": "string" },
                    "message_id": { "type": "integer" },
                },
                "required": ["agent_name", "message_id"],
            },
            "annotations": {
                "readOnlyHint": false,
                "destructiveHint": false,
                "idempotentHint": true,
                "openWorldHint": false,
            },
        },
        {
            "name": "mark_task_complete",
            "description": "Declare that both agents have no more work to exchange. This wakes any waiting peer with a task_completed event.",
            "inputSchema": {
                "type": "object",
                "additionalProperties": false,
                "properties": {
                    "agent_name": { "type": "string" },
                    "summary": { "type": "string" },
                    "metadata": { "type": "object" },
                },
                "required": ["agent_name"],
            },
            "annotations": {
                "readOnlyHint": false,
                "destructiveHint": false,
                "idempotentHint": true,
                "openWorldHint": false,
            },
        },
    ])
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
    json!({
        "jsonrpc": "2.0",
        "id": request_id,
        "error": Value::Object(error),
    })
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

fn internal_error(error: StoreError) -> RpcError {
    RpcError::new(-32603, "Internal error").with_data(json!(error.to_string()))
}

fn slugify_session_id(raw_value: &str) -> anyhow::Result<String> {
    let mut slug = String::with_capacity(raw_value.len());
    let mut previous_dash = false;
    for character in raw_value.trim().chars() {
        if character.is_ascii_alphanumeric() || matches!(character, '.' | '_' | '-') {
            slug.push(character);
            previous_dash = false;
        } else if !previous_dash {
            slug.push('-');
            previous_dash = true;
        }
    }

    let slug = slug.trim_matches(['.', '-']).to_owned();
    if slug.is_empty() {
        anyhow::bail!("session_id must contain at least one filename-safe character.");
    }
    Ok(slug)
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
mod tests {
    use super::*;

    #[test]
    fn resolve_state_file_uses_session_specific_filename() {
        let state_dir = PathBuf::from("tmp");
        let (state_path, session_id) =
            resolve_state_file(None, Some("task-42"), &state_dir).unwrap();

        assert_eq!(session_id, "task-42");
        assert_eq!(state_path, PathBuf::from("tmp").join("state_task-42.json"));
    }

    #[test]
    fn slug_rejects_empty_session_id() {
        assert!(slugify_session_id(" @@@ ").is_err());
    }

    #[test]
    fn origin_check_uses_hostname() {
        let allowed_hosts = HashSet::from(["localhost".to_owned()]);
        assert!(origin_allowed("http://localhost:8765", &allowed_hosts));
        assert!(!origin_allowed("http://example.com", &allowed_hosts));
    }
}
