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

use crate::{
    domain::{
        ConnectOrchestrator, CreateWorkerRun, InstanceId, ProjectId, WorkerRole, WorkerRunId,
        validate_key,
    },
    store::{SqliteStore, StoreError},
};

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
    allow_hosts: Vec<String>,
) -> anyhow::Result<()> {
    let mut allowed_hosts = HashSet::from(["127.0.0.1".to_owned(), "localhost".to_owned()]);
    allowed_hosts.extend(allow_hosts.into_iter().map(|host| host.to_lowercase()));

    let app = router(
        store,
        database_path.clone(),
        max_summary_bytes,
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
                    Some(json!(error.to_string())),
                ),
            );
        }
    };

    let (status, response_body) =
        handle_rpc_payload(&state.store, state.max_summary_bytes, payload).await;
    match response_body {
        Some(body) => json_response(status, body),
        None => no_content_response(status),
    }
}

async fn handle_rpc_payload(
    store: &SqliteStore,
    max_summary_bytes: u16,
    payload: Value,
) -> (StatusCode, Option<Value>) {
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
            let (item_status, item_response) =
                handle_single_request(store, max_summary_bytes, item).await;
            if item_status.is_client_error() || item_status.is_server_error() {
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
        handle_single_request(store, max_summary_bytes, payload).await
    }
}

async fn handle_single_request(
    store: &SqliteStore,
    max_summary_bytes: u16,
    request: Value,
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
            RpcError::new(-32600, "Invalid Request").with_data(json!("Expected jsonrpc='2.0'.")),
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
            RpcError::new(-32600, "Invalid Request").with_data(json!("Missing method.")),
        );
    };
    let params = match request.get("params") {
        None | Some(Value::Null) => JsonMap::new(),
        Some(Value::Object(params)) => params.clone(),
        Some(_) => {
            return rpc_request_error(
                request_id,
                is_notification,
                RpcError::new(-32602, "Invalid params")
                    .with_data(json!("Expected params to be an object.")),
            );
        }
    };

    match dispatch(store, max_summary_bytes, method, &params).await {
        Ok(_) if is_notification => (StatusCode::ACCEPTED, None),
        Ok(result) => (
            StatusCode::OK,
            Some(json!({ "jsonrpc": "2.0", "id": request_id, "result": result })),
        ),
        Err(error) => rpc_request_error(request_id, is_notification, error),
    }
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

async fn dispatch(
    store: &SqliteStore,
    max_summary_bytes: u16,
    method: &str,
    params: &JsonMap<String, Value>,
) -> Result<Value, RpcError> {
    match method {
        "initialize" => Ok(json!({
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
            "instructions": "This is a project/repository coordination hub. Start with connect_orchestrator using a user-defined project_key and repository_key. Keep the returned instance lease alive with heartbeat. Use list_repositories or hub://project/{project_id}/overview to discover peers. Before spawning a child, call create_worker_run and pass its worker_run_id to the child. The child activates and completes/fails that run through MCP; it must not register as a separate repository. Messaging APIs are not implemented yet.",
        })),
        "notifications/initialized" | "notifications/cancelled" => Ok(Value::Null),
        "ping" => Ok(json!({})),
        "tools/list" => Ok(json!({ "tools": tool_definitions(max_summary_bytes) })),
        "tools/call" => call_tool(store, max_summary_bytes, params).await,
        "resources/list" => Ok(json!({ "resources": list_resources() })),
        "resources/templates/list" => Ok(json!({
            "resourceTemplates": list_resource_templates(),
        })),
        "resources/read" => {
            let uri = require_string(params, "uri")?;
            read_resource(store, max_summary_bytes, &uri).await
        }
        _ => Err(RpcError::new(-32601, "Method not found")
            .with_status(StatusCode::NOT_FOUND)
            .with_data(json!(format!("Unsupported method: {method}")))),
    }
}

async fn call_tool(
    store: &SqliteStore,
    max_summary_bytes: u16,
    params: &JsonMap<String, Value>,
) -> Result<Value, RpcError> {
    let tool_name = require_string(params, "name")?;
    let arguments = optional_arguments(params)?;
    let result = match tool_name.as_str() {
        "connect_orchestrator" => {
            let project_key =
                validate_key("project_key", &require_string(&arguments, "project_key")?)
                    .map_err(invalid_argument)?;
            let repository_key = validate_key(
                "repository_key",
                &require_string(&arguments, "repository_key")?,
            )
            .map_err(invalid_argument)?;
            let repository_path = require_string(&arguments, "repository_path")?;
            let canonical_path = canonical_repository_path(&repository_path)?;
            let instance_id = optional_string(&arguments, "instance_id")?
                .map(|value| InstanceId::parse(&value).map_err(invalid_uuid))
                .transpose()?;
            let capabilities = arguments
                .get("capabilities")
                .cloned()
                .unwrap_or_else(|| json!({}));
            if !capabilities.is_object() {
                return Err(invalid_argument(
                    "capabilities must be an object".to_owned(),
                ));
            }
            store
                .connect_orchestrator(ConnectOrchestrator {
                    project_key,
                    project_display_name: optional_string(&arguments, "project_display_name")?,
                    repository_key,
                    repository_display_name: optional_string(
                        &arguments,
                        "repository_display_name",
                    )?,
                    canonical_path,
                    instance_id,
                    capabilities,
                })
                .await
                .map(|value| json!(value))
        }
        "heartbeat" => {
            let instance_id = parse_instance_id(&arguments)?;
            store.heartbeat(instance_id).await.map(|value| json!(value))
        }
        "disconnect_orchestrator" => {
            let instance_id = parse_instance_id(&arguments)?;
            store
                .disconnect_orchestrator(instance_id)
                .await
                .map(|value| json!(value))
        }
        "create_worker_run" => {
            let parent_instance_id = parse_instance_id(&arguments)?;
            let role = require_string(&arguments, "role")?
                .parse::<WorkerRole>()
                .map_err(invalid_argument)?;
            let task_summary = validated_summary(&arguments, "task_summary", max_summary_bytes)?;
            let context_refs = arguments
                .get("context_refs")
                .cloned()
                .unwrap_or_else(|| json!([]));
            if !context_refs.is_array() {
                return Err(invalid_argument(
                    "'context_refs' must be an array when provided".to_owned(),
                ));
            }
            store
                .create_worker_run(CreateWorkerRun {
                    parent_instance_id,
                    role,
                    task_summary,
                    context_refs,
                })
                .await
                .map(|value| json!(value))
        }
        "activate_worker_run" => {
            let worker_run_id = parse_worker_run_id(&arguments)?;
            store
                .activate_worker_run(worker_run_id)
                .await
                .map(|value| json!(value))
        }
        "heartbeat_worker_run" => {
            let worker_run_id = parse_worker_run_id(&arguments)?;
            store
                .heartbeat_worker_run(worker_run_id)
                .await
                .map(|value| json!(value))
        }
        "complete_worker_run" => {
            let worker_run_id = parse_worker_run_id(&arguments)?;
            let result_summary =
                validated_summary(&arguments, "result_summary", max_summary_bytes)?;
            store
                .complete_worker_run(worker_run_id, &result_summary)
                .await
                .map(|value| json!(value))
        }
        "fail_worker_run" => {
            let worker_run_id = parse_worker_run_id(&arguments)?;
            let error_summary = validated_summary(&arguments, "error_summary", max_summary_bytes)?;
            store
                .fail_worker_run(worker_run_id, &error_summary)
                .await
                .map(|value| json!(value))
        }
        "get_worker_run" => {
            let worker_run_id = parse_worker_run_id(&arguments)?;
            store
                .get_worker_run(worker_run_id)
                .await
                .map(|value| json!(value))
        }
        "list_repositories" => {
            let project_id = parse_project_id(&arguments)?;
            store.list_repositories(project_id).await.map(
                |repositories| json!({ "project_id": project_id, "repositories": repositories }),
            )
        }
        "get_project_overview" => {
            let project_id = parse_project_id(&arguments)?;
            store
                .project_overview(project_id)
                .await
                .map(|value| json!(value))
        }
        _ => {
            return Err(RpcError::new(-32602, "Invalid params")
                .with_data(json!(format!("Unknown tool: {tool_name}"))));
        }
    };

    Ok(match result {
        Ok(payload) => tool_result(payload, false),
        Err(error) => tool_result(store_error_payload(&error), true),
    })
}

async fn read_resource(
    store: &SqliteStore,
    max_summary_bytes: u16,
    uri: &str,
) -> Result<Value, RpcError> {
    if uri == "hub://guide" {
        return Ok(resource_contents(
            uri,
            json!({
                "server": SERVER_NAME,
                "workflow": "multi-repository coordination hub",
                "recommended_entrypoint": "connect_orchestrator",
                "identity": ["project", "repository", "orchestrator instance"],
                "lease_note": "Call heartbeat before lease_expires_at. Repository identity and future mailbox state remain stable across orchestrator instances.",
                "max_summary_bytes": max_summary_bytes,
                "summary_note": "Summary fields are compact UTF-8 indexes. Put full context in an artifact instead of enlarging every inbox/event payload.",
                "worker_flow": [
                    "The orchestrator calls create_worker_run before spawning a child.",
                    "The child receives worker_run_id and calls activate_worker_run.",
                    "The child calls complete_worker_run or fail_worker_run before exiting.",
                    "A worker never calls connect_orchestrator and never creates a repository identity."
                ],
                "current_scope": "Project/repository presence and minimal worker-run lifecycle. Messaging is not implemented yet.",
            }),
        ));
    }

    if let Some(project_id) = uri
        .strip_prefix("hub://project/")
        .and_then(|suffix| suffix.strip_suffix("/overview"))
    {
        let project_id = ProjectId::parse(project_id).map_err(invalid_uuid)?;
        let overview = store
            .project_overview(project_id)
            .await
            .map_err(store_rpc_error)?;
        return Ok(resource_contents(uri, json!(overview)));
    }

    if let Some(project_id) = uri
        .strip_prefix("hub://project/")
        .and_then(|suffix| suffix.strip_suffix("/repositories"))
    {
        let project_id = ProjectId::parse(project_id).map_err(invalid_uuid)?;
        let repositories = store
            .list_repositories(project_id)
            .await
            .map_err(store_rpc_error)?;
        return Ok(resource_contents(
            uri,
            json!({ "project_id": project_id, "repositories": repositories }),
        ));
    }

    Err(RpcError::new(-32602, "Invalid params")
        .with_data(json!(format!("Unsupported resource URI: {uri}"))))
}

fn parse_instance_id(arguments: &JsonMap<String, Value>) -> Result<InstanceId, RpcError> {
    InstanceId::parse(&require_string(arguments, "instance_id")?).map_err(invalid_uuid)
}

fn parse_project_id(arguments: &JsonMap<String, Value>) -> Result<ProjectId, RpcError> {
    ProjectId::parse(&require_string(arguments, "project_id")?).map_err(invalid_uuid)
}

fn parse_worker_run_id(arguments: &JsonMap<String, Value>) -> Result<WorkerRunId, RpcError> {
    WorkerRunId::parse(&require_string(arguments, "worker_run_id")?).map_err(invalid_uuid)
}

fn validated_summary(
    arguments: &JsonMap<String, Value>,
    field_name: &str,
    max_summary_bytes: u16,
) -> Result<String, RpcError> {
    let summary = require_string(arguments, field_name)?;
    if summary.len() > usize::from(max_summary_bytes) {
        return Err(invalid_argument(format!(
            "'{field_name}' must not exceed {max_summary_bytes} UTF-8 bytes"
        )));
    }
    Ok(summary)
}

fn canonical_repository_path(raw_path: &str) -> Result<String, RpcError> {
    std::fs::canonicalize(Path::new(raw_path))
        .map(|path| path.to_string_lossy().into_owned())
        .map_err(|error| {
            invalid_argument(format!(
                "repository_path '{raw_path}' cannot be canonicalized: {error}"
            ))
        })
}

fn optional_arguments(params: &JsonMap<String, Value>) -> Result<JsonMap<String, Value>, RpcError> {
    match params.get("arguments") {
        None | Some(Value::Null) => Ok(JsonMap::new()),
        Some(Value::Object(arguments)) => Ok(arguments.clone()),
        Some(_) => Err(invalid_argument(
            "'arguments' must be an object when provided".to_owned(),
        )),
    }
}

fn require_string(params: &JsonMap<String, Value>, field_name: &str) -> Result<String, RpcError> {
    match params.get(field_name).and_then(Value::as_str) {
        Some(value) if !value.trim().is_empty() => Ok(value.to_owned()),
        _ => Err(invalid_argument(format!(
            "'{field_name}' must be a non-empty string"
        ))),
    }
}

fn optional_string(
    params: &JsonMap<String, Value>,
    field_name: &str,
) -> Result<Option<String>, RpcError> {
    match params.get(field_name) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(value)) => Ok(Some(value.clone())),
        Some(_) => Err(invalid_argument(format!(
            "'{field_name}' must be a string when provided"
        ))),
    }
}

fn invalid_argument(message: String) -> RpcError {
    RpcError::new(-32602, "Invalid params").with_data(json!(message))
}

fn invalid_uuid(error: uuid::Error) -> RpcError {
    invalid_argument(format!("invalid UUID: {error}"))
}

fn store_rpc_error(error: StoreError) -> RpcError {
    match error {
        StoreError::NotFound(message) => {
            RpcError::new(-32602, "Not found").with_data(json!(message))
        }
        StoreError::Conflict(message) => {
            RpcError::new(-32602, "Invalid state").with_data(json!(message))
        }
        other => RpcError::new(-32603, "Internal error").with_data(json!(other.to_string())),
    }
}

fn store_error_payload(error: &StoreError) -> Value {
    let code = match error {
        StoreError::NotFound(_) => "not_found",
        StoreError::Conflict(_) => "conflict",
        StoreError::Database(_)
        | StoreError::Io(_)
        | StoreError::Json(_)
        | StoreError::InvalidUuid(_) => "internal_error",
    };
    json!({ "error": { "code": code, "message": error.to_string() } })
}

fn tool_result(payload: Value, is_error: bool) -> Value {
    json!({
        "content": [{
            "type": "text",
            "text": serde_json::to_string_pretty(&payload).unwrap_or_else(|_| "{}".to_owned()),
        }],
        "structuredContent": payload,
        "isError": is_error,
    })
}

fn tool_definitions(max_summary_bytes: u16) -> Value {
    json!([
        {
            "name": "connect_orchestrator",
            "description": "Join or reconnect an orchestrator to a user-defined project and repository. This is the required first call.",
            "inputSchema": {
                "type": "object",
                "additionalProperties": false,
                "properties": {
                    "project_key": { "type": "string", "maxLength": 64 },
                    "project_display_name": { "type": "string" },
                    "repository_key": { "type": "string", "maxLength": 64 },
                    "repository_display_name": { "type": "string" },
                    "repository_path": { "type": "string" },
                    "instance_id": { "type": "string", "format": "uuid" },
                    "capabilities": { "type": "object" }
                },
                "required": ["project_key", "repository_key", "repository_path"]
            },
            "annotations": {
                "readOnlyHint": false,
                "destructiveHint": false,
                "idempotentHint": false,
                "openWorldHint": false
            }
        },
        {
            "name": "heartbeat",
            "description": "Extend an active orchestrator instance lease.",
            "inputSchema": id_schema("instance_id"),
            "annotations": {
                "readOnlyHint": false,
                "destructiveHint": false,
                "idempotentHint": true,
                "openWorldHint": false
            }
        },
        {
            "name": "disconnect_orchestrator",
            "description": "Explicitly mark an orchestrator instance disconnected without deleting repository identity.",
            "inputSchema": id_schema("instance_id"),
            "annotations": {
                "readOnlyHint": false,
                "destructiveHint": false,
                "idempotentHint": true,
                "openWorldHint": false
            }
        },
        {
            "name": "create_worker_run",
            "description": "Create a repository-scoped executor or butler run before spawning a child agent. Pass the returned worker_run_id to that child.",
            "inputSchema": {
                "type": "object",
                "additionalProperties": false,
                "properties": {
                    "instance_id": { "type": "string", "format": "uuid" },
                    "role": { "type": "string", "enum": ["executor", "butler"] },
                    "task_summary": {
                        "type": "string",
                        "maxLength": max_summary_bytes,
                        "description": "Compact summary. The server enforces max_summary_bytes as UTF-8 bytes; put full context in artifacts."
                    },
                    "context_refs": { "type": "array", "items": { "type": "object" } }
                },
                "required": ["instance_id", "role", "task_summary"]
            },
            "annotations": {
                "readOnlyHint": false,
                "destructiveHint": false,
                "idempotentHint": false,
                "openWorldHint": false
            }
        },
        {
            "name": "activate_worker_run",
            "description": "Activate a worker run from the spawned child before it starts work.",
            "inputSchema": id_schema("worker_run_id"),
            "annotations": {
                "readOnlyHint": false,
                "destructiveHint": false,
                "idempotentHint": true,
                "openWorldHint": false
            }
        },
        {
            "name": "heartbeat_worker_run",
            "description": "Extend the lease of an active worker run at a natural work checkpoint.",
            "inputSchema": id_schema("worker_run_id"),
            "annotations": {
                "readOnlyHint": false,
                "destructiveHint": false,
                "idempotentHint": true,
                "openWorldHint": false
            }
        },
        {
            "name": "complete_worker_run",
            "description": "Complete an active worker run with a compact result summary.",
            "inputSchema": worker_finish_schema("result_summary", max_summary_bytes),
            "annotations": {
                "readOnlyHint": false,
                "destructiveHint": false,
                "idempotentHint": true,
                "openWorldHint": false
            }
        },
        {
            "name": "fail_worker_run",
            "description": "Fail a created or active worker run with a compact error summary.",
            "inputSchema": worker_finish_schema("error_summary", max_summary_bytes),
            "annotations": {
                "readOnlyHint": false,
                "destructiveHint": false,
                "idempotentHint": true,
                "openWorldHint": false
            }
        },
        {
            "name": "get_worker_run",
            "description": "Read one worker run without exposing it as a repository or mailbox identity.",
            "inputSchema": id_schema("worker_run_id"),
            "annotations": {
                "readOnlyHint": true,
                "destructiveHint": false,
                "idempotentHint": true,
                "openWorldHint": false
            }
        },
        {
            "name": "list_repositories",
            "description": "List repositories in a project with their current online instance counts.",
            "inputSchema": id_schema("project_id"),
            "annotations": {
                "readOnlyHint": true,
                "destructiveHint": false,
                "idempotentHint": true,
                "openWorldHint": false
            }
        },
        {
            "name": "get_project_overview",
            "description": "Read the compact project and repository presence overview.",
            "inputSchema": id_schema("project_id"),
            "annotations": {
                "readOnlyHint": true,
                "destructiveHint": false,
                "idempotentHint": true,
                "openWorldHint": false
            }
        }
    ])
}

fn id_schema(field_name: &str) -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "properties": {
            field_name: { "type": "string", "format": "uuid" }
        },
        "required": [field_name]
    })
}

fn worker_finish_schema(summary_field: &str, max_summary_bytes: u16) -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "properties": {
            "worker_run_id": { "type": "string", "format": "uuid" },
            summary_field: {
                "type": "string",
                "maxLength": max_summary_bytes,
                "description": "Compact summary limited by the server's max_summary_bytes setting."
            }
        },
        "required": ["worker_run_id", summary_field]
    })
}

fn list_resources() -> Value {
    json!([{
        "uri": "hub://guide",
        "name": "Coordination hub guide",
        "description": "Machine-readable bootstrap guidance and current implementation scope.",
        "mimeType": "application/json"
    }])
}

fn list_resource_templates() -> Value {
    json!([
        {
            "uriTemplate": "hub://project/{project_id}/overview",
            "name": "Project overview",
            "description": "Project metadata and repository presence.",
            "mimeType": "application/json"
        },
        {
            "uriTemplate": "hub://project/{project_id}/repositories",
            "name": "Project repositories",
            "description": "Repositories and online orchestrator counts.",
            "mimeType": "application/json"
        }
    ])
}

fn resource_contents(uri: &str, payload: Value) -> Value {
    json!({
        "contents": [{
            "uri": uri,
            "mimeType": "application/json",
            "text": serde_json::to_string_pretty(&payload).unwrap_or_else(|_| "{}".to_owned())
        }]
    })
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
mod tests {
    use super::*;

    #[test]
    fn origin_check_uses_hostname() {
        let allowed_hosts = HashSet::from(["localhost".to_owned()]);
        assert!(origin_allowed("http://localhost:8765", &allowed_hosts));
        assert!(!origin_allowed("http://example.com", &allowed_hosts));
    }

    #[test]
    fn resource_template_schema_contains_dynamic_id_key() {
        let schema = id_schema("project_id");
        assert_eq!(schema["required"][0], "project_id");
        assert_eq!(schema["properties"]["project_id"]["format"], "uuid");
    }

    #[test]
    fn summary_limit_counts_utf8_bytes() {
        let mut arguments = JsonMap::new();
        arguments.insert("summary".to_owned(), json!("аб"));

        assert!(validated_summary(&arguments, "summary", 3).is_err());
        assert_eq!(validated_summary(&arguments, "summary", 4).unwrap(), "аб");
    }

    #[test]
    fn worker_schema_uses_configured_summary_limit() {
        let schema = worker_finish_schema("result_summary", 4096);
        assert_eq!(schema["properties"]["result_summary"]["maxLength"], 4096);
    }
}
