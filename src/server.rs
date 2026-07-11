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
        ConnectOrchestrator, CreateWorkerRun, EventKind, FailRequest, InstanceId, MessageId,
        MessageKind, MessageTarget, ProjectId, PublishMessage, RepositoryId, RequestId, ThreadId,
        WorkItemId, WorkerRole, WorkerRunId, validate_key,
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
    max_artifact_bytes: u32,
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
                    Some(json!(error.to_string())),
                ),
            );
        }
    };

    let (status, response_body) = handle_rpc_payload(
        &state.store,
        state.max_summary_bytes,
        state.max_artifact_bytes,
        payload,
    )
    .await;
    match response_body {
        Some(body) => json_response(status, body),
        None => no_content_response(status),
    }
}

async fn handle_rpc_payload(
    store: &SqliteStore,
    max_summary_bytes: u16,
    max_artifact_bytes: u32,
    payload: Value,
) -> (StatusCode, Option<Value>) {
    if payload.is_array() {
        return (
            StatusCode::BAD_REQUEST,
            Some(json_rpc_error_response(
                Value::Null,
                -32600,
                "Invalid Request",
                Some(json!("JSON-RPC batching is not supported by MCP.")),
            )),
        );
    }
    handle_single_request(store, max_summary_bytes, max_artifact_bytes, payload).await
}

async fn handle_single_request(
    store: &SqliteStore,
    max_summary_bytes: u16,
    max_artifact_bytes: u32,
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

    match dispatch(
        store,
        max_summary_bytes,
        max_artifact_bytes,
        method,
        &params,
    )
    .await
    {
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
    max_artifact_bytes: u32,
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
            "instructions": "This is a project/repository coordination hub. Start with connect_orchestrator using a user-defined project_key and repository_key. Keep the returned instance lease alive with heartbeat. Use list_repositories or hub://project/{project_id}/overview to discover peers. Before spawning a child, call create_worker_run and pass its worker_run_id to the child. Roles are executor, butler and auditor. An auditor receives one or more audit_work_items with a bounded max_iterations policy and publishes an audit_report artifact. If findings require remediation, create exactly one executor with addresses_audit_run_id for the complete audit report; do not create one executor per finding. The child activates and completes/fails its run through MCP and never registers as a repository. Use ask_repository plus claim/renew/release/answer/fail for durable questions that require exclusive handling. Use publish_message for ordinary delivery, wait_for_events for efficient repository-scoped waiting, list_inbox for compact headers, read_artifact only when full content is needed, and ack_message after processing. An active wait renews its actor lease; omit timeout_seconds for no server-side timeout.",
        })),
        "notifications/initialized" | "notifications/cancelled" => Ok(Value::Null),
        "ping" => Ok(json!({})),
        "tools/list" => Ok(json!({
            "tools": tool_definitions(max_summary_bytes, max_artifact_bytes)
        })),
        "tools/call" => call_tool(store, max_summary_bytes, max_artifact_bytes, params).await,
        "resources/list" => Ok(json!({ "resources": list_resources() })),
        "resources/templates/list" => Ok(json!({
            "resourceTemplates": list_resource_templates(),
        })),
        "resources/read" => {
            let uri = require_string(params, "uri")?;
            read_resource(store, max_summary_bytes, max_artifact_bytes, &uri).await
        }
        _ => Err(RpcError::new(-32601, "Method not found")
            .with_status(StatusCode::NOT_FOUND)
            .with_data(json!(format!("Unsupported method: {method}")))),
    }
}

async fn call_tool(
    store: &SqliteStore,
    max_summary_bytes: u16,
    max_artifact_bytes: u32,
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
            let context_refs = validated_context_refs(&arguments)?;
            store
                .create_worker_run(CreateWorkerRun {
                    parent_instance_id,
                    role,
                    task_summary,
                    context_refs,
                    audit_work_items: optional_audit_work_items(&arguments)?,
                    addresses_audit_run_id: optional_string(&arguments, "addresses_audit_run_id")?
                        .map(|value| WorkerRunId::parse(&value).map_err(invalid_uuid))
                        .transpose()?,
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
        "publish_message" => {
            let actor = parse_actor(&arguments)?;
            let kind = require_string(&arguments, "kind")?
                .parse::<MessageKind>()
                .map_err(invalid_argument)?;
            let broadcast = optional_bool(&arguments, "broadcast")?.unwrap_or(false);
            let target_repository_ids = optional_repository_ids(&arguments)?;
            let target = match (broadcast, target_repository_ids) {
                (true, None) => MessageTarget::Broadcast,
                (false, Some(repository_ids)) if !repository_ids.is_empty() => {
                    MessageTarget::Repositories(repository_ids)
                }
                (true, Some(_)) => {
                    return Err(invalid_argument(
                        "use either broadcast=true or target_repository_ids, not both".to_owned(),
                    ));
                }
                _ => {
                    return Err(invalid_argument(
                        "provide broadcast=true or at least one target_repository_id".to_owned(),
                    ));
                }
            };
            let summary = validated_summary(&arguments, "summary", max_summary_bytes)?;
            let body = optional_string(&arguments, "body")?;
            if body
                .as_ref()
                .is_some_and(|body| body.len() > max_artifact_bytes as usize)
            {
                return Err(invalid_argument(format!(
                    "'body' must not exceed {max_artifact_bytes} UTF-8 bytes"
                )));
            }
            let artifact_kind = optional_string(&arguments, "artifact_kind")?
                .unwrap_or_else(|| "note".to_owned())
                .parse::<ArtifactKind>()
                .map_err(invalid_argument)?;
            let thread_id = optional_string(&arguments, "thread_id")?
                .map(|value| ThreadId::parse(&value).map_err(invalid_uuid))
                .transpose()?;
            let in_reply_to = optional_string(&arguments, "in_reply_to")?
                .map(|value| MessageId::parse(&value).map_err(invalid_uuid))
                .transpose()?;
            let correlation_id = optional_string(&arguments, "correlation_id")?;
            if correlation_id
                .as_ref()
                .is_some_and(|value| value.len() > 128)
            {
                return Err(invalid_argument(
                    "'correlation_id' must not exceed 128 UTF-8 bytes".to_owned(),
                ));
            }
            store
                .publish_message(PublishMessage {
                    actor,
                    kind,
                    target,
                    summary,
                    body,
                    artifact_kind,
                    artifact_title: optional_string(&arguments, "artifact_title")?,
                    content_type: optional_string(&arguments, "content_type")?
                        .unwrap_or_else(|| "text/markdown".to_owned()),
                    thread_id,
                    correlation_id,
                    in_reply_to,
                })
                .await
                .map(|value| json!(value))
        }
        "wait_for_events" => {
            let actor = parse_actor(&arguments)?;
            let cursor = optional_nonnegative_i64(&arguments, "cursor")?.unwrap_or(0);
            let kinds = optional_event_kinds(&arguments)?;
            let timeout = optional_timeout_seconds(&arguments)?.map(Duration::from_secs);
            let limit = optional_event_limit(&arguments)?.unwrap_or(20);
            store
                .wait_for_events(actor, cursor, &kinds, timeout, limit)
                .await
                .map(|value| json!(value))
        }
        "ask_repository" => {
            let actor = parse_actor(&arguments)?;
            let target_repository_id =
                RepositoryId::parse(&require_string(&arguments, "target_repository_id")?)
                    .map_err(invalid_uuid)?;
            let question_summary =
                validated_summary(&arguments, "question_summary", max_summary_bytes)?;
            let body = validated_optional_body(&arguments, max_artifact_bytes)?;
            let context_refs = validated_context_refs(&arguments)?;
            store
                .ask_repository(AskRepository {
                    actor,
                    target_repository_id,
                    question_summary,
                    body,
                    context_refs,
                })
                .await
                .map(|value| json!(value))
        }
        "read_request" => {
            let actor = parse_actor(&arguments)?;
            let request_id = parse_request_id(&arguments)?;
            store
                .read_request(actor, request_id)
                .await
                .map(|value| json!(value))
        }
        "claim_request" => {
            let actor = parse_actor(&arguments)?;
            let request_id = parse_request_id(&arguments)?;
            store
                .claim_request(actor, request_id)
                .await
                .map(|value| json!(value))
        }
        "renew_request_claim" => {
            let actor = parse_actor(&arguments)?;
            let request_id = parse_request_id(&arguments)?;
            store
                .renew_request_claim(actor, request_id)
                .await
                .map(|value| json!(value))
        }
        "release_request_claim" => {
            let actor = parse_actor(&arguments)?;
            let request_id = parse_request_id(&arguments)?;
            store
                .release_request_claim(actor, request_id)
                .await
                .map(|value| json!(value))
        }
        "answer_request" => {
            let actor = parse_actor(&arguments)?;
            let request_id = parse_request_id(&arguments)?;
            let answer_summary =
                validated_summary(&arguments, "answer_summary", max_summary_bytes)?;
            let body = validated_optional_body(&arguments, max_artifact_bytes)?;
            let content_type = optional_string(&arguments, "content_type")?
                .unwrap_or_else(|| "text/markdown".to_owned());
            store
                .answer_request(AnswerRequest {
                    actor,
                    request_id,
                    answer_summary,
                    body,
                    content_type,
                })
                .await
                .map(|value| json!(value))
        }
        "fail_request" => {
            let actor = parse_actor(&arguments)?;
            let request_id = parse_request_id(&arguments)?;
            let retryable = optional_bool(&arguments, "retryable")?.ok_or_else(|| {
                invalid_argument("'retryable' is required and must be a boolean".to_owned())
            })?;
            let summary = validated_summary(&arguments, "summary", max_summary_bytes)?;
            store
                .fail_request(FailRequest {
                    actor,
                    request_id,
                    retryable,
                    summary,
                })
                .await
                .map(|value| json!(value))
        }
        "list_inbox" => {
            let actor = parse_actor(&arguments)?;
            let after_delivery_id = optional_nonnegative_i64(&arguments, "after_delivery_id")?;
            let limit = optional_limit(&arguments)?.unwrap_or(20);
            store
                .list_inbox(actor, after_delivery_id, limit)
                .await
                .map(|value| json!(value))
        }
        "read_message" => {
            let actor = parse_actor(&arguments)?;
            let message_id = parse_message_id(&arguments)?;
            store
                .read_message(actor, message_id)
                .await
                .map(|value| json!(value))
        }
        "read_artifact" => {
            let actor = parse_actor(&arguments)?;
            let artifact_id = ArtifactId::parse(&require_string(&arguments, "artifact_id")?)
                .map_err(invalid_uuid)?;
            store
                .read_artifact(actor, artifact_id)
                .await
                .map(|value| json!(value))
        }
        "ack_message" => {
            let actor = parse_actor(&arguments)?;
            let message_id = parse_message_id(&arguments)?;
            store
                .ack_message(actor, message_id)
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
    max_artifact_bytes: u32,
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
                "max_artifact_bytes": max_artifact_bytes,
                "summary_note": "Summary fields are compact UTF-8 indexes. Put full context in an artifact instead of enlarging every inbox/event payload.",
                "worker_flow": [
                    "The orchestrator calls create_worker_run before spawning a child.",
                    "The child receives worker_run_id and calls activate_worker_run.",
                    "The child calls complete_worker_run or fail_worker_run before exiting.",
                    "A worker never calls connect_orchestrator and never creates a repository identity."
                ],
                "audit_flow": [
                    "Create one auditor run for one or more work_item_ids; max_iterations defaults to one and is fixed per item.",
                    "The auditor publishes one audit_report artifact covering its complete scope.",
                    "If remediation is needed, create at most one executor run with addresses_audit_run_id; that executor addresses the whole report.",
                    "A later audit increments each item's iteration independently and is rejected after that item's depth is exhausted."
                ],
                "request_flow": [
                    "Create a durable directed question with ask_repository; the target receives request_available.",
                    "One target actor atomically claims it and renews the claim while working; an expired claim can be taken over.",
                    "answer_request atomically closes the request and delivers an answer message/artifact to the requester.",
                    "A retryable failure returns the request to pending; a terminal failure closes it and notifies the requester."
                ],
                "messaging_flow": [
                    "Publish an immutable summary and optional artifact with publish_message.",
                    "Call wait_for_events with the last event cursor; omit timeout_seconds for an unbounded server wait. The server renews the waiting actor lease.",
                    "Use list_inbox to read pending headers without artifact bodies.",
                    "Use read_message/read_artifact only for selected items.",
                    "Call ack_message after processing; broadcast recipients acknowledge independently."
                ],
                "current_scope": "Project/repository presence, worker runs, immutable messaging, artifacts, reliable per-repository delivery, cursor-based event waiting and claimed repository requests.",
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

fn parse_message_id(arguments: &JsonMap<String, Value>) -> Result<MessageId, RpcError> {
    MessageId::parse(&require_string(arguments, "message_id")?).map_err(invalid_uuid)
}

fn parse_request_id(arguments: &JsonMap<String, Value>) -> Result<RequestId, RpcError> {
    RequestId::parse(&require_string(arguments, "request_id")?).map_err(invalid_uuid)
}

fn parse_actor(arguments: &JsonMap<String, Value>) -> Result<ActorRef, RpcError> {
    let instance_id = optional_string(arguments, "instance_id")?
        .map(|value| InstanceId::parse(&value).map_err(invalid_uuid))
        .transpose()?;
    let worker_run_id = optional_string(arguments, "worker_run_id")?
        .map(|value| WorkerRunId::parse(&value).map_err(invalid_uuid))
        .transpose()?;
    match (instance_id, worker_run_id) {
        (Some(instance_id), None) => Ok(ActorRef::Orchestrator(instance_id)),
        (None, Some(worker_run_id)) => Ok(ActorRef::Worker(worker_run_id)),
        _ => Err(invalid_argument(
            "provide exactly one actor: instance_id or worker_run_id".to_owned(),
        )),
    }
}

fn optional_bool(
    arguments: &JsonMap<String, Value>,
    field_name: &str,
) -> Result<Option<bool>, RpcError> {
    match arguments.get(field_name) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Bool(value)) => Ok(Some(*value)),
        Some(_) => Err(invalid_argument(format!(
            "'{field_name}' must be a boolean when provided"
        ))),
    }
}

fn validated_optional_body(
    arguments: &JsonMap<String, Value>,
    max_artifact_bytes: u32,
) -> Result<Option<String>, RpcError> {
    let body = optional_string(arguments, "body")?;
    if body
        .as_ref()
        .is_some_and(|body| body.len() > max_artifact_bytes as usize)
    {
        return Err(invalid_argument(format!(
            "'body' must not exceed {max_artifact_bytes} UTF-8 bytes"
        )));
    }
    Ok(body)
}

fn validated_context_refs(arguments: &JsonMap<String, Value>) -> Result<Value, RpcError> {
    match arguments.get("context_refs") {
        None => Ok(json!([])),
        Some(Value::Array(values)) => {
            if let Some((index, _)) = values
                .iter()
                .enumerate()
                .find(|(_, value)| !value.is_object())
            {
                return Err(invalid_argument(format!(
                    "'context_refs[{index}]' must be an object"
                )));
            }
            Ok(Value::Array(values.clone()))
        }
        Some(_) => Err(invalid_argument(
            "'context_refs' must be an array when provided".to_owned(),
        )),
    }
}

fn optional_nonnegative_i64(
    arguments: &JsonMap<String, Value>,
    field_name: &str,
) -> Result<Option<i64>, RpcError> {
    match arguments.get(field_name) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Number(value)) => value
            .as_i64()
            .filter(|value| *value >= 0)
            .map(Some)
            .ok_or_else(|| {
                invalid_argument(format!(
                    "'{field_name}' must be a non-negative integer when provided"
                ))
            }),
        Some(_) => Err(invalid_argument(format!(
            "'{field_name}' must be a non-negative integer when provided"
        ))),
    }
}

fn optional_limit(arguments: &JsonMap<String, Value>) -> Result<Option<u8>, RpcError> {
    match optional_nonnegative_i64(arguments, "limit")? {
        None => Ok(None),
        Some(value @ 1..=100) => Ok(Some(value as u8)),
        Some(_) => Err(invalid_argument(
            "'limit' must be between 1 and 100".to_owned(),
        )),
    }
}

fn optional_event_limit(arguments: &JsonMap<String, Value>) -> Result<Option<u8>, RpcError> {
    match optional_nonnegative_i64(arguments, "limit")? {
        None => Ok(None),
        Some(value @ 1..=50) => Ok(Some(value as u8)),
        Some(_) => Err(invalid_argument(
            "'limit' must be between 1 and 50 for wait_for_events".to_owned(),
        )),
    }
}

fn optional_timeout_seconds(arguments: &JsonMap<String, Value>) -> Result<Option<u64>, RpcError> {
    match optional_nonnegative_i64(arguments, "timeout_seconds")? {
        None => Ok(None),
        Some(value @ 0..=65_535) => Ok(Some(value as u64)),
        Some(_) => Err(invalid_argument(
            "'timeout_seconds' must be between 0 and 65535".to_owned(),
        )),
    }
}

fn optional_event_kinds(arguments: &JsonMap<String, Value>) -> Result<Vec<EventKind>, RpcError> {
    match arguments.get("kinds") {
        None | Some(Value::Null) => Ok(Vec::new()),
        Some(Value::Array(values)) => values
            .iter()
            .map(|value| {
                value
                    .as_str()
                    .ok_or_else(|| {
                        invalid_argument("every event kind must be a string".to_owned())
                    })?
                    .parse::<EventKind>()
                    .map_err(invalid_argument)
            })
            .collect(),
        Some(_) => Err(invalid_argument(
            "'kinds' must be an array when provided".to_owned(),
        )),
    }
}

fn optional_repository_ids(
    arguments: &JsonMap<String, Value>,
) -> Result<Option<Vec<RepositoryId>>, RpcError> {
    match arguments.get("target_repository_ids") {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Array(values)) => values
            .iter()
            .map(|value| {
                let value = value.as_str().ok_or_else(|| {
                    invalid_argument("every target_repository_id must be a UUID string".to_owned())
                })?;
                RepositoryId::parse(value).map_err(invalid_uuid)
            })
            .collect::<Result<Vec<_>, _>>()
            .map(Some),
        Some(_) => Err(invalid_argument(
            "'target_repository_ids' must be an array when provided".to_owned(),
        )),
    }
}

fn optional_audit_work_items(
    arguments: &JsonMap<String, Value>,
) -> Result<Vec<AuditWorkItemInput>, RpcError> {
    match arguments.get("audit_work_items") {
        None | Some(Value::Null) => Ok(Vec::new()),
        Some(Value::Array(values)) => values
            .iter()
            .map(|value| {
                let object = value.as_object().ok_or_else(|| {
                    invalid_argument("every audit_work_item must be an object".to_owned())
                })?;
                let work_item_id = WorkItemId::parse(&require_string(object, "work_item_id")?)
                    .map_err(invalid_uuid)?;
                let max_iterations =
                    optional_nonnegative_i64(object, "max_iterations")?.unwrap_or(1);
                if !(1..=10).contains(&max_iterations) {
                    return Err(invalid_argument(
                        "audit_work_item.max_iterations must be between 1 and 10".to_owned(),
                    ));
                }
                Ok(AuditWorkItemInput {
                    work_item_id,
                    max_iterations: max_iterations as u8,
                })
            })
            .collect(),
        Some(_) => Err(invalid_argument(
            "'audit_work_items' must be an array when provided".to_owned(),
        )),
    }
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

fn tool_definitions(max_summary_bytes: u16, max_artifact_bytes: u32) -> Value {
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
            "description": "Create a repository-scoped executor, butler or auditor run before spawning a child agent. Auditor runs accept one or more work items; remediation executors link to exactly one completed audit.",
            "inputSchema": {
                "type": "object",
                "additionalProperties": false,
                "properties": {
                    "instance_id": { "type": "string", "format": "uuid" },
                    "role": { "type": "string", "enum": ["executor", "butler", "auditor"] },
                    "task_summary": {
                        "type": "string",
                        "maxLength": max_summary_bytes,
                        "description": "Compact summary. The server enforces max_summary_bytes as UTF-8 bytes; put full context in artifacts."
                    },
                    "context_refs": { "type": "array", "items": { "type": "object" } },
                    "audit_work_items": {
                        "type": "array",
                        "minItems": 1,
                        "uniqueItems": true,
                        "items": {
                            "type": "object",
                            "additionalProperties": false,
                            "properties": {
                                "work_item_id": { "type": "string", "format": "uuid" },
                                "max_iterations": { "type": "integer", "minimum": 1, "maximum": 10, "default": 1 }
                            },
                            "required": ["work_item_id"]
                        }
                    },
                    "addresses_audit_run_id": {
                        "type": "string",
                        "format": "uuid",
                        "description": "Only for one executor run that remediates the complete report of a completed auditor run."
                    }
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
            "name": "publish_message",
            "description": "Publish an immutable message envelope and optional artifact, then atomically fan it out to repository mailboxes.",
            "inputSchema": publish_message_schema(max_summary_bytes, max_artifact_bytes),
            "annotations": {
                "readOnlyHint": false,
                "destructiveHint": false,
                "idempotentHint": false,
                "openWorldHint": false
            }
        },
        {
            "name": "wait_for_events",
            "description": "Wait for append-only events addressed to the actor's repository. The cursor is independent of inbox acknowledgement. An omitted timeout waits without a server-side deadline, and the active wait renews the actor lease.",
            "inputSchema": wait_for_events_schema(),
            "annotations": {
                "readOnlyHint": false,
                "destructiveHint": false,
                "idempotentHint": true,
                "openWorldHint": false
            }
        },
        {
            "name": "ask_repository",
            "description": "Create a durable question for one repository. The target receives a request_available event and may atomically claim it.",
            "inputSchema": ask_repository_schema(max_summary_bytes, max_artifact_bytes),
            "annotations": mutation_annotations(false)
        },
        {
            "name": "read_request",
            "description": "Read request state from its requester or target repository without renewing actor or claim leases.",
            "inputSchema": actor_entity_schema("request_id"),
            "annotations": mutation_annotations(true)
        },
        {
            "name": "claim_request",
            "description": "Atomically claim a pending request, or take over a claim whose lease expired.",
            "inputSchema": actor_entity_schema("request_id"),
            "annotations": mutation_annotations(false)
        },
        {
            "name": "renew_request_claim",
            "description": "Extend an unexpired request claim owned by this exact actor.",
            "inputSchema": actor_entity_schema("request_id"),
            "annotations": mutation_annotations(false)
        },
        {
            "name": "release_request_claim",
            "description": "Release an owned request claim back to pending and notify the target repository.",
            "inputSchema": actor_entity_schema("request_id"),
            "annotations": mutation_annotations(false)
        },
        {
            "name": "answer_request",
            "description": "Atomically close an owned request and deliver an answer message with an optional artifact to the requester.",
            "inputSchema": answer_request_schema(max_summary_bytes, max_artifact_bytes),
            "annotations": mutation_annotations(false)
        },
        {
            "name": "fail_request",
            "description": "Fail an owned request. Retryable failures return it to pending; terminal failures notify the requester.",
            "inputSchema": fail_request_schema(max_summary_bytes),
            "annotations": mutation_annotations(false)
        },
        {
            "name": "list_inbox",
            "description": "List pending message headers for the actor's repository. Full artifact content is never included.",
            "inputSchema": inbox_schema(),
            "annotations": {
                "readOnlyHint": true,
                "destructiveHint": false,
                "idempotentHint": true,
                "openWorldHint": false
            }
        },
        {
            "name": "read_message",
            "description": "Read one accessible immutable message envelope. Read its artifact separately when needed.",
            "inputSchema": actor_entity_schema("message_id"),
            "annotations": {
                "readOnlyHint": true,
                "destructiveHint": false,
                "idempotentHint": true,
                "openWorldHint": false
            }
        },
        {
            "name": "read_artifact",
            "description": "Read full artifact content only when the actor's repository produced it or received its message.",
            "inputSchema": actor_entity_schema("artifact_id"),
            "annotations": {
                "readOnlyHint": true,
                "destructiveHint": false,
                "idempotentHint": true,
                "openWorldHint": false
            }
        },
        {
            "name": "ack_message",
            "description": "Idempotently acknowledge one message in the actor repository's mailbox after required content has been processed.",
            "inputSchema": actor_entity_schema("message_id"),
            "annotations": {
                "readOnlyHint": false,
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

fn actor_properties() -> JsonMap<String, Value> {
    let mut properties = JsonMap::new();
    properties.insert(
        "instance_id".to_owned(),
        json!({ "type": "string", "format": "uuid" }),
    );
    properties.insert(
        "worker_run_id".to_owned(),
        json!({ "type": "string", "format": "uuid" }),
    );
    properties
}

fn actor_entity_schema(entity_field: &str) -> Value {
    let mut properties = actor_properties();
    properties.insert(
        entity_field.to_owned(),
        json!({ "type": "string", "format": "uuid" }),
    );
    json!({
        "type": "object",
        "additionalProperties": false,
        "properties": properties,
        "required": [entity_field],
        "oneOf": [
            { "required": ["instance_id"], "not": { "required": ["worker_run_id"] } },
            { "required": ["worker_run_id"], "not": { "required": ["instance_id"] } }
        ]
    })
}

fn inbox_schema() -> Value {
    let mut properties = actor_properties();
    properties.insert(
        "after_delivery_id".to_owned(),
        json!({ "type": "integer", "minimum": 0 }),
    );
    properties.insert(
        "limit".to_owned(),
        json!({ "type": "integer", "minimum": 1, "maximum": 100, "default": 20 }),
    );
    json!({
        "type": "object",
        "additionalProperties": false,
        "properties": properties,
        "oneOf": [
            { "required": ["instance_id"], "not": { "required": ["worker_run_id"] } },
            { "required": ["worker_run_id"], "not": { "required": ["instance_id"] } }
        ]
    })
}

fn wait_for_events_schema() -> Value {
    let mut properties = actor_properties();
    properties.insert(
        "cursor".to_owned(),
        json!({ "type": "integer", "minimum": 0, "default": 0 }),
    );
    properties.insert(
        "kinds".to_owned(),
        json!({
            "type": "array",
            "uniqueItems": true,
            "items": {
                "type": "string",
                "enum": [
                    "message_available",
                    "worker_run_created",
                    "worker_run_active",
                    "worker_run_completed",
                    "worker_run_failed",
                    "request_available",
                    "request_claimed",
                    "request_answered",
                    "request_failed"
                ]
            }
        }),
    );
    properties.insert(
        "timeout_seconds".to_owned(),
        json!({
            "type": "integer",
            "minimum": 0,
            "maximum": 65_535,
            "description": "Omit for no server-side timeout; zero performs a non-blocking event check."
        }),
    );
    properties.insert(
        "limit".to_owned(),
        json!({ "type": "integer", "minimum": 1, "maximum": 50, "default": 20 }),
    );
    json!({
        "type": "object",
        "additionalProperties": false,
        "properties": properties,
        "oneOf": [
            { "required": ["instance_id"], "not": { "required": ["worker_run_id"] } },
            { "required": ["worker_run_id"], "not": { "required": ["instance_id"] } }
        ]
    })
}

fn ask_repository_schema(max_summary_bytes: u16, max_artifact_bytes: u32) -> Value {
    let mut properties = actor_properties();
    properties.insert(
        "target_repository_id".to_owned(),
        json!({ "type": "string", "format": "uuid" }),
    );
    properties.insert(
        "question_summary".to_owned(),
        json!({ "type": "string", "maxLength": max_summary_bytes }),
    );
    properties.insert(
        "body".to_owned(),
        json!({ "type": "string", "maxLength": max_artifact_bytes }),
    );
    properties.insert(
        "context_refs".to_owned(),
        json!({ "type": "array", "items": { "type": "object" } }),
    );
    json!({
        "type": "object",
        "additionalProperties": false,
        "properties": properties,
        "required": ["target_repository_id", "question_summary"],
        "oneOf": [
            { "required": ["instance_id"], "not": { "required": ["worker_run_id"] } },
            { "required": ["worker_run_id"], "not": { "required": ["instance_id"] } }
        ]
    })
}

fn answer_request_schema(max_summary_bytes: u16, max_artifact_bytes: u32) -> Value {
    let mut properties = actor_properties();
    properties.insert(
        "request_id".to_owned(),
        json!({ "type": "string", "format": "uuid" }),
    );
    properties.insert(
        "answer_summary".to_owned(),
        json!({ "type": "string", "maxLength": max_summary_bytes }),
    );
    properties.insert(
        "body".to_owned(),
        json!({ "type": "string", "maxLength": max_artifact_bytes }),
    );
    properties.insert(
        "content_type".to_owned(),
        json!({ "type": "string", "default": "text/markdown" }),
    );
    json!({
        "type": "object",
        "additionalProperties": false,
        "properties": properties,
        "required": ["request_id", "answer_summary"],
        "oneOf": [
            { "required": ["instance_id"], "not": { "required": ["worker_run_id"] } },
            { "required": ["worker_run_id"], "not": { "required": ["instance_id"] } }
        ]
    })
}

fn fail_request_schema(max_summary_bytes: u16) -> Value {
    let mut properties = actor_properties();
    properties.insert(
        "request_id".to_owned(),
        json!({ "type": "string", "format": "uuid" }),
    );
    properties.insert("retryable".to_owned(), json!({ "type": "boolean" }));
    properties.insert(
        "summary".to_owned(),
        json!({ "type": "string", "maxLength": max_summary_bytes }),
    );
    json!({
        "type": "object",
        "additionalProperties": false,
        "properties": properties,
        "required": ["request_id", "retryable", "summary"],
        "oneOf": [
            { "required": ["instance_id"], "not": { "required": ["worker_run_id"] } },
            { "required": ["worker_run_id"], "not": { "required": ["instance_id"] } }
        ]
    })
}

fn mutation_annotations(read_only: bool) -> Value {
    json!({
        "readOnlyHint": read_only,
        "destructiveHint": false,
        "idempotentHint": read_only,
        "openWorldHint": false
    })
}

fn publish_message_schema(max_summary_bytes: u16, max_artifact_bytes: u32) -> Value {
    let mut properties = actor_properties();
    properties.extend(JsonMap::from_iter([
        (
            "kind".to_owned(),
            json!({ "type": "string", "enum": ["report", "announcement", "command"] }),
        ),
        ("broadcast".to_owned(), json!({ "type": "boolean" })),
        (
            "target_repository_ids".to_owned(),
            json!({
                "type": "array",
                "minItems": 1,
                "uniqueItems": true,
                "items": { "type": "string", "format": "uuid" }
            }),
        ),
        (
            "summary".to_owned(),
            json!({ "type": "string", "maxLength": max_summary_bytes }),
        ),
        (
            "body".to_owned(),
            json!({
                "type": "string",
                "maxLength": max_artifact_bytes,
                "description": "Optional full content stored as a separate artifact. Server limit is UTF-8 bytes."
            }),
        ),
        (
            "artifact_kind".to_owned(),
            json!({
                "type": "string",
                "enum": ["implementation_report", "api_contract", "answer", "audit_report", "note"],
                "default": "note"
            }),
        ),
        ("artifact_title".to_owned(), json!({ "type": "string" })),
        (
            "content_type".to_owned(),
            json!({ "type": "string", "default": "text/markdown" }),
        ),
        (
            "thread_id".to_owned(),
            json!({ "type": "string", "format": "uuid" }),
        ),
        (
            "correlation_id".to_owned(),
            json!({ "type": "string", "maxLength": 128 }),
        ),
        (
            "in_reply_to".to_owned(),
            json!({ "type": "string", "format": "uuid" }),
        ),
    ]));
    json!({
        "type": "object",
        "additionalProperties": false,
        "properties": properties,
        "required": ["kind", "summary"],
        "allOf": [
            {
                "oneOf": [
                    { "required": ["instance_id"], "not": { "required": ["worker_run_id"] } },
                    { "required": ["worker_run_id"], "not": { "required": ["instance_id"] } }
                ]
            },
            {
                "oneOf": [
                    { "required": ["broadcast"], "properties": { "broadcast": { "const": true } }, "not": { "required": ["target_repository_ids"] } },
                    { "required": ["target_repository_ids"], "not": { "required": ["broadcast"] } }
                ]
            }
        ]
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

    #[test]
    fn event_wait_schema_has_explicit_unbounded_timeout_contract() {
        let schema = wait_for_events_schema();
        assert_eq!(schema["properties"]["limit"]["maximum"], 50);
        assert_eq!(schema["properties"]["timeout_seconds"]["maximum"], 65_535);
        assert!(
            schema["properties"]["timeout_seconds"]
                .get("default")
                .is_none()
        );

        let tools = tool_definitions(1024, 524_288);
        assert!(tools.as_array().unwrap().iter().any(|tool| {
            tool["name"] == "wait_for_events"
                && tool["annotations"]["readOnlyHint"] == Value::Bool(false)
        }));
    }

    #[test]
    fn event_wait_arguments_enforce_public_limits() {
        let mut arguments = JsonMap::new();
        arguments.insert("timeout_seconds".to_owned(), json!(65_535));
        arguments.insert("limit".to_owned(), json!(50));
        arguments.insert(
            "kinds".to_owned(),
            json!(["message_available", "worker_run_completed"]),
        );

        assert_eq!(optional_timeout_seconds(&arguments).unwrap(), Some(65_535));
        assert_eq!(optional_event_limit(&arguments).unwrap(), Some(50));
        assert_eq!(optional_event_kinds(&arguments).unwrap().len(), 2);

        arguments.insert("limit".to_owned(), json!(51));
        assert!(optional_event_limit(&arguments).is_err());
    }

    #[test]
    fn request_tools_expose_actor_scoping_and_payload_limits() {
        let ask = ask_repository_schema(2048, 65_536);
        assert_eq!(ask["properties"]["question_summary"]["maxLength"], 2048);
        assert_eq!(ask["properties"]["body"]["maxLength"], 65_536);

        let tools = tool_definitions(2048, 65_536);
        let names = tools
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|tool| tool["name"].as_str())
            .collect::<HashSet<_>>();
        for expected in [
            "ask_repository",
            "read_request",
            "claim_request",
            "renew_request_claim",
            "release_request_claim",
            "answer_request",
            "fail_request",
        ] {
            assert!(names.contains(expected));
        }
    }

    #[test]
    fn context_refs_runtime_validation_matches_mcp_schema() {
        let mut arguments = JsonMap::new();
        assert_eq!(validated_context_refs(&arguments).unwrap(), json!([]));

        arguments.insert(
            "context_refs".to_owned(),
            json!([
                { "kind": "message", "id": "one" },
                { "kind": "artifact", "id": "two", "metadata": { "optional": true } }
            ]),
        );
        assert_eq!(
            validated_context_refs(&arguments)
                .unwrap()
                .as_array()
                .unwrap()
                .len(),
            2
        );

        for invalid in [json!(null), json!({}), json!([null, 42, "invalid"])] {
            arguments.insert("context_refs".to_owned(), invalid);
            assert!(validated_context_refs(&arguments).is_err());
        }
    }
}
