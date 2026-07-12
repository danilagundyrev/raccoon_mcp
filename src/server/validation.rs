use super::*;

pub(super) fn parse_instance_id(
    arguments: &JsonMap<String, Value>,
) -> Result<InstanceId, RpcError> {
    InstanceId::parse(&require_string(arguments, "instance_id")?).map_err(invalid_uuid)
}

pub(super) fn parse_project_id(arguments: &JsonMap<String, Value>) -> Result<ProjectId, RpcError> {
    ProjectId::parse(&require_string(arguments, "project_id")?).map_err(invalid_uuid)
}

pub(super) fn parse_worker_run_id(
    arguments: &JsonMap<String, Value>,
) -> Result<WorkerRunId, RpcError> {
    WorkerRunId::parse(&require_string(arguments, "worker_run_id")?).map_err(invalid_uuid)
}

pub(super) fn parse_message_id(arguments: &JsonMap<String, Value>) -> Result<MessageId, RpcError> {
    MessageId::parse(&require_string(arguments, "message_id")?).map_err(invalid_uuid)
}

pub(super) fn parse_request_id(arguments: &JsonMap<String, Value>) -> Result<RequestId, RpcError> {
    RequestId::parse(&require_string(arguments, "request_id")?).map_err(invalid_uuid)
}

pub(super) fn parse_actor(arguments: &JsonMap<String, Value>) -> Result<ActorRef, RpcError> {
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

pub(super) fn optional_bool(
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

pub(super) fn validated_optional_body(
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

pub(super) fn validated_context_refs(
    arguments: &JsonMap<String, Value>,
) -> Result<Value, RpcError> {
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

pub(super) fn optional_idempotency_key(
    arguments: &JsonMap<String, Value>,
) -> Result<Option<String>, RpcError> {
    let value = optional_string(arguments, "idempotency_key")?;
    if value
        .as_ref()
        .is_some_and(|value| value.is_empty() || value.len() > 128)
    {
        return Err(invalid_argument(
            "'idempotency_key' must contain between 1 and 128 UTF-8 bytes".to_owned(),
        ));
    }
    Ok(value)
}

pub(super) fn optional_nonnegative_i64(
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

pub(super) fn optional_limit(arguments: &JsonMap<String, Value>) -> Result<Option<u8>, RpcError> {
    match optional_nonnegative_i64(arguments, "limit")? {
        None => Ok(None),
        Some(value @ 1..=100) => Ok(Some(value as u8)),
        Some(_) => Err(invalid_argument(
            "'limit' must be between 1 and 100".to_owned(),
        )),
    }
}

pub(super) fn optional_event_limit(
    arguments: &JsonMap<String, Value>,
) -> Result<Option<u8>, RpcError> {
    match optional_nonnegative_i64(arguments, "limit")? {
        None => Ok(None),
        Some(value @ 1..=50) => Ok(Some(value as u8)),
        Some(_) => Err(invalid_argument(
            "'limit' must be between 1 and 50 for wait_for_events".to_owned(),
        )),
    }
}

pub(super) fn optional_timeout_seconds(
    arguments: &JsonMap<String, Value>,
) -> Result<Option<u64>, RpcError> {
    match optional_nonnegative_i64(arguments, "timeout_seconds")? {
        None => Ok(None),
        Some(value @ 0..=65_535) => Ok(Some(value as u64)),
        Some(_) => Err(invalid_argument(
            "'timeout_seconds' must be between 0 and 65535".to_owned(),
        )),
    }
}

pub(super) fn optional_event_kinds(
    arguments: &JsonMap<String, Value>,
) -> Result<Vec<EventKind>, RpcError> {
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

pub(super) fn optional_repository_ids(
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

pub(super) fn optional_audit_work_items(
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

pub(super) fn validated_summary(
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

pub(super) fn canonical_repository_path(raw_path: &str) -> Result<String, RpcError> {
    std::fs::canonicalize(Path::new(raw_path))
        .map(|path| path.to_string_lossy().into_owned())
        .map_err(|error| {
            invalid_argument(format!(
                "repository_path '{raw_path}' cannot be canonicalized: {error}"
            ))
        })
}

pub(super) fn optional_arguments(
    params: &JsonMap<String, Value>,
) -> Result<JsonMap<String, Value>, RpcError> {
    match params.get("arguments") {
        None | Some(Value::Null) => Ok(JsonMap::new()),
        Some(Value::Object(arguments)) => Ok(arguments.clone()),
        Some(_) => Err(invalid_argument(
            "'arguments' must be an object when provided".to_owned(),
        )),
    }
}

pub(super) fn require_string(
    params: &JsonMap<String, Value>,
    field_name: &str,
) -> Result<String, RpcError> {
    match params.get(field_name).and_then(Value::as_str) {
        Some(value) if !value.trim().is_empty() => Ok(value.to_owned()),
        _ => Err(invalid_argument(format!(
            "'{field_name}' must be a non-empty string"
        ))),
    }
}

pub(super) fn optional_string(
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

pub(super) fn invalid_argument(message: String) -> RpcError {
    RpcError::new(-32602, "Invalid params").with_data(json!(message))
}

pub(super) fn invalid_uuid(error: uuid::Error) -> RpcError {
    invalid_argument(format!("invalid UUID: {error}"))
}

pub(super) fn store_rpc_error(error: StoreError) -> RpcError {
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

pub(super) fn store_error_payload(error: &StoreError) -> Value {
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

pub(super) fn tool_result(payload: Value, is_error: bool) -> Value {
    json!({
        "content": [{
            "type": "text",
            "text": serde_json::to_string_pretty(&payload).unwrap_or_else(|_| "{}".to_owned()),
        }],
        "structuredContent": payload,
        "isError": is_error,
    })
}
