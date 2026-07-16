use super::*;

pub(super) fn parse_instance_id(
    arguments: &JsonMap<String, Value>,
) -> Result<InstanceId, RpcError> {
    InstanceId::parse(&require_string(arguments, "instance_id")?).map_err(invalid_uuid)
}

pub(super) fn parse_project_id(arguments: &JsonMap<String, Value>) -> Result<ProjectId, RpcError> {
    ProjectId::parse(&require_string(arguments, "project_id")?).map_err(invalid_uuid)
}

pub(super) fn optional_presence_filter(
    arguments: &JsonMap<String, Value>,
) -> Result<RepositoryPresenceFilter, RpcError> {
    optional_string(arguments, "presence_filter")?
        .as_deref()
        .unwrap_or("all")
        .parse()
        .map_err(invalid_argument)
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

pub(super) fn parse_feature_id(arguments: &JsonMap<String, Value>) -> Result<FeatureId, RpcError> {
    FeatureId::parse(&require_string(arguments, "feature_id")?).map_err(invalid_uuid)
}

pub(super) fn parse_work_item_id(
    arguments: &JsonMap<String, Value>,
) -> Result<WorkItemId, RpcError> {
    WorkItemId::parse(&require_string(arguments, "work_item_id")?).map_err(invalid_uuid)
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
            ensure_array_limit("context_refs", values.len(), MAX_CONTEXT_REFS)?;
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

pub(super) fn required_revision(arguments: &JsonMap<String, Value>) -> Result<u64, RpcError> {
    match optional_nonnegative_i64(arguments, "expected_revision")? {
        Some(value @ 1..) => Ok(value as u64),
        _ => Err(invalid_argument(
            "'expected_revision' is required and must be a positive integer".to_owned(),
        )),
    }
}

pub(super) fn optional_i16(
    arguments: &JsonMap<String, Value>,
    field_name: &str,
) -> Result<Option<i16>, RpcError> {
    match arguments.get(field_name) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Number(value)) => value
            .as_i64()
            .and_then(|value| i16::try_from(value).ok())
            .map(Some)
            .ok_or_else(|| invalid_argument(format!("'{field_name}' must be a 16-bit integer"))),
        Some(_) => Err(invalid_argument(format!(
            "'{field_name}' must be a 16-bit integer"
        ))),
    }
}

pub(super) fn optional_bounded_u8(
    arguments: &JsonMap<String, Value>,
    field_name: &str,
    minimum: u8,
    maximum: u8,
) -> Result<Option<u8>, RpcError> {
    match optional_nonnegative_i64(arguments, field_name)? {
        None => Ok(None),
        Some(value) if (i64::from(minimum)..=i64::from(maximum)).contains(&value) => {
            Ok(Some(value as u8))
        }
        Some(_) => Err(invalid_argument(format!(
            "'{field_name}' must be between {minimum} and {maximum}"
        ))),
    }
}

pub(super) fn optional_work_item_id(
    arguments: &JsonMap<String, Value>,
    field_name: &str,
) -> Result<Option<WorkItemId>, RpcError> {
    optional_string(arguments, field_name)?
        .map(|value| WorkItemId::parse(&value).map_err(invalid_uuid))
        .transpose()
}

pub(super) fn optional_feature_id(
    arguments: &JsonMap<String, Value>,
    field_name: &str,
) -> Result<Option<FeatureId>, RpcError> {
    optional_string(arguments, field_name)?
        .map(|value| FeatureId::parse(&value).map_err(invalid_uuid))
        .transpose()
}

pub(super) fn optional_request_id(
    arguments: &JsonMap<String, Value>,
    field_name: &str,
) -> Result<Option<RequestId>, RpcError> {
    optional_string(arguments, field_name)?
        .map(|value| RequestId::parse(&value).map_err(invalid_uuid))
        .transpose()
}

pub(super) fn optional_nullable_work_item_id(
    arguments: &JsonMap<String, Value>,
    field_name: &str,
) -> Result<Option<Option<WorkItemId>>, RpcError> {
    match arguments.get(field_name) {
        None => Ok(None),
        Some(Value::Null) => Ok(Some(None)),
        Some(Value::String(value)) => WorkItemId::parse(value)
            .map(|id| Some(Some(id)))
            .map_err(invalid_uuid),
        Some(_) => Err(invalid_argument(format!(
            "'{field_name}' must be a UUID string or null"
        ))),
    }
}

pub(super) fn optional_work_item_ids(
    arguments: &JsonMap<String, Value>,
    field_name: &str,
) -> Result<Option<Vec<WorkItemId>>, RpcError> {
    match arguments.get(field_name) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Array(values)) => {
            ensure_array_limit(field_name, values.len(), MAX_WORK_ITEM_DEPENDENCIES)?;
            let ids = values
                .iter()
                .map(|value| {
                    let value = value.as_str().ok_or_else(|| {
                        invalid_argument(format!("every {field_name} entry must be a UUID string"))
                    })?;
                    WorkItemId::parse(value).map_err(invalid_uuid)
                })
                .collect::<Result<Vec<_>, _>>()?;
            ensure_unique_strings(field_name, ids.iter().map(ToString::to_string))?;
            Ok(Some(ids))
        }
        Some(_) => Err(invalid_argument(format!("'{field_name}' must be an array"))),
    }
}

pub(super) fn optional_validated_summary(
    arguments: &JsonMap<String, Value>,
    field_name: &str,
    max_summary_bytes: u16,
) -> Result<Option<String>, RpcError> {
    match optional_string(arguments, field_name)? {
        Some(value) => {
            let mut single = JsonMap::new();
            single.insert(field_name.to_owned(), Value::String(value));
            validated_summary(&single, field_name, max_summary_bytes).map(Some)
        }
        None => Ok(None),
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
        Some(Value::Array(values)) => {
            ensure_array_limit("kinds", values.len(), 15)?;
            let kinds = values
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
                .collect::<Result<Vec<_>, _>>()?;
            ensure_unique_strings("kinds", kinds.iter().map(|kind| kind.as_str().to_owned()))?;
            Ok(kinds)
        }
        Some(_) => Err(invalid_argument(
            "'kinds' must be an array when provided".to_owned(),
        )),
    }
}

pub(super) fn optional_message_kinds(
    arguments: &JsonMap<String, Value>,
) -> Result<Vec<MessageKind>, RpcError> {
    match arguments.get("kinds") {
        None | Some(Value::Null) => Ok(Vec::new()),
        Some(Value::Array(values)) => {
            ensure_array_limit("kinds", values.len(), 3)?;
            let kinds = values
                .iter()
                .map(|value| {
                    value
                        .as_str()
                        .ok_or_else(|| {
                            invalid_argument("every message kind must be a string".to_owned())
                        })?
                        .parse::<MessageKind>()
                        .map_err(invalid_argument)
                })
                .collect::<Result<Vec<_>, _>>()?;
            ensure_unique_strings("kinds", kinds.iter().map(|kind| kind.as_str().to_owned()))?;
            Ok(kinds)
        }
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
        Some(Value::Array(values)) => {
            ensure_array_limit(
                "target_repository_ids",
                values.len(),
                MAX_MESSAGE_RECIPIENTS,
            )?;
            let ids = values
                .iter()
                .map(|value| {
                    let value = value.as_str().ok_or_else(|| {
                        invalid_argument(
                            "every target_repository_id must be a UUID string".to_owned(),
                        )
                    })?;
                    RepositoryId::parse(value).map_err(invalid_uuid)
                })
                .collect::<Result<Vec<_>, _>>()?;
            ensure_unique_strings("target_repository_ids", ids.iter().map(ToString::to_string))?;
            Ok(Some(ids))
        }
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
        Some(Value::Array(values)) => {
            ensure_array_limit("audit_work_items", values.len(), MAX_AUDIT_WORK_ITEMS)?;
            let items = values.iter().map(|value| {
                let object = value.as_object().ok_or_else(|| {
                    invalid_argument("every audit_work_item must be an object".to_owned())
                })?;
                if object.keys().any(|key| key != "work_item_id") {
                    return Err(invalid_argument(
                        "audit_work_item only accepts work_item_id; audit depth is stored on the work-item card"
                            .to_owned(),
                    ));
                }
                let work_item_id = WorkItemId::parse(&require_string(object, "work_item_id")?)
                    .map_err(invalid_uuid)?;
                Ok(AuditWorkItemInput { work_item_id })
            })
            .collect::<Result<Vec<_>, _>>()?;
            ensure_unique_strings(
                "audit_work_items",
                items.iter().map(|item| item.work_item_id.to_string()),
            )?;
            Ok(items)
        }
        Some(_) => Err(invalid_argument(
            "'audit_work_items' must be an array when provided".to_owned(),
        )),
    }
}

fn ensure_array_limit(field_name: &str, actual: usize, maximum: usize) -> Result<(), RpcError> {
    if actual > maximum {
        return Err(invalid_argument(format!(
            "'{field_name}' must contain at most {maximum} items"
        )));
    }
    Ok(())
}

fn ensure_unique_strings(
    field_name: &str,
    values: impl IntoIterator<Item = String>,
) -> Result<(), RpcError> {
    let mut unique = HashSet::new();
    for value in values {
        if !unique.insert(value) {
            return Err(invalid_argument(format!(
                "'{field_name}' must not contain duplicate items"
            )));
        }
    }
    Ok(())
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

pub(super) fn validate_tool_argument_members(
    tool_name: &str,
    arguments: &JsonMap<String, Value>,
    input_schema: &Value,
) -> Result<(), RpcError> {
    validate_object_members(tool_name, arguments, input_schema, "arguments")
}

fn validate_object_members(
    tool_name: &str,
    object: &JsonMap<String, Value>,
    schema: &Value,
    path: &str,
) -> Result<(), RpcError> {
    let properties = schema.get("properties").and_then(Value::as_object);
    if schema.get("additionalProperties") == Some(&Value::Bool(false)) {
        let mut allowed_arguments = properties
            .into_iter()
            .flat_map(|properties| properties.keys().cloned())
            .collect::<Vec<_>>();
        allowed_arguments.sort_unstable();
        for field_name in object.keys() {
            if !properties.is_some_and(|properties| properties.contains_key(field_name)) {
                let argument_path = format!("{path}.{field_name}");
                return Err(RpcError::new(-32602, "Invalid params").with_data(json!({
                    "code": "unknown_argument",
                    "message": format!(
                        "tool '{tool_name}' does not declare argument '{argument_path}'"
                    ),
                    "tool_name": tool_name,
                    "argument_path": argument_path,
                    "allowed_arguments": allowed_arguments,
                })));
            }
        }
    }

    let Some(properties) = properties else {
        return Ok(());
    };
    for (field_name, value) in object {
        let Some(property_schema) = properties.get(field_name) else {
            continue;
        };
        let field_path = format!("{path}.{field_name}");
        match value {
            Value::Object(nested) => {
                validate_object_members(tool_name, nested, property_schema, &field_path)?;
            }
            Value::Array(items) => {
                let Some(item_schema) = property_schema.get("items") else {
                    continue;
                };
                for (index, item) in items.iter().enumerate() {
                    if let Value::Object(nested) = item {
                        validate_object_members(
                            tool_name,
                            nested,
                            item_schema,
                            &format!("{field_path}[{index}]"),
                        )?;
                    }
                }
            }
            _ => {}
        }
    }
    Ok(())
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
    RpcError::new(-32602, "Invalid params").with_data(machine_error_data("invalid_params", message))
}

pub(super) fn invalid_uuid(error: uuid::Error) -> RpcError {
    invalid_argument(format!("invalid UUID: {error}"))
}

pub(super) fn store_rpc_error(error: StoreError) -> RpcError {
    match error {
        StoreError::NotFound(message) => {
            RpcError::new(-32602, "Not found").with_data(machine_error_data("not_found", message))
        }
        StoreError::Conflict(message) => RpcError::new(-32602, "Invalid state")
            .with_data(machine_error_data("conflict", message)),
        StoreError::RevisionConflict {
            work_item_id,
            expected,
            actual,
            current,
        } => {
            let retry_at = current.claim_expires_at.clone();
            let retry_after_seconds = retry_after_seconds(retry_at.as_deref());
            RpcError::new(-32602, "Revision conflict").with_data(json!({
                "code": "revision_conflict",
                "message": format!(
                    "work item {work_item_id} revision conflict: expected {expected}, actual {actual}"
                ),
                "work_item_id": work_item_id,
                "expected_revision": expected,
                "actual_revision": actual,
                "current": current,
                "retry_at": retry_at,
                "retry_after_seconds": retry_after_seconds,
            }))
        }
        StoreError::FeatureRevisionConflict {
            feature_id,
            expected,
            actual,
            current,
        } => RpcError::new(-32602, "Revision conflict").with_data(json!({
            "code": "revision_conflict",
            "message": format!(
                "feature {feature_id} revision conflict: expected {expected}, actual {actual}"
            ),
            "resource_type": "feature",
            "feature_id": feature_id,
            "expected_revision": expected,
            "actual_revision": actual,
            "current": current,
        })),
        StoreError::ClaimConflict {
            resource_type,
            resource_id,
            message,
            retry_at,
            retry_after_seconds,
        } => RpcError::new(-32602, "Claim conflict").with_data(json!({
            "code": "claim_conflict",
            "message": message,
            "resource_type": resource_type,
            "resource_id": resource_id,
            "retry_at": retry_at,
            "retry_after_seconds": retry_after_seconds,
        })),
        other => RpcError::new(-32603, "Internal error")
            .with_data(machine_error_data("internal_error", other.to_string())),
    }
}

pub(super) fn machine_error_data(code: &str, message: impl Into<String>) -> Value {
    json!({ "code": code, "message": message.into() })
}

pub(super) fn store_error_payload(error: &StoreError) -> Value {
    if let StoreError::FeatureRevisionConflict {
        expected,
        actual,
        current,
        ..
    } = error
    {
        return json!({
            "error": {
                "code": "revision_conflict",
                "message": error.to_string(),
                "resource_type": "feature",
                "expected_revision": expected,
                "actual_revision": actual,
                "current": current,
            }
        });
    }
    if let StoreError::RevisionConflict {
        expected,
        actual,
        current,
        ..
    } = error
    {
        let retry_at = current.claim_expires_at.clone();
        return json!({
            "error": {
                "code": "revision_conflict",
                "message": error.to_string(),
                "expected_revision": expected,
                "actual_revision": actual,
                "current": current,
                "retry_after_seconds": retry_after_seconds(retry_at.as_deref()),
                "retry_at": retry_at,
            }
        });
    }
    let code = match error {
        StoreError::NotFound(_) => "not_found",
        StoreError::Conflict(_) => "conflict",
        StoreError::RevisionConflict { .. } => unreachable!("handled above"),
        StoreError::FeatureRevisionConflict { .. } => unreachable!("handled above"),
        StoreError::ClaimConflict { .. } => "claim_conflict",
        StoreError::Database(_)
        | StoreError::Io(_)
        | StoreError::Json(_)
        | StoreError::InvalidUuid(_) => "internal_error",
    };
    if let StoreError::ClaimConflict {
        resource_type,
        resource_id,
        message,
        retry_at,
        retry_after_seconds,
    } = error
    {
        return json!({
            "error": {
                "code": code,
                "message": message,
                "resource_type": resource_type,
                "resource_id": resource_id,
                "retry_at": retry_at,
                "retry_after_seconds": retry_after_seconds,
            }
        });
    }
    json!({ "error": { "code": code, "message": error.to_string() } })
}

fn retry_after_seconds(retry_at: Option<&str>) -> Option<u64> {
    let retry_at = chrono::DateTime::parse_from_rfc3339(retry_at?).ok()?;
    let milliseconds = retry_at
        .signed_duration_since(chrono::Utc::now())
        .num_milliseconds();
    (milliseconds > 0).then(|| ((milliseconds + 999) / 1_000) as u64)
}

pub(super) fn tool_result(payload: Value, is_error: bool) -> Value {
    let text = if is_error {
        let code = payload
            .pointer("/error/code")
            .and_then(Value::as_str)
            .unwrap_or("tool_error");
        let message = payload
            .pointer("/error/message")
            .and_then(Value::as_str)
            .unwrap_or("Tool call failed");
        format!("Tool error [{code}]: {message}")
    } else {
        "Tool call succeeded; inspect structuredContent for the result.".to_owned()
    };
    json!({
        "content": [{
            "type": "text",
            "text": text,
        }],
        "structuredContent": payload,
        "isError": is_error,
    })
}
