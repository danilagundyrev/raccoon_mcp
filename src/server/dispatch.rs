use super::*;

pub(super) async fn dispatch(
    store: &SqliteStore,
    max_summary_bytes: u16,
    max_artifact_bytes: u32,
    max_wait_seconds: u16,
    method: &str,
    params: &JsonMap<String, Value>,
) -> Result<Value, RpcError> {
    match method {
        "initialize" => {
            let _requested_protocol_version = require_string(params, "protocolVersion")?;
            if !params.get("capabilities").is_some_and(Value::is_object) {
                return Err(invalid_argument(
                    "'capabilities' is required and must be an object".to_owned(),
                ));
            }
            let client_info = params
                .get("clientInfo")
                .and_then(Value::as_object)
                .ok_or_else(|| {
                    invalid_argument("'clientInfo' is required and must be an object".to_owned())
                })?;
            require_string(client_info, "name")?;
            require_string(client_info, "version")?;
            Ok(json!({
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
            "instructions": "This is a project/repository coordination hub. Start with connect_orchestrator using a user-defined project_key and repository_key, retain snapshot_event_cursor, then read hub://guide for the complete machine-readable workflow. Recover durable state through bounded list_features, list_work_items, list_worker_runs, list_requests, list_deliveries, list_messages and metadata-only list_artifacts pages; then wait strictly after the snapshot watermark. Same-project orchestrators may recover accepted WorkItem reports and non-request artifacts explicitly linked to a Feature/WorkItem; unlinked and request artifacts retain their narrower authorization. Events are wake-up hints, not the source of truth. Named repository event consumers use get_event_checkpoint and explicit monotonic commit_event_cursor; returned pages never auto-commit. Model shared work with create_feature/create_work_item and expected_revision, and claim an owner-repository item before creating its matching worker run. Spawn a fresh executor child for each independent implementation WorkItem; same-lineage remediation may reuse that child context but always gets a new worker_run_id. Audit cards use fresh auditor children with no executor/remediation conversation history and carry repository-local scope. Every audit_report includes exact structured audit_outcomes; remediation requires a later fresh passing audit, and Feature completion requires verified coverage for every accepted implementation report. Workers never register as repositories. Use ask_repository for durable questions: orchestrator_allowed permits an answer from coordination state already in hand, while butler_required can be claimed and answered only by a fresh request-scoped butler linked through request_id. Use actor-scoped idempotency_key values for retryable creation calls. Use wait_for_events for finite repository-scoped waiting; an active wait renews actor, request-claim and work-item-claim leases. JSON-RPC batches are not supported.",
            }))
        }
        "notifications/initialized" | "notifications/cancelled" => Ok(Value::Null),
        "ping" => Ok(json!({})),
        "tools/list" => Ok(json!({
            "tools": tool_definitions(max_summary_bytes, max_artifact_bytes, max_wait_seconds)
        })),
        "tools/call" => {
            call_tool(
                store,
                max_summary_bytes,
                max_artifact_bytes,
                max_wait_seconds,
                params,
            )
            .await
        }
        "resources/list" => Ok(json!({ "resources": list_resources() })),
        "resources/templates/list" => Ok(json!({
            "resourceTemplates": list_resource_templates(),
        })),
        "resources/read" => {
            let uri = require_string(params, "uri")?;
            read_resource(
                store,
                max_summary_bytes,
                max_artifact_bytes,
                max_wait_seconds,
                &uri,
            )
            .await
        }
        _ => Err(RpcError::new(-32601, "Method not found")
            .with_status(StatusCode::NOT_FOUND)
            .with_data(machine_error_data(
                "method_not_found",
                format!("Unsupported method: {method}"),
            ))),
    }
}

pub(super) async fn call_tool(
    store: &SqliteStore,
    max_summary_bytes: u16,
    max_artifact_bytes: u32,
    max_wait_seconds: u16,
    params: &JsonMap<String, Value>,
) -> Result<Value, RpcError> {
    let tool_name = require_string(params, "name")?;
    let arguments = optional_arguments(params)?;
    let definitions = tool_definitions(max_summary_bytes, max_artifact_bytes, max_wait_seconds);
    let input_schema = definitions
        .as_array()
        .and_then(|definitions| {
            definitions
                .iter()
                .find(|definition| definition["name"].as_str() == Some(tool_name.as_str()))
        })
        .and_then(|definition| definition.get("inputSchema"))
        .ok_or_else(|| {
            RpcError::new(-32602, "Invalid params").with_data(machine_error_data(
                "unknown_tool",
                format!("Unknown tool: {tool_name}"),
            ))
        })?;
    validate_tool_argument_members(&tool_name, &arguments, input_schema)?;
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
            let idempotency_key = optional_idempotency_key(&arguments)?;
            store
                .create_worker_run_idempotent(
                    CreateWorkerRun {
                        parent_instance_id,
                        role,
                        task_summary,
                        context_refs,
                        addresses_audit_run_id: optional_string(
                            &arguments,
                            "addresses_audit_run_id",
                        )?
                        .map(|value| WorkerRunId::parse(&value).map_err(invalid_uuid))
                        .transpose()?,
                        work_item_id: optional_string(&arguments, "work_item_id")?
                            .map(|value| WorkItemId::parse(&value).map_err(invalid_uuid))
                            .transpose()?,
                        request_id: optional_string(&arguments, "request_id")?
                            .map(|value| RequestId::parse(&value).map_err(invalid_uuid))
                            .transpose()?,
                    },
                    idempotency_key.as_deref(),
                )
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
        "create_feature" => {
            let parent_instance_id = parse_instance_id(&arguments)?;
            let title = validated_summary(&arguments, "title", max_summary_bytes)?;
            let summary = validated_summary(&arguments, "summary", max_summary_bytes)?;
            let idempotency_key = optional_idempotency_key(&arguments)?;
            store
                .create_feature_idempotent(
                    CreateFeature {
                        parent_instance_id,
                        title,
                        summary,
                    },
                    idempotency_key.as_deref(),
                )
                .await
                .map(|value| json!(value))
        }
        "get_feature" => {
            let feature_id = parse_feature_id(&arguments)?;
            store
                .get_feature(feature_id)
                .await
                .map(|value| json!(value))
        }
        "list_features" => {
            let project_id = parse_project_id(&arguments)?;
            let status = optional_string(&arguments, "status")?
                .map(|value| value.parse::<FeatureStatus>().map_err(invalid_argument))
                .transpose()?;
            let after_feature_id = optional_feature_id(&arguments, "after_feature_id")?;
            let limit = optional_history_limit(&arguments)?.unwrap_or(20);
            store
                .list_features(project_id, status, after_feature_id, limit)
                .await
                .map(|features| json!({ "project_id": project_id, "features": features }))
        }
        "complete_feature" | "cancel_feature" => {
            let parent_instance_id = parse_instance_id(&arguments)?;
            let feature_id = parse_feature_id(&arguments)?;
            let expected_revision = required_revision(&arguments)?;
            match tool_name.as_str() {
                "complete_feature" => {
                    store
                        .complete_feature(CompleteFeature {
                            parent_instance_id,
                            feature_id,
                            expected_revision,
                        })
                        .await
                }
                _ => {
                    store
                        .cancel_feature(CancelFeature {
                            parent_instance_id,
                            feature_id,
                            expected_revision,
                        })
                        .await
                }
            }
            .map(|value| json!(value))
        }
        "create_work_item" => {
            let parent_instance_id = parse_instance_id(&arguments)?;
            let feature_id = parse_feature_id(&arguments)?;
            let owner_repository_id =
                RepositoryId::parse(&require_string(&arguments, "owner_repository_id")?)
                    .map_err(invalid_uuid)?;
            let idempotency_key = optional_idempotency_key(&arguments)?;
            store
                .create_work_item_idempotent(
                    CreateWorkItem {
                        parent_instance_id,
                        feature_id,
                        parent_work_item_id: optional_work_item_id(
                            &arguments,
                            "parent_work_item_id",
                        )?,
                        owner_repository_id,
                        title: validated_summary(&arguments, "title", max_summary_bytes)?,
                        summary: validated_summary(&arguments, "summary", max_summary_bytes)?,
                        kind: optional_string(&arguments, "kind")?
                            .map(|value| value.parse::<WorkItemKind>().map_err(invalid_argument))
                            .transpose()?
                            .unwrap_or(WorkItemKind::Implementation),
                        audit_scope_work_item_ids: optional_audit_scope_work_item_ids(&arguments)?
                            .unwrap_or_default(),
                        addresses_audit_run_id: optional_string(
                            &arguments,
                            "addresses_audit_run_id",
                        )?
                        .map(|value| WorkerRunId::parse(&value).map_err(invalid_uuid))
                        .transpose()?,
                        dependency_ids: optional_work_item_ids(&arguments, "dependency_ids")?
                            .unwrap_or_default(),
                        priority: optional_i16(&arguments, "priority")?.unwrap_or(0),
                        max_audit_iterations: optional_bounded_u8(
                            &arguments,
                            "max_audit_iterations",
                            1,
                            10,
                        )?
                        .unwrap_or(2),
                    },
                    idempotency_key.as_deref(),
                )
                .await
                .map(|value| json!(value))
        }
        "get_work_item" => {
            let work_item_id = parse_work_item_id(&arguments)?;
            store
                .get_work_item(work_item_id)
                .await
                .map(|value| json!(value))
        }
        "list_work_items" => {
            let feature_id = parse_feature_id(&arguments)?;
            let after_work_item_id = optional_work_item_id(&arguments, "after_work_item_id")?;
            let limit = optional_limit(&arguments)?.unwrap_or(20);
            store
                .list_work_items(feature_id, after_work_item_id, limit)
                .await
                .map(|page| json!({ "feature_id": feature_id, "work_items": page }))
        }
        "update_work_item" => {
            let parent_instance_id = parse_instance_id(&arguments)?;
            let work_item_id = parse_work_item_id(&arguments)?;
            let expected_revision = required_revision(&arguments)?;
            let status = optional_string(&arguments, "status")?
                .map(|value| value.parse::<WorkItemStatus>().map_err(invalid_argument))
                .transpose()?;
            let owner_repository_id = optional_string(&arguments, "owner_repository_id")?
                .map(|value| RepositoryId::parse(&value).map_err(invalid_uuid))
                .transpose()?;
            let implementation_artifact_id =
                optional_string(&arguments, "implementation_artifact_id")?
                    .map(|value| ArtifactId::parse(&value).map_err(invalid_uuid))
                    .transpose()?;
            store
                .update_work_item(UpdateWorkItem {
                    parent_instance_id,
                    work_item_id,
                    expected_revision,
                    title: optional_validated_summary(&arguments, "title", max_summary_bytes)?,
                    summary: optional_validated_summary(&arguments, "summary", max_summary_bytes)?,
                    owner_repository_id,
                    parent_work_item_id: optional_nullable_work_item_id(
                        &arguments,
                        "parent_work_item_id",
                    )?,
                    dependency_ids: optional_work_item_ids(&arguments, "dependency_ids")?,
                    priority: optional_i16(&arguments, "priority")?,
                    status,
                    implementation_artifact_id,
                })
                .await
                .map(|value| json!(value))
        }
        "increase_audit_depth" => {
            let parent_instance_id = parse_instance_id(&arguments)?;
            let work_item_id = parse_work_item_id(&arguments)?;
            let expected_revision = required_revision(&arguments)?;
            let max_audit_iterations =
                optional_bounded_u8(&arguments, "max_audit_iterations", 2, 10)?.ok_or_else(
                    || {
                        invalid_argument(
                            "'max_audit_iterations' is required and must not be null".to_owned(),
                        )
                    },
                )?;
            let reason = validated_summary(&arguments, "reason", max_summary_bytes)?;
            store
                .increase_audit_depth(IncreaseAuditDepth {
                    parent_instance_id,
                    work_item_id,
                    expected_revision,
                    max_audit_iterations,
                    reason,
                })
                .await
                .map(|value| json!(value))
        }
        "claim_work_item" | "renew_work_item_claim" | "release_work_item_claim" => {
            let instance_id = parse_instance_id(&arguments)?;
            let work_item_id = parse_work_item_id(&arguments)?;
            let expected_revision = required_revision(&arguments)?;
            match tool_name.as_str() {
                "claim_work_item" => {
                    store
                        .claim_work_item(instance_id, work_item_id, expected_revision)
                        .await
                }
                "renew_work_item_claim" => {
                    store
                        .renew_work_item_claim(instance_id, work_item_id, expected_revision)
                        .await
                }
                _ => {
                    store
                        .release_work_item_claim(instance_id, work_item_id, expected_revision)
                        .await
                }
            }
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
            let idempotency_key = optional_idempotency_key(&arguments)?;
            let audit_outcomes = optional_audit_outcomes(&arguments, max_summary_bytes)?;
            store
                .publish_message_idempotent(
                    PublishMessage {
                        actor,
                        kind,
                        target,
                        summary,
                        body,
                        artifact_kind,
                        artifact_title: optional_string(&arguments, "artifact_title")?,
                        content_type: optional_string(&arguments, "content_type")?
                            .unwrap_or_else(|| "text/markdown".to_owned()),
                        feature_id: optional_feature_id(&arguments, "feature_id")?,
                        work_item_id: optional_work_item_id(&arguments, "work_item_id")?,
                        request_id: optional_request_id(&arguments, "request_id")?,
                        thread_id,
                        correlation_id,
                        in_reply_to,
                    },
                    &audit_outcomes,
                    idempotency_key.as_deref(),
                )
                .await
                .map(|value| json!(value))
        }
        "wait_for_events" => {
            let actor = parse_actor(&arguments)?;
            let cursor = optional_nonnegative_i64(&arguments, "cursor")?;
            let consumer_key = optional_consumer_key(&arguments)?;
            let kinds = optional_event_kinds(&arguments)?;
            let requested_timeout_seconds = optional_timeout_seconds(&arguments)?;
            let effective_timeout_seconds =
                effective_wait_seconds(requested_timeout_seconds, max_wait_seconds);
            let limit = optional_event_limit(&arguments)?.unwrap_or(20);
            let started_at = tokio::time::Instant::now();
            store.wait_for_events_from(
                actor,
                cursor,
                consumer_key.as_deref(),
                &kinds,
                Duration::from_secs(effective_timeout_seconds),
                limit,
            )
            .await
            .map(|value| {
                json!({
                    "outcome": value.outcome,
                    "events": value.events,
                    "next_cursor": value.next_cursor,
                    "has_more": value.has_more,
                    "consumer_key": consumer_key,
                    "requested_timeout_seconds": requested_timeout_seconds,
                    "effective_timeout_seconds": effective_timeout_seconds,
                    "elapsed_ms": u64::try_from(started_at.elapsed().as_millis()).unwrap_or(u64::MAX),
                })
            })
        }
        "get_event_checkpoint" => {
            let actor = parse_actor(&arguments)?;
            let consumer_key = required_consumer_key(&arguments)?;
            store
                .get_event_checkpoint(actor, &consumer_key)
                .await
                .map(|value| json!(value))
        }
        "commit_event_cursor" => {
            let actor = parse_actor(&arguments)?;
            let consumer_key = required_consumer_key(&arguments)?;
            let cursor = optional_nonnegative_i64(&arguments, "cursor")?.ok_or_else(|| {
                invalid_argument(
                    "'cursor' is required and must be a non-negative integer".to_owned(),
                )
            })?;
            store
                .commit_event_cursor(actor, &consumer_key, cursor)
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
            let idempotency_key = optional_idempotency_key(&arguments)?;
            store
                .ask_repository_idempotent(
                    AskRepository {
                        actor,
                        target_repository_id,
                        feature_id: optional_feature_id(&arguments, "feature_id")?,
                        work_item_id: optional_work_item_id(&arguments, "work_item_id")?,
                        question_summary,
                        body,
                        context_refs,
                        handler_requirement: optional_string(&arguments, "handler_requirement")?
                            .map(|value| {
                                value
                                    .parse::<HandlerRequirement>()
                                    .map_err(invalid_argument)
                            })
                            .transpose()?
                            .unwrap_or(HandlerRequirement::OrchestratorAllowed),
                        answer_requirement: optional_string(&arguments, "answer_requirement")?
                            .map(|value| {
                                value.parse::<AnswerRequirement>().map_err(invalid_argument)
                            })
                            .transpose()?
                            .unwrap_or(AnswerRequirement::SummaryAllowed),
                    },
                    idempotency_key.as_deref(),
                )
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
            let kinds = optional_message_kinds(&arguments)?;
            let limit = optional_limit(&arguments)?.unwrap_or(20);
            store
                .list_inbox(actor, after_delivery_id, &kinds, limit)
                .await
                .map(|value| json!(value))
        }
        "list_worker_runs" => {
            let actor = parse_actor(&arguments)?;
            let filter = WorkerRunListFilter {
                repository_id: optional_repository_id(&arguments, "repository_id")?,
                feature_id: optional_feature_id(&arguments, "feature_id")?,
                work_item_id: optional_work_item_id(&arguments, "work_item_id")?,
                role: optional_string(&arguments, "role")?
                    .map(|value| value.parse::<WorkerRole>().map_err(invalid_argument))
                    .transpose()?,
                status: optional_string(&arguments, "status")?
                    .map(|value| value.parse::<WorkerRunStatus>().map_err(invalid_argument))
                    .transpose()?,
                addresses_audit_run_id: optional_worker_run_id(
                    &arguments,
                    "addresses_audit_run_id",
                )?,
            };
            let cursor = optional_worker_run_id(&arguments, "after_worker_run_id")?;
            let limit = optional_history_limit(&arguments)?.unwrap_or(20);
            store
                .list_worker_runs(actor, filter, cursor, limit)
                .await
                .map(|value| json!(value))
        }
        "list_requests" => {
            let actor = parse_actor(&arguments)?;
            let filter = RequestListFilter {
                requester_repository_id: optional_repository_id(
                    &arguments,
                    "requester_repository_id",
                )?,
                target_repository_id: optional_repository_id(&arguments, "target_repository_id")?,
                feature_id: optional_feature_id(&arguments, "feature_id")?,
                work_item_id: optional_work_item_id(&arguments, "work_item_id")?,
                status: optional_string(&arguments, "status")?
                    .map(|value| value.parse::<RequestStatus>().map_err(invalid_argument))
                    .transpose()?,
            };
            let cursor = optional_request_id(&arguments, "after_request_id")?;
            let limit = optional_history_limit(&arguments)?.unwrap_or(20);
            store
                .list_requests(actor, filter, cursor, limit)
                .await
                .map(|value| json!(value))
        }
        "list_deliveries" => {
            let actor = parse_actor(&arguments)?;
            let status = match optional_string(&arguments, "status")?.as_deref() {
                None | Some("all") => DeliveryListStatus::All,
                Some("pending") => DeliveryListStatus::Pending,
                Some("acknowledged") => DeliveryListStatus::Acknowledged,
                Some(value) => {
                    return Err(invalid_argument(format!(
                        "unknown delivery status '{value}'"
                    )));
                }
            };
            let cursor = optional_nonnegative_i64(&arguments, "after_delivery_id")?;
            let limit = optional_history_limit(&arguments)?.unwrap_or(20);
            store
                .list_deliveries(actor, status, cursor, limit)
                .await
                .map(|value| json!(value))
        }
        "list_messages" => {
            let actor = parse_actor(&arguments)?;
            let filter = MessageListFilter {
                feature_id: optional_feature_id(&arguments, "feature_id")?,
                work_item_id: optional_work_item_id(&arguments, "work_item_id")?,
                request_id: optional_request_id(&arguments, "request_id")?,
                thread_id: optional_thread_id(&arguments, "thread_id")?,
                kind: optional_string(&arguments, "kind")?
                    .map(|value| value.parse::<MessageKind>().map_err(invalid_argument))
                    .transpose()?,
                producer_repository_id: optional_repository_id(
                    &arguments,
                    "producer_repository_id",
                )?,
                recipient_repository_id: optional_repository_id(
                    &arguments,
                    "recipient_repository_id",
                )?,
            };
            let cursor = optional_message_id(&arguments, "after_message_id")?;
            let limit = optional_history_limit(&arguments)?.unwrap_or(20);
            store
                .list_messages(actor, filter, cursor, limit)
                .await
                .map(|value| json!(value))
        }
        "list_artifacts" => {
            let actor = parse_actor(&arguments)?;
            let filter = ArtifactListFilter {
                kind: optional_string(&arguments, "kind")?
                    .map(|value| value.parse::<ArtifactKind>().map_err(invalid_argument))
                    .transpose()?,
                feature_id: optional_feature_id(&arguments, "feature_id")?,
                work_item_id: optional_work_item_id(&arguments, "work_item_id")?,
                request_id: optional_request_id(&arguments, "request_id")?,
                producer_repository_id: optional_repository_id(
                    &arguments,
                    "producer_repository_id",
                )?,
                producer_worker_run_id: optional_worker_run_id(
                    &arguments,
                    "producer_worker_run_id",
                )?,
            };
            let cursor = optional_artifact_id(&arguments, "after_artifact_id")?;
            let limit = optional_history_limit(&arguments)?.unwrap_or(20);
            store
                .list_artifacts(actor, filter, cursor, limit)
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
            let presence_filter = optional_presence_filter(&arguments)?;
            store.list_repositories(project_id, presence_filter).await.map(
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
            return Err(
                RpcError::new(-32603, "Internal error").with_data(machine_error_data(
                    "tool_contract_mismatch",
                    format!("Tool '{tool_name}' is published without a dispatcher"),
                )),
            );
        }
    };

    Ok(match result {
        Ok(payload) => tool_result(payload, false),
        Err(error) => tool_result(store_error_payload(&error), true),
    })
}

pub(super) fn effective_wait_seconds(requested: Option<u64>, maximum: u16) -> u64 {
    requested
        .unwrap_or(u64::from(maximum))
        .min(u64::from(maximum))
}

pub(super) async fn read_resource(
    store: &SqliteStore,
    max_summary_bytes: u16,
    max_artifact_bytes: u32,
    max_wait_seconds: u16,
    uri: &str,
) -> Result<Value, RpcError> {
    if uri == "hub://guide" {
        return Ok(resource_contents(
            uri,
            json!({
                "server": SERVER_NAME,
                "protocol_version": PROTOCOL_VERSION,
                "workflow": "multi-repository coordination hub",
                "recommended_entrypoint": "connect_orchestrator",
                "identity": ["project", "repository", "orchestrator instance"],
                "lease_note": "Call heartbeat before lease_expires_at. Repository identity and future mailbox state remain stable across orchestrator instances.",
                "max_summary_bytes": max_summary_bytes,
                "max_artifact_bytes": max_artifact_bytes,
                "max_wait_seconds": max_wait_seconds,
                "collection_limits": {
                    "context_refs": MAX_CONTEXT_REFS,
                    "audit_scope_work_items": MAX_AUDIT_WORK_ITEMS,
                    "work_item_dependencies": MAX_WORK_ITEM_DEPENDENCIES,
                    "message_recipients": MAX_MESSAGE_RECIPIENTS,
                    "event_page": 50,
                    "event_kind_filters": 16,
                    "inbox_page": 100,
                    "inbox_kind_filters": 3,
                    "work_item_page": 100
                },
                "summary_note": "Summary fields are compact UTF-8 indexes. Put full context in an artifact instead of enlarging every inbox/event payload.",
                "protocol_behavior": {
                    "json_rpc_batching": "unsupported",
                    "notifications": "accepted without a response body",
                    "errors": "JSON-RPC error.data and tool structuredContent contain stable machine-readable codes; active claim conflicts include retry_at and retry_after_seconds",
                    "sessions": "initialize returns Mcp-Session-Id; every subsequent HTTP request must include that header. Idle sessions are reaped; initialize is rejected when the configured hard limit is occupied by live sessions.",
                    "protocol_header": "every request after initialize must include MCP-Protocol-Version matching the negotiated session version",
                    "cancellation": "notifications/cancelled stops an in-flight request only inside the same MCP session and the cancelled request receives no response",
                    "session_termination": "DELETE /mcp with session and protocol headers cancels in-flight requests and terminates the session"
                },
                "worker_flow": [
                    "The orchestrator calls create_worker_run before spawning a child.",
                    "The child receives worker_run_id and calls activate_worker_run.",
                    "The child calls complete_worker_run or fail_worker_run before exiting.",
                    "Use one fresh executor child context per independent implementation WorkItem. Never move it to an unrelated WorkItem.",
                    "Same-lineage remediation may continue in the original executor child when that context remains healthy, but create a new linked remediation worker_run_id. Use a fresh remediation child if the original is unavailable, saturated, repeatedly confused, or anchored to a fundamentally rejected approach.",
                    "A failed or abandoned work-item worker may be replaced on the same reopened and reclaimed card; any created, active or completed attempt blocks replacement.",
                    "A worker never calls connect_orchestrator and never creates a repository identity."
                ],
                "work_flow": [
                    "Create a project feature and repository-owned implementation, audit and remediation work items; dependencies remain planned until all prerequisites complete.",
                    "Higher numeric priority values are listed and claimed before lower values when other eligibility rules are equal.",
                    "Claim a ready item with expected_revision, then create an executor worker run linked by work_item_id.",
                    "The latest executor linked to the item publishes an implementation_report and completes its worker run.",
                    "Once a linked executor publishes implementation_report, the implementation card cannot be cancelled to bypass verification; complete it with that report and audit it.",
                    "An orchestrator holding the active repository claim completes the item through update_work_item. A completed repository-local worker report remains valid after an expired claim is taken over by another orchestrator instance.",
                    "Every mutation increments revision and emits project-wide compact work_item_updated events; stale writes return the current card.",
                    "The feature coordinator repository calls complete_feature with its current revision only after durable linked work, requests and every linked worker are terminal, including a butler whose request is already answered; cancel_feature closes abandoned work under its stricter safety checks.",
                    "Peer repositories stop waiting on feature_completed or feature_cancelled for the current feature and call disconnect_orchestrator before exit. MCP session DELETE is transport cleanup, not the durable completion signal."
                ],
                "recovery_flow": [
                    "Retain connect_orchestrator.snapshot_event_cursor before reading recovery pages. It is captured in the same transaction as bootstrap counters and repository presence.",
                    "Treat durable list APIs as the source of truth and events only as wake-up hints. Use list_features(status, after_feature_id, limit) and list_work_items to recover the work graph without remembered entity IDs.",
                    "Use actor-scoped list_worker_runs, list_requests, list_deliveries, list_messages and list_artifacts pages to recover execution and communication history. Their default page size is 20 and maximum is 50. Request summaries include both handler_requirement and answer_requirement.",
                    "list_artifacts returns metadata only; load selected bodies with read_artifact after actor-scoped authorization. Same-project orchestrators can recover accepted WorkItem reports and explicitly linked non-request coordination artifacts. Batch audit rows expose audit_work_item_id and the complete deterministic scoped_work_item_ids array.",
                    "get_work_item includes the active or latest worker projection so a replacement orchestrator can resume the correct run lineage.",
                    "Continue each page with its next_cursor while has_more is true, retaining the same filters between pages. Then call wait_for_events with cursor=snapshot_event_cursor and reconcile newer events.",
                    "For durable event progress, choose a repository-local consumer_key. get_event_checkpoint reads it, wait_for_events uses it only when cursor is omitted, and commit_event_cursor advances it monotonically after processing. The server never auto-commits."
                ],
                "audit_flow": [
                    "Create one kind=audit work item in the repository that owns every completed implementation in audit_scope_work_item_ids; split cross-repository batches.",
                    "The owner repository claims the audit card and spawns a fresh auditor child with no executor or remediation conversation history, linked by work_item_id. Pass only specification, acceptance criteria, IDs, artifact references and current repository state; scope and iteration are derived from the card.",
                    "The auditor publishes exactly one audit_report with one audit_outcomes verdict per scoped item and completes; the owner orchestrator then completes the audit card without implementation_artifact_id.",
                    "A changes_required batch permits one kind=remediation work item only when every changes_required item has remaining depth; partially reopened batches are rejected. Completing remediation produces pending_reaudit, never verified.",
                    "A later fresh auditor child must pass each remediated item. Orchestrator review and executor self-review are supplemental, never formal verification. A final non-passing verdict produces verification_blocked; increase_audit_depth requires the current revision, an explicit reason, and user authorization.",
                    "The Feature coordinator calls complete_feature only after every accepted implementation report has verification_status=verified."
                ],
                "request_flow": [
                    "Create a durable directed question with ask_repository and include feature_id/work_item_id when it affects tracked work; a work-item link infers its feature. Set answer_requirement=artifact_required when a non-empty answer body is mandatory. The target receives request_available.",
                    "Use handler_requirement=orchestrator_allowed only when the target can answer from coordination state, existing reports, scope or ownership already in hand. Use butler_required whenever current source inspection, contract verification, multi-file tracing, commands or tests are required.",
                    "For butler_required, create a fresh role=butler worker with this request_id before spawning it. Only that active linked butler can claim and answer the request, and it cannot handle a different request.",
                    "One permitted target actor atomically claims it; explicit renew, heartbeat, mutating actor activity and active event waiting extend only an unexpired claim.",
                    "answer_request atomically closes the request and delivers an answer message/artifact to the requester. The stored answer_provenance and answered_by fields distinguish a direct orchestrator answer from a butler-produced answer.",
                    "While an outgoing request remains unresolved, include request_answered and request_failed in wait_for_events filters, then read_request and selectively load its answer message/artifact.",
                    "A retryable failure returns the request to pending; a terminal failure closes it and notifies the requester."
                ],
                "messaging_flow": [
                    "Publish an immutable summary and optional artifact with publish_message.",
                    "Supply an actor-scoped idempotency_key to creation calls that may be retried; the same key requires an identical payload.",
                    "Call wait_for_events with an explicit last event cursor or a consumer_key whose checkpoint should be used. Explicit cursor wins. Every call is finite and never auto-commits; call commit_event_cursor only after processing.",
                    "Use list_inbox for pending headers; use list_deliveries when acknowledged history is also needed.",
                    "Use list_messages and metadata-only list_artifacts to recover accessible history without loading bodies.",
                    "Use the kinds filter to select report, announcement or command headers.",
                    "Use read_message/read_artifact only for selected items.",
                    "Call ack_message after processing; broadcast recipients acknowledge independently."
                ],
                "artifact_access": [
                    "An artifact accepted as a WorkItem implementation report or published with persisted audit outcomes is project-shared for orchestrators in that project, including superseded audit iterations.",
                    "Attaching feature_id or work_item_id to a non-request message explicitly declares its artifact project-shared for same-project orchestrators.",
                    "An unlinked message artifact remains limited to its producer repository and message recipients.",
                    "Request question and answer artifacts remain limited to requester, target and producer even when the request also links a Feature or WorkItem.",
                    "Worker actors do not inherit project-wide shared-artifact access; they retain repository/run-scoped authorization."
                ],
                "operations": {
                    "health": "GET /healthz checks the HTTP process without depending on SQLite.",
                    "readiness": "GET /readyz performs a harmless SQLite write and rolls the transaction back.",
                    "metrics": "GET /metrics is exposed only with --enable-metrics and computes compact counters on demand without message or artifact bodies.",
                    "maintenance": "A frequent indexed lease/session sweep disconnects expired actors and requeues expired request claims. Retention is disabled by default; when configured, each pass deletes at most 1000 old events and 1000 idempotency records.",
                    "shutdown": "Graceful shutdown stops accepting calls, cancels session waiters and expires local actor and claim leases."
                },
                "current_scope": "Project/repository presence, feature/work graph with optimistic revisions and claims, linked worker/audit runs, bounded recovery/history discovery, immutable messaging/artifacts, reliable delivery, event waiting and claimed repository requests.",
                "resource_scope": "Feature and work-item cards are addressable resources. Actor-sensitive inbox, message and artifact reads remain tools so producer, delivery, request-participant and project-shared-link authorization receive explicit actor identity.",
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
            .list_repositories(project_id, RepositoryPresenceFilter::All)
            .await
            .map_err(store_rpc_error)?;
        return Ok(resource_contents(
            uri,
            json!({ "project_id": project_id, "repositories": repositories }),
        ));
    }

    if let Some(feature_id) = uri.strip_prefix("hub://feature/") {
        let feature_id = FeatureId::parse(feature_id).map_err(invalid_uuid)?;
        let feature = store
            .get_feature(feature_id)
            .await
            .map_err(store_rpc_error)?;
        return Ok(resource_contents(uri, json!(feature)));
    }

    if let Some(work_item_id) = uri.strip_prefix("hub://work-item/") {
        let work_item_id = WorkItemId::parse(work_item_id).map_err(invalid_uuid)?;
        let work_item = store
            .get_work_item(work_item_id)
            .await
            .map_err(store_rpc_error)?;
        return Ok(resource_contents(uri, json!(work_item)));
    }

    Err(
        RpcError::new(-32602, "Invalid params").with_data(machine_error_data(
            "unknown_resource",
            format!("Unsupported resource URI: {uri}"),
        )),
    )
}
