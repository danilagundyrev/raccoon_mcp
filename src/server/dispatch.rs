use super::*;

pub(super) async fn dispatch(
    store: &SqliteStore,
    max_summary_bytes: u16,
    max_artifact_bytes: u32,
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
            if !params.get("clientInfo").is_some_and(Value::is_object) {
                return Err(invalid_argument(
                    "'clientInfo' is required and must be an object".to_owned(),
                ));
            }
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
            "instructions": "This is a project/repository coordination hub. Start with connect_orchestrator using a user-defined project_key and repository_key, then read hub://guide for the complete machine-readable workflow. Model shared work with create_feature/create_work_item, use expected_revision for updates, and claim an owner-repository item before creating its executor worker run. The current linked executor must publish an implementation_report and complete before its orchestrator completes the item. Roles are executor, butler and auditor. Auditor scope contains completed work_item_ids and publishes exactly one audit_report; a later iteration waits for the previous audit and any created remediation executor to complete. Workers never register as repositories. Use ask_repository for durable questions and actor-scoped idempotency_key values for retryable creation calls. Use wait_for_events for repository-scoped waiting; an active wait renews actor, request-claim and work-item-claim leases. Omit timeout_seconds for no server-side timeout. JSON-RPC batches are not supported.",
            }))
        }
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
            let idempotency_key = optional_idempotency_key(&arguments)?;
            store
                .create_worker_run_idempotent(
                    CreateWorkerRun {
                        parent_instance_id,
                        role,
                        task_summary,
                        context_refs,
                        audit_work_items: optional_audit_work_items(&arguments)?,
                        addresses_audit_run_id: optional_string(
                            &arguments,
                            "addresses_audit_run_id",
                        )?
                        .map(|value| WorkerRunId::parse(&value).map_err(invalid_uuid))
                        .transpose()?,
                        work_item_id: optional_string(&arguments, "work_item_id")?
                            .map(|value| WorkItemId::parse(&value).map_err(invalid_uuid))
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
            store
                .list_features(project_id)
                .await
                .map(|features| json!({ "project_id": project_id, "features": features }))
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
                        dependency_ids: optional_work_item_ids(&arguments, "dependency_ids")?
                            .unwrap_or_default(),
                        priority: optional_i16(&arguments, "priority")?.unwrap_or(0),
                        max_audit_iterations: optional_bounded_u8(
                            &arguments,
                            "max_audit_iterations",
                            1,
                            10,
                        )?
                        .unwrap_or(1),
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
            store
                .list_work_items(feature_id)
                .await
                .map(|items| json!({ "feature_id": feature_id, "work_items": items }))
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
                        thread_id,
                        correlation_id,
                        in_reply_to,
                    },
                    idempotency_key.as_deref(),
                )
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
            let idempotency_key = optional_idempotency_key(&arguments)?;
            store
                .ask_repository_idempotent(
                    AskRepository {
                        actor,
                        target_repository_id,
                        question_summary,
                        body,
                        context_refs,
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
            return Err(
                RpcError::new(-32602, "Invalid params").with_data(machine_error_data(
                    "unknown_tool",
                    format!("Unknown tool: {tool_name}"),
                )),
            );
        }
    };

    Ok(match result {
        Ok(payload) => tool_result(payload, false),
        Err(error) => tool_result(store_error_payload(&error), true),
    })
}

pub(super) async fn read_resource(
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
                "protocol_version": PROTOCOL_VERSION,
                "workflow": "multi-repository coordination hub",
                "recommended_entrypoint": "connect_orchestrator",
                "identity": ["project", "repository", "orchestrator instance"],
                "lease_note": "Call heartbeat before lease_expires_at. Repository identity and future mailbox state remain stable across orchestrator instances.",
                "max_summary_bytes": max_summary_bytes,
                "max_artifact_bytes": max_artifact_bytes,
                "summary_note": "Summary fields are compact UTF-8 indexes. Put full context in an artifact instead of enlarging every inbox/event payload.",
                "protocol_behavior": {
                    "json_rpc_batching": "unsupported",
                    "notifications": "accepted without a response body",
                    "errors": "JSON-RPC error.data and tool structuredContent contain stable machine-readable codes; active claim conflicts include retry_at and retry_after_seconds",
                    "sessions": "initialize returns Mcp-Session-Id; every subsequent HTTP request must include that header",
                    "protocol_header": "every request after initialize must include MCP-Protocol-Version matching the negotiated session version",
                    "cancellation": "notifications/cancelled stops an in-flight request only inside the same MCP session and the cancelled request receives no response",
                    "session_termination": "DELETE /mcp with session and protocol headers cancels in-flight requests and terminates the session"
                },
                "worker_flow": [
                    "The orchestrator calls create_worker_run before spawning a child.",
                    "The child receives worker_run_id and calls activate_worker_run.",
                    "The child calls complete_worker_run or fail_worker_run before exiting.",
                    "A worker never calls connect_orchestrator and never creates a repository identity."
                ],
                "work_flow": [
                    "Create a project feature and repository-owned work items; dependencies remain planned until all prerequisites complete.",
                    "Claim a ready item with expected_revision, then create an executor worker run linked by work_item_id.",
                    "The latest executor linked to the item publishes an implementation_report and completes its worker run.",
                    "Its assigned orchestrator then completes the item through update_work_item while the claim is still active.",
                    "Every mutation increments revision and emits project-wide compact work_item_updated events; stale writes return the current card."
                ],
                "audit_flow": [
                    "Create one auditor run for one or more work_item_ids; max_iterations defaults to one and is fixed per item.",
                    "The auditor publishes one audit_report artifact covering its complete scope.",
                    "If remediation is needed, create at most one executor run with addresses_audit_run_id; that executor addresses the whole report.",
                    "A later audit waits for the previous auditor and any created remediation executor to complete, increments each item's iteration independently, and is rejected after depth is exhausted."
                ],
                "request_flow": [
                    "Create a durable directed question with ask_repository; the target receives request_available.",
                    "One target actor atomically claims it; explicit renew, heartbeat, mutating actor activity and active event waiting extend only an unexpired claim.",
                    "answer_request atomically closes the request and delivers an answer message/artifact to the requester.",
                    "A retryable failure returns the request to pending; a terminal failure closes it and notifies the requester."
                ],
                "messaging_flow": [
                    "Publish an immutable summary and optional artifact with publish_message.",
                    "Supply an actor-scoped idempotency_key to creation calls that may be retried; the same key requires an identical payload.",
                    "Call wait_for_events with the last event cursor; omit timeout_seconds for an unbounded server wait. The server renews the waiting actor lease.",
                    "Use list_inbox to read pending headers without artifact bodies.",
                    "Use read_message/read_artifact only for selected items.",
                    "Call ack_message after processing; broadcast recipients acknowledge independently."
                ],
                "operations": {
                    "health": "GET /healthz checks the HTTP process without depending on SQLite.",
                    "readiness": "GET /readyz verifies that a SQLite transaction can begin and roll back.",
                    "metrics": "GET /metrics returns compact counters without message or artifact bodies.",
                    "maintenance": "A frequent indexed lease sweep disconnects expired orchestrators, abandons expired workers and requeues expired request claims. A separate slower retention schedule deletes at most 1000 old events and 1000 idempotency records per pass.",
                    "shutdown": "Graceful shutdown stops accepting calls, cancels session waiters and expires local actor and claim leases."
                },
                "current_scope": "Project/repository presence, feature/work graph with optimistic revisions and claims, linked worker/audit runs, immutable messaging/artifacts, reliable delivery, event waiting and claimed repository requests.",
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

    Err(
        RpcError::new(-32602, "Invalid params").with_data(machine_error_data(
            "unknown_resource",
            format!("Unsupported resource URI: {uri}"),
        )),
    )
}
