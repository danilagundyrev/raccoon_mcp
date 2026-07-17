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
    let templates = list_resource_templates();
    let uris = templates
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|template| template["uriTemplate"].as_str())
        .collect::<HashSet<_>>();
    assert!(uris.contains("hub://feature/{feature_id}"));
    assert!(uris.contains("hub://work-item/{work_item_id}"));
    assert!(
        !uris
            .iter()
            .any(|uri| uri.contains("message") || uri.contains("artifact"))
    );
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
fn actor_scoped_schemas_keep_named_properties_without_top_level_composition() {
    for schema in [
        actor_entity_schema("request_id"),
        inbox_schema(),
        wait_for_events_schema(60),
        ask_repository_schema(1024, 4096),
        answer_request_schema(1024, 4096),
        fail_request_schema(1024),
        publish_message_schema(1024, 4096),
    ] {
        assert_eq!(schema["properties"]["instance_id"]["format"], "uuid");
        assert_eq!(schema["properties"]["worker_run_id"]["format"], "uuid");
        assert!(schema.get("oneOf").is_none());
        assert!(schema.get("allOf").is_none());
    }
}

#[test]
fn actor_runtime_still_requires_exactly_one_identity() {
    let mut arguments = JsonMap::new();
    assert!(parse_actor(&arguments).is_err());
    arguments.insert("instance_id".to_owned(), json!(InstanceId::new()));
    assert!(matches!(
        parse_actor(&arguments),
        Ok(ActorRef::Orchestrator(_))
    ));
    arguments.insert("worker_run_id".to_owned(), json!(WorkerRunId::new()));
    assert!(parse_actor(&arguments).is_err());
}

#[test]
fn closed_tool_schemas_reject_unknown_members_recursively() {
    let tools = tool_definitions(1024, 4096, 60);
    let answer_schema = tools
        .as_array()
        .unwrap()
        .iter()
        .find(|tool| tool["name"] == "answer_request")
        .unwrap()
        .get("inputSchema")
        .unwrap();
    let error = validate_tool_argument_members(
        "answer_request",
        json!({
            "instance_id": InstanceId::new(),
            "request_id": RequestId::new(),
            "answer_summary": "Contract attached",
            "artifact": { "kind": "answer", "content": "unexpected" }
        })
        .as_object()
        .unwrap(),
        answer_schema,
    )
    .unwrap_err();
    assert_eq!(error.data.as_ref().unwrap()["code"], "unknown_argument");
    assert_eq!(
        error.data.as_ref().unwrap()["argument_path"],
        "arguments.artifact"
    );
    assert_eq!(error.data.as_ref().unwrap()["tool_name"], "answer_request");

    let worker_schema = tools
        .as_array()
        .unwrap()
        .iter()
        .find(|tool| tool["name"] == "create_worker_run")
        .unwrap()
        .get("inputSchema")
        .unwrap();
    let removed_worker_scope = validate_tool_argument_members(
        "create_worker_run",
        json!({
            "instance_id": InstanceId::new(),
            "role": "auditor",
            "task_summary": "Audit",
            "audit_work_items": [{ "work_item_id": WorkItemId::new() }]
        })
        .as_object()
        .unwrap(),
        worker_schema,
    )
    .unwrap_err();
    assert_eq!(
        removed_worker_scope.data.as_ref().unwrap()["argument_path"],
        "arguments.audit_work_items"
    );

    let connect_schema = tools
        .as_array()
        .unwrap()
        .iter()
        .find(|tool| tool["name"] == "connect_orchestrator")
        .unwrap()
        .get("inputSchema")
        .unwrap();
    assert!(
        validate_tool_argument_members(
            "connect_orchestrator",
            json!({
                "project_key": "project",
                "repository_key": "backend",
                "repository_path": "C:/project/backend",
                "capabilities": { "custom": { "nested": true } }
            })
            .as_object()
            .unwrap(),
            connect_schema,
        )
        .is_ok()
    );
}

#[test]
fn tool_results_do_not_duplicate_structured_payload_in_text() {
    let payload = json!({
        "artifact_id": ArtifactId::new(),
        "body": "unique-large-payload-marker"
    });
    let result = tool_result(payload.clone(), false);
    assert_eq!(result["structuredContent"], payload);
    let text = result["content"][0]["text"].as_str().unwrap();
    assert!(!text.contains("unique-large-payload-marker"));
    assert!(text.len() < 160);
}

#[test]
fn event_wait_schema_has_explicit_finite_timeout_contract() {
    let schema = wait_for_events_schema(60);
    assert_eq!(schema["properties"]["limit"]["maximum"], 50);
    assert_eq!(schema["properties"]["timeout_seconds"]["maximum"], 60);
    assert_eq!(schema["properties"]["consumer_key"]["maxLength"], 64);
    assert_eq!(schema["properties"]["feature_id"]["format"], "uuid");
    assert_eq!(
        schema["properties"]["owner_repository_id"]["format"],
        "uuid"
    );
    assert!(schema["properties"]["cursor"].get("default").is_none());
    assert!(
        schema["properties"]["timeout_seconds"]
            .get("default")
            .is_none()
    );
    assert_eq!(dispatch::effective_wait_seconds(None, 60), 60);
    assert_eq!(dispatch::effective_wait_seconds(Some(600), 60), 60);
    assert_eq!(dispatch::effective_wait_seconds(Some(0), 60), 0);

    let tools = tool_definitions(1024, 524_288, 60);
    for expected in ["get_event_checkpoint", "commit_event_cursor"] {
        assert!(
            tools
                .as_array()
                .unwrap()
                .iter()
                .any(|tool| tool["name"] == expected)
        );
    }
    assert_eq!(
        event_checkpoint_schema(true)["required"],
        json!(["consumer_key", "cursor"])
    );
    assert!(tools.as_array().unwrap().iter().any(|tool| {
        tool["name"] == "wait_for_events"
            && tool["annotations"]["readOnlyHint"] == Value::Bool(false)
            && tool["annotations"]["idempotentHint"] == Value::Bool(false)
    }));
}

#[tokio::test]
async fn event_wait_reaches_the_configured_deadline_and_reports_clamping() {
    let path = std::env::temp_dir()
        .join("raccoon-mcp-server-tests")
        .join(format!("{}.sqlite3", uuid::Uuid::new_v4()));
    let store = SqliteStore::open(&path, 1, 90, 3600, 600, 3600)
        .await
        .unwrap();
    let connected = store
        .connect_orchestrator(ConnectOrchestrator {
            project_key: "bounded-wait".to_owned(),
            project_display_name: None,
            repository_key: "backend".to_owned(),
            repository_display_name: None,
            canonical_path: "C:/bounded-wait/backend".to_owned(),
            instance_id: None,
            capabilities: json!({}),
        })
        .await
        .unwrap();
    let started_at = tokio::time::Instant::now();
    let result = dispatch(
        &store,
        1024,
        4096,
        1,
        "tools/call",
        json!({
            "name": "wait_for_events",
            "arguments": {
                "instance_id": connected.instance.instance_id,
                "cursor": 0,
                "timeout_seconds": 65_535
            }
        })
        .as_object()
        .unwrap(),
    )
    .await
    .unwrap();
    let payload = &result["structuredContent"];
    assert_eq!(payload["outcome"], "deadline");
    assert_eq!(payload["requested_timeout_seconds"], 65_535);
    assert_eq!(payload["effective_timeout_seconds"], 1);
    assert!(payload["elapsed_ms"].as_u64().unwrap() >= 900);
    assert!(started_at.elapsed() < Duration::from_millis(1_500));

    drop(store);
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(path.with_extension("sqlite3-shm"));
    let _ = std::fs::remove_file(path.with_extension("sqlite3-wal"));
}

#[test]
fn tool_annotations_match_lease_mutation_semantics() {
    let tools = tool_definitions(1024, 524_288, 60);
    for name in [
        "heartbeat",
        "activate_worker_run",
        "heartbeat_worker_run",
        "wait_for_events",
    ] {
        let tool = tools
            .as_array()
            .unwrap()
            .iter()
            .find(|tool| tool["name"] == name)
            .unwrap();
        assert_eq!(tool["annotations"]["readOnlyHint"], false, "{name}");
        assert_eq!(tool["annotations"]["idempotentHint"], false, "{name}");
    }
    for name in [
        "get_worker_run",
        "read_request",
        "list_inbox",
        "read_message",
    ] {
        let tool = tools
            .as_array()
            .unwrap()
            .iter()
            .find(|tool| tool["name"] == name)
            .unwrap();
        assert_eq!(tool["annotations"]["readOnlyHint"], true, "{name}");
        assert_eq!(tool["annotations"]["idempotentHint"], true, "{name}");
    }
}

#[test]
fn rpc_errors_expose_machine_readable_data_codes() {
    let invalid = invalid_argument("missing actor identity".to_owned());
    assert_eq!(invalid.data.as_ref().unwrap()["code"], "invalid_params");
    assert_eq!(
        invalid.data.as_ref().unwrap()["message"],
        "missing actor identity"
    );

    let missing = store_rpc_error(StoreError::NotFound("work item x".to_owned()));
    assert_eq!(missing.data.as_ref().unwrap()["code"], "not_found");

    let conflict = store_rpc_error(StoreError::Conflict("claim expired".to_owned()));
    assert_eq!(conflict.data.as_ref().unwrap()["code"], "conflict");

    let claim_conflict = StoreError::ClaimConflict {
        resource_type: "request",
        resource_id: "request-1".to_owned(),
        message: "already claimed".to_owned(),
        retry_at: Some("2026-07-13T10:00:00Z".to_owned()),
        retry_after_seconds: Some(30),
    };
    let tool_claim = store_error_payload(&claim_conflict);
    assert_eq!(tool_claim["error"]["code"], "claim_conflict");
    assert_eq!(tool_claim["error"]["retry_after_seconds"], 30);
    let rpc_claim = store_rpc_error(claim_conflict);
    assert_eq!(rpc_claim.data.as_ref().unwrap()["code"], "claim_conflict");
    assert_eq!(rpc_claim.data.as_ref().unwrap()["retry_after_seconds"], 30);

    let feature = crate::domain::Feature {
        feature_id: FeatureId::new(),
        project_id: ProjectId::new(),
        created_by_instance_id: InstanceId::new(),
        coordinator_repository_id: RepositoryId::new(),
        title: "Feature".to_owned(),
        summary: "Current state".to_owned(),
        status: crate::domain::FeatureStatus::Completed,
        revision: 2,
        created_at: "2026-07-16T00:00:00Z".to_owned(),
        updated_at: "2026-07-16T00:01:00Z".to_owned(),
        completed_at: Some("2026-07-16T00:01:00Z".to_owned()),
        cancelled_at: None,
    };
    let feature_conflict = StoreError::FeatureRevisionConflict {
        feature_id: feature.feature_id,
        expected: 1,
        actual: 2,
        current: Box::new(feature),
    };
    let feature_payload = store_error_payload(&feature_conflict);
    assert_eq!(feature_payload["error"]["code"], "revision_conflict");
    assert_eq!(feature_payload["error"]["resource_type"], "feature");
    assert_eq!(feature_payload["error"]["current"]["revision"], 2);
}

#[tokio::test]
async fn unknown_answer_argument_is_rejected_before_request_mutation() {
    let path = std::env::temp_dir()
        .join("raccoon-mcp-server-tests")
        .join(format!("{}.sqlite3", uuid::Uuid::new_v4()));
    let store = SqliteStore::open(&path, 1, 90, 3600, 600, 3600)
        .await
        .unwrap();
    let requester = store
        .connect_orchestrator(ConnectOrchestrator {
            project_key: "unknown-argument-test".to_owned(),
            project_display_name: None,
            repository_key: "web".to_owned(),
            repository_display_name: None,
            canonical_path: "C:/unknown-argument-test/web".to_owned(),
            instance_id: None,
            capabilities: json!({}),
        })
        .await
        .unwrap();
    let target = store
        .connect_orchestrator(ConnectOrchestrator {
            project_key: "unknown-argument-test".to_owned(),
            project_display_name: None,
            repository_key: "backend".to_owned(),
            repository_display_name: None,
            canonical_path: "C:/unknown-argument-test/backend".to_owned(),
            instance_id: None,
            capabilities: json!({}),
        })
        .await
        .unwrap();
    let request = store
        .ask_repository(AskRepository {
            actor: ActorRef::Orchestrator(requester.instance.instance_id),
            target_repository_id: target.repository.repository_id,
            feature_id: None,
            work_item_id: None,
            question_summary: "Return the contract".to_owned(),
            body: None,
            context_refs: json!([]),
            handler_requirement: HandlerRequirement::OrchestratorAllowed,
            answer_requirement: AnswerRequirement::SummaryAllowed,
        })
        .await
        .unwrap();
    store
        .claim_request(
            ActorRef::Orchestrator(target.instance.instance_id),
            request.request_id,
        )
        .await
        .unwrap();

    let params = json!({
        "name": "answer_request",
        "arguments": {
            "instance_id": target.instance.instance_id,
            "request_id": request.request_id,
            "answer_summary": "Contract attached",
            "artifact": { "kind": "answer", "content": "unexpected" }
        }
    });
    let error = dispatch(
        &store,
        1024,
        4096,
        60,
        "tools/call",
        params.as_object().unwrap(),
    )
    .await
    .unwrap_err();
    assert_eq!(error.data.as_ref().unwrap()["code"], "unknown_argument");

    let unchanged = store
        .read_request(
            ActorRef::Orchestrator(target.instance.instance_id),
            request.request_id,
        )
        .await
        .unwrap();
    assert_eq!(unchanged.status, crate::domain::RequestStatus::Claimed);
    assert!(unchanged.answer_message_id.is_none());
    let inbox = store
        .list_inbox(
            ActorRef::Orchestrator(requester.instance.instance_id),
            None,
            &[],
            20,
        )
        .await
        .unwrap();
    assert!(inbox.items.is_empty());

    drop(store);
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(path.with_extension("sqlite3-shm"));
    let _ = std::fs::remove_file(path.with_extension("sqlite3-wal"));
}

#[tokio::test]
async fn increase_audit_depth_requires_a_non_null_depth_without_panicking() {
    let path = std::env::temp_dir()
        .join("raccoon-mcp-server-tests")
        .join(format!("{}.sqlite3", uuid::Uuid::new_v4()));
    let store = SqliteStore::open(&path, 1, 90, 3600, 600, 3600)
        .await
        .unwrap();
    for (id, depth) in [(1, None), (2, Some(Value::Null))] {
        let mut arguments = json!({
            "instance_id": InstanceId::new(),
            "work_item_id": WorkItemId::new(),
            "expected_revision": 1,
            "reason": "Explicit reopening"
        });
        if let Some(depth) = depth {
            arguments["max_audit_iterations"] = depth;
        }
        let (status, body) = handle_rpc_payload(
            &store,
            1024,
            4096,
            60,
            json!({
                "jsonrpc": "2.0",
                "id": id,
                "method": "tools/call",
                "params": {
                    "name": "increase_audit_depth",
                    "arguments": arguments
                }
            }),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body.unwrap()["error"]["data"]["code"], "invalid_params");
    }

    drop(store);
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(path.with_extension("sqlite3-shm"));
    let _ = std::fs::remove_file(path.with_extension("sqlite3-wal"));
}

#[tokio::test]
async fn protocol_rejects_batches_and_never_responds_to_notifications() {
    let path = std::env::temp_dir()
        .join("raccoon-mcp-server-tests")
        .join(format!("{}.sqlite3", uuid::Uuid::new_v4()));
    let store = SqliteStore::open(&path, 1, 90, 3600, 600, 3600)
        .await
        .unwrap();

    let (batch_status, batch_body) = handle_rpc_payload(
        &store,
        1024,
        524_288,
        60,
        json!([{ "jsonrpc": "2.0", "id": 1, "method": "ping" }]),
        None,
    )
    .await;
    assert_eq!(batch_status, StatusCode::BAD_REQUEST);
    assert_eq!(
        batch_body.unwrap()["error"]["data"]["code"],
        "batch_not_supported"
    );

    let (notification_status, notification_body) = handle_rpc_payload(
        &store,
        1024,
        524_288,
        60,
        json!({ "jsonrpc": "2.0", "method": "ping" }),
        None,
    )
    .await;
    assert_eq!(notification_status, StatusCode::ACCEPTED);
    assert!(notification_body.is_none());

    let (invalid_status, invalid_body) = handle_rpc_payload(
        &store,
        1024,
        524_288,
        60,
        json!({ "jsonrpc": "2.0", "id": 2, "method": "ping", "params": [] }),
        None,
    )
    .await;
    assert_eq!(invalid_status, StatusCode::BAD_REQUEST);
    assert_eq!(
        invalid_body.unwrap()["error"]["data"]["code"],
        "invalid_params"
    );

    let initialize = dispatch(
        &store,
        1024,
        524_288,
        60,
        "initialize",
        &JsonMap::from_iter([
            ("protocolVersion".to_owned(), json!(PROTOCOL_VERSION)),
            ("capabilities".to_owned(), json!({})),
            (
                "clientInfo".to_owned(),
                json!({ "name": "test", "version": "1" }),
            ),
        ]),
    )
    .await
    .unwrap();
    assert!(
        dispatch(
            &store,
            1024,
            524_288,
            60,
            "initialize",
            &JsonMap::from_iter([
                ("protocolVersion".to_owned(), json!(PROTOCOL_VERSION)),
                ("capabilities".to_owned(), json!({})),
                ("clientInfo".to_owned(), json!({})),
            ]),
        )
        .await
        .is_err()
    );
    assert!(
        initialize["instructions"]
            .as_str()
            .unwrap()
            .contains("hub://guide")
    );
    let guide = dispatch(
        &store,
        1024,
        524_288,
        60,
        "resources/read",
        &JsonMap::from_iter([("uri".to_owned(), json!("hub://guide"))]),
    )
    .await
    .unwrap();
    let guide_payload: Value =
        serde_json::from_str(guide["contents"][0]["text"].as_str().unwrap()).unwrap();
    assert_eq!(
        guide_payload["protocol_behavior"]["json_rpc_batching"],
        "unsupported"
    );
    assert_eq!(
        guide_payload["protocol_behavior"]["notifications"],
        "accepted without a response body"
    );
    assert_eq!(guide_payload["collection_limits"]["event_page"], 50);
    assert_eq!(guide_payload["collection_limits"]["event_kind_filters"], 16);
    assert_eq!(
        guide_payload["event_headers"]["filters"],
        json!(["kinds", "feature_id", "owner_repository_id"])
    );
    assert!(
        guide_payload["work_flow"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(Value::as_str)
            .any(|step| step.contains("Higher numeric priority"))
    );
    assert!(
        guide_payload["worker_flow"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(Value::as_str)
            .any(|step| step.contains("fresh executor child context"))
    );
    assert!(
        guide_payload["audit_flow"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(Value::as_str)
            .any(|step| step.contains("no executor or remediation conversation history"))
    );
    assert!(
        guide_payload["request_flow"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(Value::as_str)
            .any(|step| step.contains("butler_required"))
    );

    drop(store);
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(path.with_extension("sqlite3-shm"));
    let _ = std::fs::remove_file(path.with_extension("sqlite3-wal"));
}

#[tokio::test]
async fn sessions_isolate_cancellation_and_cancel_long_waits() {
    let isolated_registry = SessionRegistry::default();
    let first_session = isolated_registry.create(PROTOCOL_VERSION).unwrap();
    let second_session = isolated_registry.create(PROTOCOL_VERSION).unwrap();
    let first_request = isolated_registry
        .begin_request(&first_session, PROTOCOL_VERSION, &json!(7))
        .unwrap();
    assert!(matches!(
        isolated_registry.begin_request(&first_session, PROTOCOL_VERSION, &json!(7)),
        Err(SessionAccessError::DuplicateRequestId)
    ));
    let second_request = isolated_registry
        .begin_request(&second_session, PROTOCOL_VERSION, &json!(7))
        .unwrap();
    isolated_registry
        .cancel(&first_session, PROTOCOL_VERSION, &json!(7))
        .unwrap();
    tokio::time::timeout(
        Duration::from_millis(100),
        first_request.cancellation().notified(),
    )
    .await
    .unwrap();
    assert!(
        tokio::time::timeout(
            Duration::from_millis(10),
            second_request.cancellation().notified(),
        )
        .await
        .is_err()
    );
    isolated_registry
        .terminate(&first_session, PROTOCOL_VERSION)
        .unwrap();
    tokio::time::timeout(
        Duration::from_millis(100),
        first_request.cancellation().notified(),
    )
    .await
    .unwrap();
    assert!(matches!(
        isolated_registry.begin_request(&first_session, PROTOCOL_VERSION, &json!(8)),
        Err(SessionAccessError::NotFound)
    ));

    let bounded_registry = SessionRegistry::new(2, Duration::from_secs(3600));
    let oldest = bounded_registry.create(PROTOCOL_VERSION).unwrap();
    bounded_registry.create(PROTOCOL_VERSION).unwrap();
    assert!(matches!(
        bounded_registry.create(PROTOCOL_VERSION),
        Err(SessionCreateError::CapacityExceeded)
    ));
    assert_eq!(bounded_registry.len(), 2);
    assert!(bounded_registry.contains(&oldest));

    let idle_registry = SessionRegistry::new(2, Duration::from_millis(20));
    let idle = idle_registry.create(PROTOCOL_VERSION).unwrap();
    let active = idle_registry
        .begin_request(&idle, PROTOCOL_VERSION, &json!(9))
        .unwrap();
    tokio::time::sleep(Duration::from_millis(25)).await;
    assert_eq!(idle_registry.reap_idle(), 0);
    drop(active);
    assert_eq!(idle_registry.reap_idle(), 0);
    tokio::time::sleep(Duration::from_millis(25)).await;
    assert_eq!(idle_registry.reap_idle(), 1);
    assert!(!idle_registry.contains(&idle));

    let path = std::env::temp_dir()
        .join("raccoon-mcp-server-tests")
        .join(format!("{}.sqlite3", uuid::Uuid::new_v4()));
    let store = SqliteStore::open(&path, 1, 90, 3600, 600, 3600)
        .await
        .unwrap();
    let connected = store
        .connect_orchestrator(ConnectOrchestrator {
            project_key: "session-test".to_owned(),
            project_display_name: None,
            repository_key: "backend".to_owned(),
            repository_display_name: None,
            canonical_path: "C:/session-test/backend".to_owned(),
            instance_id: None,
            capabilities: json!({}),
        })
        .await
        .unwrap();
    let state = AppState {
        store: store.clone(),
        allowed_hosts: Arc::new(HashSet::from(["localhost".to_owned()])),
        database_path: Arc::new(path.clone()),
        max_summary_bytes: 1024,
        max_artifact_bytes: 524_288,
        max_wait_seconds: 60,
        sessions: SessionRegistry::default(),
        metrics_enabled: true,
    };

    let initialize = mcp_post(
        State(state.clone()),
        HeaderMap::new(),
        Bytes::from(
            json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "initialize",
                "params": {
                    "protocolVersion": PROTOCOL_VERSION,
                    "capabilities": {},
                    "clientInfo": { "name": "test", "version": "1" }
                }
            })
            .to_string(),
        ),
    )
    .await;
    assert_eq!(initialize.status(), StatusCode::OK);
    let session_id = initialize
        .headers()
        .get("mcp-session-id")
        .unwrap()
        .to_str()
        .unwrap()
        .to_owned();
    assert!(session_id.is_ascii());
    assert_eq!(PROTOCOL_VERSION, "2025-11-25");

    let missing_session = mcp_post(
        State(state.clone()),
        HeaderMap::new(),
        Bytes::from(json!({ "jsonrpc": "2.0", "id": 2, "method": "ping" }).to_string()),
    )
    .await;
    assert_eq!(missing_session.status(), StatusCode::BAD_REQUEST);

    let mut unknown_headers = HeaderMap::new();
    unknown_headers.insert(
        "mcp-session-id",
        axum::http::HeaderValue::from_static("00000000-0000-0000-0000-000000000000"),
    );
    unknown_headers.insert(
        "mcp-protocol-version",
        axum::http::HeaderValue::from_static(PROTOCOL_VERSION),
    );
    let unknown_session = mcp_post(
        State(state.clone()),
        unknown_headers,
        Bytes::from(json!({ "jsonrpc": "2.0", "id": 3, "method": "ping" }).to_string()),
    )
    .await;
    assert_eq!(unknown_session.status(), StatusCode::NOT_FOUND);

    let mut session_headers = HeaderMap::new();
    session_headers.insert(
        "mcp-session-id",
        axum::http::HeaderValue::from_str(&session_id).unwrap(),
    );
    session_headers.insert(
        "mcp-protocol-version",
        axum::http::HeaderValue::from_static(PROTOCOL_VERSION),
    );
    let mut missing_version_headers = session_headers.clone();
    missing_version_headers.remove("mcp-protocol-version");
    let missing_version = mcp_post(
        State(state.clone()),
        missing_version_headers,
        Bytes::from(json!({ "jsonrpc": "2.0", "id": 4, "method": "ping" }).to_string()),
    )
    .await;
    assert_eq!(missing_version.status(), StatusCode::BAD_REQUEST);
    let mut wrong_version_headers = session_headers.clone();
    wrong_version_headers.insert(
        "mcp-protocol-version",
        axum::http::HeaderValue::from_static("2025-06-18"),
    );
    let wrong_version = mcp_post(
        State(state.clone()),
        wrong_version_headers,
        Bytes::from(json!({ "jsonrpc": "2.0", "id": 5, "method": "ping" }).to_string()),
    )
    .await;
    assert_eq!(wrong_version.status(), StatusCode::BAD_REQUEST);

    let request_only_notification = mcp_post(
        State(state.clone()),
        session_headers.clone(),
        Bytes::from(
            json!({
                "jsonrpc": "2.0",
                "method": "tools/call",
                "params": {
                    "name": "disconnect_orchestrator",
                    "arguments": { "instance_id": connected.instance.instance_id }
                }
            })
            .to_string(),
        ),
    )
    .await;
    assert_eq!(request_only_notification.status(), StatusCode::ACCEPTED);
    assert_ne!(
        store
            .get_instance(connected.instance.instance_id)
            .await
            .unwrap()
            .status,
        crate::domain::PresenceStatus::Disconnected
    );
    let invalid_id = mcp_post(
        State(state.clone()),
        session_headers.clone(),
        Bytes::from(json!({ "jsonrpc": "2.0", "id": {}, "method": "ping" }).to_string()),
    )
    .await;
    assert_eq!(invalid_id.status(), StatusCode::BAD_REQUEST);
    let wait_state = state.clone();
    let wait_headers = session_headers.clone();
    let instance_id = connected.instance.instance_id;
    let wait = tokio::spawn(async move {
        mcp_post(
            State(wait_state),
            wait_headers,
            Bytes::from(
                json!({
                    "jsonrpc": "2.0",
                    "id": "wait-1",
                    "method": "tools/call",
                    "params": {
                        "name": "wait_for_events",
                        "arguments": { "instance_id": instance_id, "cursor": 0 }
                    }
                })
                .to_string(),
            ),
        )
        .await
    });
    tokio::time::timeout(Duration::from_secs(1), async {
        while !state
            .sessions
            .has_in_flight_request(&session_id, &json!("wait-1"))
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();

    let duplicate_id = mcp_post(
        State(state.clone()),
        session_headers.clone(),
        Bytes::from(json!({ "jsonrpc": "2.0", "id": "wait-1", "method": "ping" }).to_string()),
    )
    .await;
    assert_eq!(duplicate_id.status(), StatusCode::CONFLICT);
    assert!(
        state
            .sessions
            .has_in_flight_request(&session_id, &json!("wait-1"))
    );

    let cancel = mcp_post(
        State(state.clone()),
        session_headers.clone(),
        Bytes::from(
            json!({
                "jsonrpc": "2.0",
                "method": "notifications/cancelled",
                "params": { "requestId": "wait-1", "reason": "test complete" }
            })
            .to_string(),
        ),
    )
    .await;
    assert_eq!(cancel.status(), StatusCode::ACCEPTED);
    let cancelled_wait = wait.await.unwrap();
    assert_eq!(cancelled_wait.status(), StatusCode::ACCEPTED);
    assert_eq!(cancelled_wait.headers()[CONTENT_LENGTH], "0");

    let delete_wait_state = state.clone();
    let delete_wait_headers = session_headers.clone();
    let delete_wait = tokio::spawn(async move {
        mcp_post(
            State(delete_wait_state),
            delete_wait_headers,
            Bytes::from(
                json!({
                    "jsonrpc": "2.0",
                    "id": "wait-delete",
                    "method": "tools/call",
                    "params": {
                        "name": "wait_for_events",
                        "arguments": { "instance_id": instance_id, "cursor": 0 }
                    }
                })
                .to_string(),
            ),
        )
        .await
    });
    tokio::time::timeout(Duration::from_secs(1), async {
        while !state
            .sessions
            .has_in_flight_request(&session_id, &json!("wait-delete"))
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let deleted = mcp_delete(State(state.clone()), session_headers.clone()).await;
    assert_eq!(deleted.status(), StatusCode::NO_CONTENT);
    assert_eq!(delete_wait.await.unwrap().status(), StatusCode::ACCEPTED);
    let after_delete = mcp_post(
        State(state.clone()),
        session_headers,
        Bytes::from(json!({ "jsonrpc": "2.0", "id": 6, "method": "ping" }).to_string()),
    )
    .await;
    assert_eq!(after_delete.status(), StatusCode::NOT_FOUND);

    assert_eq!(healthz().await.status(), StatusCode::OK);
    assert_eq!(readyz(State(state.clone())).await.status(), StatusCode::OK);
    assert_eq!(metrics(State(state.clone())).await.status(), StatusCode::OK);
    let shutdown_session = state.sessions.create(PROTOCOL_VERSION).unwrap();
    let mut shutdown_headers = HeaderMap::new();
    shutdown_headers.insert(
        "mcp-session-id",
        axum::http::HeaderValue::from_str(&shutdown_session).unwrap(),
    );
    shutdown_headers.insert(
        "mcp-protocol-version",
        axum::http::HeaderValue::from_static(PROTOCOL_VERSION),
    );
    let shutdown_wait_state = state.clone();
    let shutdown_wait = tokio::spawn(async move {
        mcp_post(
            State(shutdown_wait_state),
            shutdown_headers,
            Bytes::from(
                json!({
                    "jsonrpc": "2.0",
                    "id": "wait-shutdown",
                    "method": "tools/call",
                    "params": {
                        "name": "wait_for_events",
                        "arguments": { "instance_id": instance_id, "cursor": 0 }
                    }
                })
                .to_string(),
            ),
        )
        .await
    });
    tokio::time::timeout(Duration::from_secs(1), async {
        while !state
            .sessions
            .has_in_flight_request(&shutdown_session, &json!("wait-shutdown"))
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    prepare_shutdown(&store, &state.sessions).await.unwrap();
    assert_eq!(shutdown_wait.await.unwrap().status(), StatusCode::ACCEPTED);
    assert!(!state.sessions.contains(&shutdown_session));
    assert_eq!(
        store
            .operational_metrics()
            .await
            .unwrap()
            .online_orchestrators,
        0
    );

    drop(state);
    drop(store);
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(path.with_extension("sqlite3-shm"));
    let _ = std::fs::remove_file(path.with_extension("sqlite3-wal"));
}

#[test]
fn event_wait_arguments_enforce_public_limits() {
    let mut arguments = JsonMap::new();
    let feature_id = FeatureId::new();
    let owner_repository_id = RepositoryId::new();
    arguments.insert("timeout_seconds".to_owned(), json!(65_535));
    arguments.insert("limit".to_owned(), json!(50));
    arguments.insert(
        "kinds".to_owned(),
        json!(["message_available", "worker_run_completed"]),
    );
    arguments.insert("feature_id".to_owned(), json!(feature_id));
    arguments.insert("owner_repository_id".to_owned(), json!(owner_repository_id));

    assert_eq!(optional_timeout_seconds(&arguments).unwrap(), Some(65_535));
    assert_eq!(optional_event_limit(&arguments).unwrap(), Some(50));
    assert_eq!(optional_event_kinds(&arguments).unwrap().len(), 2);
    assert_eq!(
        optional_feature_id(&arguments, "feature_id").unwrap(),
        Some(feature_id)
    );
    assert_eq!(
        optional_repository_id(&arguments, "owner_repository_id").unwrap(),
        Some(owner_repository_id)
    );

    arguments.insert("limit".to_owned(), json!(51));
    assert!(optional_event_limit(&arguments).is_err());
}

#[test]
fn inbox_kind_filter_is_bounded_and_typed() {
    let schema = inbox_schema();
    assert_eq!(schema["properties"]["kinds"]["maxItems"], 3);
    let mut arguments = JsonMap::new();
    arguments.insert("kinds".to_owned(), json!(["report", "command"]));
    assert_eq!(optional_message_kinds(&arguments).unwrap().len(), 2);
    arguments.insert("kinds".to_owned(), json!(["unknown"]));
    assert!(optional_message_kinds(&arguments).is_err());
}

#[test]
fn request_tools_expose_actor_scoping_and_payload_limits() {
    let ask = ask_repository_schema(2048, 65_536);
    assert_eq!(ask["properties"]["question_summary"]["maxLength"], 2048);
    assert_eq!(ask["properties"]["body"]["maxLength"], 65_536);
    assert_eq!(
        ask["properties"]["handler_requirement"]["enum"],
        json!(["orchestrator_allowed", "butler_required"])
    );
    assert_eq!(
        ask["properties"]["handler_requirement"]["default"],
        "orchestrator_allowed"
    );
    assert_eq!(
        ask["properties"]["answer_requirement"]["enum"],
        json!(["summary_allowed", "artifact_required"])
    );
    assert_eq!(
        ask["properties"]["answer_requirement"]["default"],
        "summary_allowed"
    );

    let tools = tool_definitions(2048, 65_536, 60);
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
    for link in ["feature_id", "work_item_id"] {
        assert_eq!(ask["properties"][link]["format"], "uuid");
    }
    let publish = publish_message_schema(2048, 65_536);
    for link in ["feature_id", "work_item_id", "request_id"] {
        assert_eq!(publish["properties"][link]["format"], "uuid");
    }
    let create_worker = tools
        .as_array()
        .unwrap()
        .iter()
        .find(|tool| tool["name"] == "create_worker_run")
        .unwrap();
    assert_eq!(
        create_worker["inputSchema"]["properties"]["request_id"]["format"],
        "uuid"
    );
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

    arguments.insert(
        "context_refs".to_owned(),
        Value::Array(vec![json!({}); MAX_CONTEXT_REFS + 1]),
    );
    assert!(validated_context_refs(&arguments).is_err());
    assert_eq!(
        ask_repository_schema(1024, 4096)["properties"]["context_refs"]["maxItems"],
        MAX_CONTEXT_REFS
    );
}

#[test]
fn collection_cardinality_limits_match_runtime_and_schemas() {
    let work_item_ids = (0..=MAX_WORK_ITEM_DEPENDENCIES)
        .map(|_| Value::String(WorkItemId::new().to_string()))
        .collect::<Vec<_>>();
    let repository_ids = (0..=MAX_MESSAGE_RECIPIENTS)
        .map(|_| Value::String(RepositoryId::new().to_string()))
        .collect::<Vec<_>>();
    let audit_items = (0..=MAX_AUDIT_WORK_ITEMS)
        .map(|_| Value::String(WorkItemId::new().to_string()))
        .collect::<Vec<_>>();

    let mut arguments = JsonMap::new();
    arguments.insert("dependency_ids".to_owned(), Value::Array(work_item_ids));
    assert!(optional_work_item_ids(&arguments, "dependency_ids").is_err());
    arguments.insert(
        "target_repository_ids".to_owned(),
        Value::Array(repository_ids),
    );
    assert!(optional_repository_ids(&arguments).is_err());
    arguments.insert(
        "audit_scope_work_item_ids".to_owned(),
        Value::Array(audit_items),
    );
    assert!(optional_audit_scope_work_item_ids(&arguments).is_err());

    assert_eq!(
        create_work_item_schema(1024)["properties"]["dependency_ids"]["maxItems"],
        MAX_WORK_ITEM_DEPENDENCIES
    );
    assert_eq!(
        publish_message_schema(1024, 4096)["properties"]["target_repository_ids"]["maxItems"],
        MAX_MESSAGE_RECIPIENTS
    );
    assert_eq!(
        create_work_item_schema(1024)["properties"]["audit_scope_work_item_ids"]["maxItems"],
        MAX_AUDIT_WORK_ITEMS
    );
}

#[test]
fn idempotency_key_runtime_validation_matches_creation_schemas() {
    let mut arguments = JsonMap::new();
    assert_eq!(optional_idempotency_key(&arguments).unwrap(), None);
    arguments.insert("idempotency_key".to_owned(), json!("retry-1"));
    assert_eq!(
        optional_idempotency_key(&arguments).unwrap().as_deref(),
        Some("retry-1")
    );
    for invalid in [json!(""), json!("x".repeat(129)), json!(42)] {
        arguments.insert("idempotency_key".to_owned(), invalid);
        assert!(optional_idempotency_key(&arguments).is_err());
    }

    assert_eq!(
        ask_repository_schema(1024, 4096)["properties"]["idempotency_key"]["maxLength"],
        128
    );
    assert_eq!(
        publish_message_schema(1024, 4096)["properties"]["idempotency_key"]["maxLength"],
        128
    );
}

#[test]
fn work_graph_tools_publish_revision_and_dependency_contracts() {
    let tools = tool_definitions(2048, 65_536, 60);
    let names = tools
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|tool| tool["name"].as_str())
        .collect::<HashSet<_>>();
    for expected in [
        "create_feature",
        "get_feature",
        "list_features",
        "complete_feature",
        "cancel_feature",
        "create_work_item",
        "get_work_item",
        "list_work_items",
        "update_work_item",
        "increase_audit_depth",
        "claim_work_item",
        "renew_work_item_claim",
        "release_work_item_claim",
        "list_worker_runs",
        "list_requests",
        "list_deliveries",
        "list_messages",
        "list_artifacts",
    ] {
        assert!(names.contains(expected));
    }
    let update = update_work_item_schema(2048);
    assert!(
        update["required"]
            .as_array()
            .unwrap()
            .contains(&json!("expected_revision"))
    );
    let update_tool = tools
        .as_array()
        .unwrap()
        .iter()
        .find(|tool| tool["name"] == "update_work_item")
        .unwrap();
    assert!(
        update_tool["description"]
            .as_str()
            .unwrap()
            .contains("audit_report")
    );
    let list_artifacts_tool = tools
        .as_array()
        .unwrap()
        .iter()
        .find(|tool| tool["name"] == "list_artifacts")
        .unwrap();
    assert!(
        list_artifacts_tool["description"]
            .as_str()
            .unwrap()
            .contains("project-shared")
    );
    assert!(
        list_artifacts_tool["description"]
            .as_str()
            .unwrap()
            .contains("scoped_work_item_ids")
    );
    let list_requests_tool = tools
        .as_array()
        .unwrap()
        .iter()
        .find(|tool| tool["name"] == "list_requests")
        .unwrap();
    assert!(
        list_requests_tool["description"]
            .as_str()
            .unwrap()
            .contains("answer_requirement")
    );
    let read_artifact_tool = tools
        .as_array()
        .unwrap()
        .iter()
        .find(|tool| tool["name"] == "read_artifact")
        .unwrap();
    assert!(
        read_artifact_tool["description"]
            .as_str()
            .unwrap()
            .contains("workers retain narrower")
    );
    assert!(
        update["properties"]["implementation_artifact_id"]["description"]
            .as_str()
            .unwrap()
            .contains("audit completion")
    );
    let create = create_work_item_schema(2048);
    assert_eq!(create["properties"]["dependency_ids"]["uniqueItems"], true);
    assert_eq!(create["properties"]["max_audit_iterations"]["maximum"], 10);
    assert_eq!(create["properties"]["max_audit_iterations"]["default"], 2);
    let increase = increase_audit_depth_schema(2048);
    assert_eq!(
        increase["properties"]["max_audit_iterations"]["maximum"],
        10
    );
    assert!(
        increase["required"]
            .as_array()
            .unwrap()
            .contains(&json!("reason"))
    );
    let publish = publish_message_schema(2048, 65_536);
    assert_eq!(
        publish["properties"]["audit_outcomes"]["items"]["properties"]["verdict"]["enum"],
        json!(["passed", "changes_required", "blocked"])
    );
    assert_eq!(
        create["properties"]["kind"]["enum"],
        json!(["implementation", "audit", "remediation"])
    );
    assert!(
        create["properties"]["priority"]["description"]
            .as_str()
            .unwrap()
            .contains("Higher numeric values")
    );
    let list = list_work_items_schema();
    assert_eq!(list["properties"]["limit"]["maximum"], 100);
    assert_eq!(list["properties"]["after_work_item_id"]["format"], "uuid");
    for schema in [
        list_features_schema(),
        list_worker_runs_schema(),
        list_requests_schema(),
        list_deliveries_schema(),
        list_messages_schema(),
        list_artifacts_schema(),
    ] {
        assert_eq!(schema["properties"]["limit"]["default"], 20);
        assert_eq!(schema["properties"]["limit"]["maximum"], 50);
    }
    assert_eq!(
        list_deliveries_schema()["properties"]["status"]["enum"],
        json!(["pending", "acknowledged", "all"])
    );
    assert_eq!(
        list_repositories_schema()["properties"]["presence_filter"]["enum"],
        json!(["all", "online", "offline"])
    );

    let mut audit_arguments = JsonMap::new();
    audit_arguments.insert(
        "audit_scope_work_item_ids".to_owned(),
        json!([WorkItemId::new()]),
    );
    assert_eq!(
        optional_work_item_ids(&audit_arguments, "audit_scope_work_item_ids")
            .unwrap()
            .unwrap()
            .len(),
        1
    );
}

#[test]
fn structured_audit_outcomes_are_typed_and_bounded() {
    let work_item_id = WorkItemId::new();
    let mut arguments = JsonMap::new();
    arguments.insert(
        "audit_outcomes".to_owned(),
        json!([{
            "work_item_id": work_item_id,
            "verdict": "changes_required",
            "summary": "Fix the contract mismatch"
        }]),
    );
    let outcomes = optional_audit_outcomes(&arguments, 1024).unwrap();
    assert_eq!(outcomes.len(), 1);
    assert_eq!(outcomes[0].work_item_id, work_item_id);
    assert_eq!(outcomes[0].verdict, AuditVerdict::ChangesRequired);

    arguments["audit_outcomes"][0]["verdict"] = json!("nitpick");
    assert!(optional_audit_outcomes(&arguments, 1024).is_err());
    arguments["audit_outcomes"] = Value::Array(
        (0..=MAX_AUDIT_WORK_ITEMS)
            .map(|_| {
                json!({
                    "work_item_id": WorkItemId::new(),
                    "verdict": "passed",
                    "summary": "Pass"
                })
            })
            .collect(),
    );
    assert!(optional_audit_outcomes(&arguments, 1024).is_err());
}
