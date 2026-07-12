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
            && tool["annotations"]["idempotentHint"] == Value::Bool(false)
    }));
}

#[test]
fn tool_annotations_match_lease_mutation_semantics() {
    let tools = tool_definitions(1024, 524_288);
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
}

#[tokio::test]
async fn protocol_rejects_batches_and_never_responds_to_notifications() {
    let path = std::env::temp_dir()
        .join("agent-sync-mcp-server-tests")
        .join(format!("{}.sqlite3", uuid::Uuid::new_v4()));
    let store = SqliteStore::open(&path, 1, 90, 3600, 600, 3600)
        .await
        .unwrap();

    let (batch_status, batch_body) = handle_rpc_payload(
        &store,
        1024,
        524_288,
        json!([{ "jsonrpc": "2.0", "id": 1, "method": "ping" }]),
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
        json!({ "jsonrpc": "2.0", "method": "ping" }),
    )
    .await;
    assert_eq!(notification_status, StatusCode::ACCEPTED);
    assert!(notification_body.is_none());

    let initialize = dispatch(&store, 1024, 524_288, "initialize", &JsonMap::new())
        .await
        .unwrap();
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

    drop(store);
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(path.with_extension("sqlite3-shm"));
    let _ = std::fs::remove_file(path.with_extension("sqlite3-wal"));
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
    let tools = tool_definitions(2048, 65_536);
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
        "create_work_item",
        "get_work_item",
        "list_work_items",
        "update_work_item",
        "claim_work_item",
        "renew_work_item_claim",
        "release_work_item_claim",
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
    let create = create_work_item_schema(2048);
    assert_eq!(create["properties"]["dependency_ids"]["uniqueItems"], true);
    assert_eq!(create["properties"]["max_audit_iterations"]["maximum"], 10);

    let mut audit_arguments = JsonMap::new();
    audit_arguments.insert(
        "audit_work_items".to_owned(),
        json!([{ "work_item_id": WorkItemId::new() }]),
    );
    assert_eq!(
        optional_audit_work_items(&audit_arguments).unwrap().len(),
        1
    );
    audit_arguments.insert(
        "audit_work_items".to_owned(),
        json!([{ "work_item_id": WorkItemId::new(), "max_iterations": 3 }]),
    );
    assert!(optional_audit_work_items(&audit_arguments).is_err());
}
