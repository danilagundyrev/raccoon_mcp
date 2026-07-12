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
