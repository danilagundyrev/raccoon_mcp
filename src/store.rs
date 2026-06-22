use std::{
    collections::{BTreeMap, BTreeSet},
    error::Error,
    fmt, fs,
    path::PathBuf,
    sync::{Arc, Mutex, MutexGuard},
    time::Duration,
};

use chrono::{SecondsFormat, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Map as JsonMap, Value, json};
use tokio::{
    sync::Notify,
    time::{Instant, timeout_at},
};

type StoreResult<T> = Result<T, StoreError>;

#[derive(Debug)]
pub enum StoreError {
    Invalid(String),
    Io(std::io::Error),
    Json(serde_json::Error),
}

impl fmt::Display for StoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StoreError::Invalid(message) => f.write_str(message),
            StoreError::Io(error) => write!(f, "{error}"),
            StoreError::Json(error) => write!(f, "{error}"),
        }
    }
}

impl Error for StoreError {}

impl From<std::io::Error> for StoreError {
    fn from(value: std::io::Error) -> Self {
        StoreError::Io(value)
    }
}

impl From<serde_json::Error> for StoreError {
    fn from(value: serde_json::Error) -> Self {
        StoreError::Json(value)
    }
}

#[derive(Clone)]
pub struct SyncStore {
    inner: Arc<StoreInner>,
}

struct StoreInner {
    state_path: PathBuf,
    state: Mutex<State>,
    notify: Notify,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct State {
    next_message_id: u64,
    messages: Vec<Message>,
    agents: BTreeMap<String, Agent>,
    profiles: BTreeMap<String, AgentProfile>,
    session: JsonMap<String, Value>,
    task: Task,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Agent {
    #[serde(default)]
    agent_name: String,
    #[serde(default)]
    peer_name: Option<String>,
    #[serde(default)]
    repo_path: Option<String>,
    #[serde(default)]
    handoff_path: Option<String>,
    #[serde(default)]
    metadata: JsonMap<String, Value>,
    #[serde(default)]
    registered_at: Option<String>,
    #[serde(default)]
    last_seen_at: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct AgentProfile {
    #[serde(default)]
    agent_name: String,
    #[serde(default)]
    peer_name: Option<String>,
    #[serde(default)]
    repo_path: Option<String>,
    #[serde(default)]
    handoff_path: Option<String>,
    #[serde(default)]
    notes: Option<String>,
    #[serde(default)]
    metadata: JsonMap<String, Value>,
    #[serde(default)]
    created_at: Option<String>,
    #[serde(default)]
    updated_at: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Message {
    #[serde(default)]
    message_id: u64,
    #[serde(default)]
    from_agent: String,
    #[serde(default)]
    to_agent: String,
    #[serde(default)]
    content: String,
    #[serde(default)]
    file_path: Option<String>,
    #[serde(default)]
    summary: Option<String>,
    #[serde(default)]
    metadata: JsonMap<String, Value>,
    #[serde(default)]
    created_at: String,
    #[serde(default)]
    acked_at: Option<String>,
    #[serde(default)]
    acked_by: Option<String>,
    #[serde(default)]
    delivery_count: u64,
    #[serde(default)]
    last_delivered_at: Option<String>,
    #[serde(default)]
    last_delivered_to: Option<String>,
    #[serde(default)]
    consumed_at: Option<String>,
    #[serde(default)]
    consumed_by: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Task {
    #[serde(default = "default_task_status")]
    status: String,
    #[serde(default)]
    completed_at: Option<String>,
    #[serde(default)]
    completed_by: Option<String>,
    #[serde(default)]
    summary: Option<String>,
    #[serde(default)]
    metadata: JsonMap<String, Value>,
}

impl Default for Task {
    fn default() -> Self {
        Self {
            status: default_task_status(),
            completed_at: None,
            completed_by: None,
            summary: None,
            metadata: JsonMap::new(),
        }
    }
}

impl Agent {
    fn new(agent_name: &str) -> Self {
        Self {
            agent_name: agent_name.to_owned(),
            peer_name: None,
            repo_path: None,
            handoff_path: None,
            metadata: JsonMap::new(),
            registered_at: None,
            last_seen_at: None,
        }
    }
}

impl AgentProfile {
    fn new(agent_name: &str) -> Self {
        Self {
            agent_name: agent_name.to_owned(),
            peer_name: None,
            repo_path: None,
            handoff_path: None,
            notes: None,
            metadata: JsonMap::new(),
            created_at: None,
            updated_at: None,
        }
    }
}

impl SyncStore {
    pub fn new(state_path: PathBuf, session_id: Option<String>) -> StoreResult<Self> {
        let state = load_state(&state_path, session_id.as_deref())?;
        Ok(Self {
            inner: Arc::new(StoreInner {
                state_path,
                state: Mutex::new(state),
                notify: Notify::new(),
            }),
        })
    }

    pub fn register_agent(
        &self,
        agent_name: &str,
        peer_name: Option<String>,
        repo_path: Option<String>,
        handoff_path: Option<String>,
        metadata: Option<JsonMap<String, Value>>,
    ) -> StoreResult<Value> {
        let mut state = self.lock_state();
        self.register_agent_locked(
            &mut state,
            agent_name,
            peer_name,
            repo_path,
            handoff_path,
            metadata,
        )
    }

    pub fn upsert_agent_profile(
        &self,
        agent_name: &str,
        peer_name: Option<String>,
        repo_path: Option<String>,
        handoff_path: Option<String>,
        notes: Option<String>,
        metadata: Option<JsonMap<String, Value>>,
    ) -> StoreResult<Value> {
        let mut state = self.lock_state();
        let profile = ensure_profile(&mut state, agent_name);
        let now = utc_now();
        if profile.created_at.is_none() {
            profile.created_at = Some(now.clone());
        }
        profile.updated_at = Some(now);
        if peer_name.is_some() {
            profile.peer_name = peer_name;
        }
        if repo_path.is_some() {
            profile.repo_path = repo_path;
        }
        if handoff_path.is_some() {
            profile.handoff_path = handoff_path;
        }
        if notes.is_some() {
            profile.notes = notes;
        }
        if let Some(metadata) = metadata {
            profile.metadata = metadata;
        }
        let result = to_value(profile);
        self.save_locked(&state)?;
        Ok(result)
    }

    pub fn bootstrap_agent_session(
        &self,
        agent_name: &str,
        register: bool,
        peer_name: Option<String>,
        repo_path: Option<String>,
        handoff_path: Option<String>,
        metadata: Option<JsonMap<String, Value>>,
    ) -> StoreResult<Value> {
        let mut state = self.lock_state();
        self.bootstrap_agent_session_locked(
            &mut state,
            agent_name,
            register,
            peer_name,
            repo_path,
            handoff_path,
            metadata,
        )
    }

    pub fn send_handoff(
        &self,
        from_agent: &str,
        to_agent: &str,
        content: &str,
        file_path: Option<String>,
        summary: Option<String>,
        metadata: Option<JsonMap<String, Value>>,
    ) -> StoreResult<Value> {
        let result = {
            let mut state = self.lock_state();
            if state.task.status == "completed" {
                return Err(StoreError::Invalid(
                    "Task is already marked as completed.".to_owned(),
                ));
            }

            ensure_agent(&mut state, from_agent);
            ensure_agent(&mut state, to_agent);

            let message_id = state.next_message_id;
            state.next_message_id += 1;
            let now = utc_now();
            state.messages.push(Message {
                message_id,
                from_agent: from_agent.to_owned(),
                to_agent: to_agent.to_owned(),
                content: content.to_owned(),
                file_path,
                summary,
                metadata: metadata.unwrap_or_default(),
                created_at: now,
                acked_at: None,
                acked_by: None,
                delivery_count: 0,
                last_delivered_at: None,
                last_delivered_to: None,
                consumed_at: None,
                consumed_by: None,
            });
            ensure_agent(&mut state, from_agent).last_seen_at = Some(utc_now());
            let pending_for_recipient = pending_message_count(&state, to_agent);
            self.save_locked(&state)?;
            json!({
                "message_id": message_id,
                "to_agent": to_agent,
                "pending_for_recipient": pending_for_recipient,
            })
        };

        self.inner.notify.notify_waiters();
        Ok(result)
    }

    pub fn ack_handoff(&self, agent_name: &str, message_id: i64) -> StoreResult<Value> {
        let mut state = self.lock_state();
        ensure_agent(&mut state, agent_name).last_seen_at = Some(utc_now());
        let message = state
            .messages
            .iter_mut()
            .find(|message| message.message_id == message_id as u64)
            .ok_or_else(|| StoreError::Invalid(format!("Unknown message_id: {message_id}")))?;

        if message.to_agent != agent_name {
            return Err(StoreError::Invalid(format!(
                "message_id {message_id} is addressed to {}, not {agent_name}.",
                message.to_agent
            )));
        }

        if is_message_acked(message) {
            if message.acked_by.as_deref() != Some(agent_name) {
                return Err(StoreError::Invalid(format!(
                    "message_id {message_id} was already acknowledged by {}.",
                    message.acked_by.as_deref().unwrap_or("null")
                )));
            }
            return Ok(to_value(message));
        }

        let acked_at = utc_now();
        message.acked_at = Some(acked_at.clone());
        message.acked_by = Some(agent_name.to_owned());
        message.consumed_at = Some(acked_at);
        message.consumed_by = Some(agent_name.to_owned());
        let result = to_value(message);
        self.save_locked(&state)?;
        Ok(result)
    }

    pub fn poll_event(&self, agent_name: &str) -> StoreResult<Option<Value>> {
        let mut state = self.lock_state();
        ensure_agent(&mut state, agent_name).last_seen_at = Some(utc_now());
        let event = peek_event(&mut state, agent_name);
        if event.is_some() {
            self.save_locked(&state)?;
        }
        Ok(event)
    }

    pub async fn wait_for_event(
        &self,
        agent_name: &str,
        timeout_seconds: Option<f64>,
    ) -> StoreResult<Value> {
        let deadline = timeout_seconds
            .map(|seconds| Instant::now() + Duration::from_secs_f64(seconds.max(0.0)));

        {
            let mut state = self.lock_state();
            ensure_agent(&mut state, agent_name).last_seen_at = Some(utc_now());
        }

        loop {
            let notified = self.inner.notify.notified();
            {
                let mut state = self.lock_state();
                if let Some(event) = peek_event(&mut state, agent_name) {
                    self.save_locked(&state)?;
                    return Ok(event);
                }
            }

            if let Some(deadline) = deadline {
                if Instant::now() >= deadline {
                    return Ok(json!({
                        "event_type": "timeout",
                        "agent_name": agent_name,
                        "task_status": self.get_task_status()?["status"].clone(),
                    }));
                }

                if timeout_at(deadline, notified).await.is_err() {
                    return Ok(json!({
                        "event_type": "timeout",
                        "agent_name": agent_name,
                        "task_status": self.get_task_status()?["status"].clone(),
                    }));
                }
            } else {
                notified.await;
            }
        }
    }

    pub fn mark_task_complete(
        &self,
        agent_name: &str,
        summary: Option<String>,
        metadata: Option<JsonMap<String, Value>>,
    ) -> StoreResult<Value> {
        let task = {
            let mut state = self.lock_state();
            if state.task.status != "completed" {
                state.task.status = "completed".to_owned();
                state.task.completed_at = Some(utc_now());
                state.task.completed_by = Some(agent_name.to_owned());
                state.task.summary = summary;
                state.task.metadata = metadata.unwrap_or_default();
                self.save_locked(&state)?;
            }
            to_value(&state.task)
        };

        self.inner.notify.notify_waiters();
        Ok(task)
    }

    pub fn get_task_status(&self) -> StoreResult<Value> {
        let state = self.lock_state();
        Ok(to_value(&state.task))
    }

    pub fn get_snapshot(&self) -> Value {
        let state = self.lock_state();
        json!({
            "session": state.session,
            "task": state.task,
            "agents": state.agents,
            "profiles": state.profiles,
            "messages": state.messages,
            "next_message_id": state.next_message_id,
        })
    }

    pub fn read_resource(&self, uri: &str) -> StoreResult<Value> {
        let mut state = self.lock_state();

        if uri == "sync://guide" {
            let payload = self.guide_payload_locked(&mut state)?;
            return Ok(resource_contents(uri, payload));
        }

        if uri == "sync://profiles" {
            return Ok(resource_contents(
                uri,
                json!({ "profiles": list_agent_profiles_locked(&state) }),
            ));
        }

        if uri == "sync://status" {
            let mut agents = JsonMap::new();
            let agent_names: Vec<String> = state.agents.keys().cloned().collect();
            for agent_name in agent_names {
                agents.insert(agent_name.clone(), agent_status(&mut state, &agent_name));
            }
            return Ok(resource_contents(
                uri,
                json!({
                    "session": state.session,
                    "task": state.task,
                    "agents": agents,
                }),
            ));
        }

        if uri == "sync://history" {
            return Ok(resource_contents(
                uri,
                json!({ "messages": state.messages }),
            ));
        }

        if let Some(agent_name) = uri.strip_prefix("sync://profile/") {
            if agent_name.is_empty() {
                return Err(StoreError::Invalid(format!(
                    "Unsupported resource URI: {uri}"
                )));
            }
            let profile = merged_profile(&state, agent_name);
            let preview = self.bootstrap_agent_session_locked(
                &mut state, agent_name, false, None, None, None, None,
            )?;
            return Ok(resource_contents(
                uri,
                json!({
                    "agent_name": agent_name,
                    "profile_found": profile.is_some(),
                    "profile": profile,
                    "bootstrap_preview": preview,
                }),
            ));
        }

        if let Some(suffix) = uri.strip_prefix("sync://agent/") {
            let Some((agent_name, resource_name)) = suffix.split_once('/') else {
                return Err(StoreError::Invalid(format!(
                    "Unsupported resource URI: {uri}"
                )));
            };
            if agent_name.is_empty() {
                return Err(StoreError::Invalid(format!(
                    "Unsupported resource URI: {uri}"
                )));
            }

            ensure_agent(&mut state, agent_name);
            if resource_name == "mailbox" {
                return Ok(resource_contents(
                    uri,
                    json!({
                        "agent": agent_status(&mut state, agent_name),
                        "pending_messages": pending_messages(&state, agent_name),
                    }),
                ));
            }

            if resource_name == "history" {
                let messages: Vec<Message> = state
                    .messages
                    .iter()
                    .filter(|message| {
                        message.from_agent == agent_name || message.to_agent == agent_name
                    })
                    .cloned()
                    .collect();
                return Ok(resource_contents(
                    uri,
                    json!({
                        "agent": agent_status(&mut state, agent_name),
                        "messages": messages,
                    }),
                ));
            }
        }

        Err(StoreError::Invalid(format!(
            "Unsupported resource URI: {uri}"
        )))
    }

    pub fn list_resources() -> Value {
        json!([
            {
                "uri": "sync://guide",
                "name": "Sync server guide",
                "description": "Machine-readable bootstrap instructions for using this MCP server.",
                "mimeType": "application/json",
            },
            {
                "uri": "sync://profiles",
                "name": "Agent role profiles",
                "description": "Configured role profiles such as backend-agent and client-agent.",
                "mimeType": "application/json",
            },
            {
                "uri": "sync://status",
                "name": "Sync task status",
                "description": "Current task status and agent summaries.",
                "mimeType": "application/json",
            },
            {
                "uri": "sync://history",
                "name": "Handoff history",
                "description": "Full persisted handoff log across both agents.",
                "mimeType": "application/json",
            },
        ])
    }

    pub fn list_resource_templates() -> Value {
        json!([
            {
                "uriTemplate": "sync://profile/{agent_name}",
                "name": "Agent profile",
                "description": "Configured role profile and bootstrap preview for an agent.",
                "mimeType": "application/json",
            },
            {
                "uriTemplate": "sync://agent/{agent_name}/mailbox",
                "name": "Agent mailbox",
                "description": "Pending messages and current status for an agent.",
                "mimeType": "application/json",
            },
            {
                "uriTemplate": "sync://agent/{agent_name}/history",
                "name": "Agent history",
                "description": "All sent and received handoffs for an agent.",
                "mimeType": "application/json",
            },
        ])
    }

    fn bootstrap_agent_session_locked(
        &self,
        state: &mut State,
        agent_name: &str,
        register: bool,
        peer_name: Option<String>,
        repo_path: Option<String>,
        handoff_path: Option<String>,
        metadata: Option<JsonMap<String, Value>>,
    ) -> StoreResult<Value> {
        let stored_profile = merged_profile(state, agent_name);
        let effective_peer_name =
            peer_name.or_else(|| profile_string(&stored_profile, "peer_name"));
        let effective_repo_path =
            repo_path.or_else(|| profile_string(&stored_profile, "repo_path"));
        let effective_handoff_path =
            handoff_path.or_else(|| profile_string(&stored_profile, "handoff_path"));
        let effective_metadata = effective_metadata(&stored_profile, metadata);

        let mut missing_fields = Vec::new();
        if effective_peer_name.is_none() {
            missing_fields.push("peer_name");
        }
        if effective_handoff_path.is_none() {
            missing_fields.push("handoff_path");
        }

        let registration = if register {
            Some(self.register_agent_locked(
                state,
                agent_name,
                effective_peer_name.clone(),
                effective_repo_path.clone(),
                effective_handoff_path.clone(),
                Some(effective_metadata.clone()),
            )?)
        } else {
            None
        };

        Ok(json!({
            "agent_name": agent_name,
            "profile_found": stored_profile.is_some(),
            "profile": stored_profile,
            "effective_config": {
                "agent_name": agent_name,
                "peer_name": effective_peer_name,
                "repo_path": effective_repo_path,
                "handoff_path": effective_handoff_path,
                "metadata": effective_metadata,
            },
            "registration": registration,
            "session": state.session,
            "readiness": if missing_fields.is_empty() { "ready" } else { "needs_configuration" },
            "missing_fields": missing_fields,
            "guide_uri": "sync://guide",
            "profile_uri": format!("sync://profile/{agent_name}"),
            "next_actions": next_actions(agent_name, effective_handoff_path.as_deref()),
            "event_types": event_types(),
        }))
    }

    fn register_agent_locked(
        &self,
        state: &mut State,
        agent_name: &str,
        peer_name: Option<String>,
        repo_path: Option<String>,
        handoff_path: Option<String>,
        metadata: Option<JsonMap<String, Value>>,
    ) -> StoreResult<Value> {
        let agent = ensure_agent(state, agent_name);
        let now = utc_now();
        if agent.registered_at.is_none() {
            agent.registered_at = Some(now.clone());
        }
        agent.last_seen_at = Some(now);
        if peer_name.is_some() {
            agent.peer_name = peer_name;
        }
        if repo_path.is_some() {
            agent.repo_path = repo_path;
        }
        if handoff_path.is_some() {
            agent.handoff_path = handoff_path;
        }
        if let Some(metadata) = metadata {
            agent.metadata = metadata;
        }
        self.save_locked(state)?;
        Ok(agent_status(state, agent_name))
    }

    fn guide_payload_locked(&self, state: &mut State) -> StoreResult<Value> {
        Ok(json!({
            "server": {
                "name": "agent-sync-mcp",
                "workflow": "two-agent handoff loop",
            },
            "session": state.session,
            "recommended_entrypoint": "bootstrap_agent_session",
            "bootstrap_expectation": "If the user tells you your role, call bootstrap_agent_session first. It returns effective config, readiness, and next actions.",
            "built_in_roles": list_agent_profiles_locked(state),
            "tool_summary": {
                "configure_agent_profile": "Optional override for a built-in role profile when you need to change its defaults.",
                "bootstrap_agent_session": "Session bootstrap from a configured role profile, optionally registering the agent.",
                "register_agent": "Direct registration when you already know all fields.",
                "send_handoff": "Send the full current handoff text to the peer agent.",
                "pull_updates": "Fetch the next event for this agent. Returned handoffs remain pending until ack_handoff succeeds.",
                "ack_handoff": "Acknowledge an inbound handoff only after its content has been written to the local handoff file.",
                "mark_task_complete": "Stop the loop cleanly for both agents.",
            },
            "event_types": event_types(),
            "workflow_steps": [
                "Bootstrap the session from your role profile.",
                "Call pull_updates with wait=false before starting work.",
                "If you receive a handoff event, write message.content into your local handoff file.",
                "After the local file is updated, call ack_handoff with message.message_id.",
                "Do one work iteration in the current repo.",
                "Write your local handoff file, then send_handoff with that full content.",
                "Call pull_updates with wait=true and wait for the peer reply or task_completed.",
                "When both sides are done, call mark_task_complete once and stop.",
            ],
            "startup_recovery": [
                "If you are the second-started agent and expected a startup handoff, call pull_updates with wait=false first.",
                "If pull_updates returns none but the peer claims it already sent a message, inspect sync://agent/{agent_name}/mailbox.",
                "If mailbox is empty, inspect sync://agent/{agent_name}/history or sync://history to recover the latest inbound handoff.",
                "Only call ack_handoff after the recovered handoff content has been copied into the local handoff file.",
            ],
            "important_note": "The server stores handoff history in JSON state. It does not edit local markdown files by itself. The agent must write local handoff files explicitly, and handoffs remain pending until ack_handoff succeeds.",
        }))
    }

    fn lock_state(&self) -> MutexGuard<'_, State> {
        self.inner
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn save_locked(&self, state: &State) -> StoreResult<()> {
        save_state(&self.inner.state_path, state)
    }
}

fn load_state(state_path: &PathBuf, configured_session_id: Option<&str>) -> StoreResult<State> {
    if !state_path.exists() {
        return Ok(default_state(state_path, configured_session_id));
    }

    let raw_text = fs::read_to_string(state_path)?;
    let raw: Value = serde_json::from_str(&raw_text)?;
    let mut state = default_state(state_path, configured_session_id);

    if let Some(next_message_id) = raw.get("next_message_id").and_then(Value::as_u64) {
        state.next_message_id = next_message_id;
    }

    if let Some(messages) = raw.get("messages").and_then(Value::as_array) {
        state.messages = messages
            .iter()
            .filter_map(|message| serde_json::from_value::<Message>(message.clone()).ok())
            .map(normalize_message)
            .collect();
    }

    if let Some(agents) = raw.get("agents").and_then(Value::as_object) {
        state.agents = agents
            .iter()
            .filter_map(|(agent_name, agent)| {
                serde_json::from_value::<Agent>(agent.clone())
                    .ok()
                    .map(|mut agent| {
                        if agent.agent_name.is_empty() {
                            agent.agent_name = agent_name.clone();
                        }
                        (agent_name.clone(), agent)
                    })
            })
            .collect();
    }

    if let Some(profiles) = raw.get("profiles").and_then(Value::as_object) {
        state.profiles = profiles
            .iter()
            .filter_map(|(agent_name, profile)| {
                serde_json::from_value::<AgentProfile>(profile.clone())
                    .ok()
                    .map(|mut profile| {
                        if profile.agent_name.is_empty() {
                            profile.agent_name = agent_name.clone();
                        }
                        (agent_name.clone(), profile)
                    })
            })
            .collect();
    }

    if let Some(task) = raw.get("task") {
        if let Ok(task) = serde_json::from_value::<Task>(task.clone()) {
            state.task = task;
        }
    }

    state.session = normalize_session(
        state_path,
        configured_session_id,
        raw.get("session").and_then(Value::as_object).cloned(),
    );
    Ok(state)
}

fn default_state(state_path: &PathBuf, configured_session_id: Option<&str>) -> State {
    State {
        next_message_id: 1,
        messages: Vec::new(),
        agents: BTreeMap::new(),
        profiles: BTreeMap::new(),
        session: normalize_session(state_path, configured_session_id, None),
        task: Task::default(),
    }
}

fn save_state(state_path: &PathBuf, state: &State) -> StoreResult<()> {
    if let Some(parent) = state_path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)?;
        }
    }

    let payload = serde_json::to_vec_pretty(state)?;
    let temp_path = temp_path_for(state_path);
    fs::write(&temp_path, payload)?;
    match fs::remove_file(state_path) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(StoreError::Io(error)),
    }
    fs::rename(temp_path, state_path)?;
    Ok(())
}

fn temp_path_for(path: &PathBuf) -> PathBuf {
    if let Some(file_name) = path.file_name() {
        path.with_file_name(format!("{}.tmp", file_name.to_string_lossy()))
    } else {
        path.with_extension("tmp")
    }
}

fn normalize_session(
    state_path: &PathBuf,
    configured_session_id: Option<&str>,
    session: Option<JsonMap<String, Value>>,
) -> JsonMap<String, Value> {
    let mut normalized = session.unwrap_or_default();
    normalized
        .entry("session_id".to_owned())
        .or_insert_with(|| {
            json!(configured_session_id.map(str::to_owned).unwrap_or_else(|| {
                state_path.file_stem().map_or_else(
                    || "state".to_owned(),
                    |stem| stem.to_string_lossy().into_owned(),
                )
            }))
        });
    normalized
        .entry("created_at".to_owned())
        .or_insert_with(|| json!(utc_now()));
    normalized.insert(
        "state_path".to_owned(),
        json!(state_path.to_string_lossy().into_owned()),
    );
    normalized
}

fn normalize_message(mut message: Message) -> Message {
    let acked_at = message.acked_at.clone().or(message.consumed_at.clone());
    let acked_by = message.acked_by.clone().or(message.consumed_by.clone());
    message.acked_at = acked_at.clone();
    message.acked_by = acked_by.clone();
    message.consumed_at = acked_at;
    message.consumed_by = acked_by;
    message
}

fn ensure_agent<'a>(state: &'a mut State, agent_name: &str) -> &'a mut Agent {
    state
        .agents
        .entry(agent_name.to_owned())
        .or_insert_with(|| Agent::new(agent_name))
}

fn ensure_profile<'a>(state: &'a mut State, agent_name: &str) -> &'a mut AgentProfile {
    state
        .profiles
        .entry(agent_name.to_owned())
        .or_insert_with(|| AgentProfile::new(agent_name))
}

fn built_in_profile(agent_name: &str) -> Option<AgentProfile> {
    match agent_name {
        "backend-agent" => {
            let mut metadata = JsonMap::new();
            metadata.insert("repo_kind".to_owned(), json!("backend"));
            Some(AgentProfile {
                agent_name: "backend-agent".to_owned(),
                peer_name: Some("client-agent".to_owned()),
                repo_path: None,
                handoff_path: Some("SYNC_BACKEND_AGENT_HANDOFF.md".to_owned()),
                notes: Some(
                    "Built-in backend role profile. The handoff path is relative to the backend repo root."
                        .to_owned(),
                ),
                metadata,
                created_at: None,
                updated_at: None,
            })
        }
        "client-agent" => {
            let mut metadata = JsonMap::new();
            metadata.insert("repo_kind".to_owned(), json!("client"));
            Some(AgentProfile {
                agent_name: "client-agent".to_owned(),
                peer_name: Some("backend-agent".to_owned()),
                repo_path: None,
                handoff_path: Some("SYNC_CLIENT_AGENT_HANDOFF.md".to_owned()),
                notes: Some(
                    "Built-in client role profile. The handoff path is relative to the client repo root."
                        .to_owned(),
                ),
                metadata,
                created_at: None,
                updated_at: None,
            })
        }
        _ => None,
    }
}

fn merged_profile(state: &State, agent_name: &str) -> Option<Value> {
    let built_in = built_in_profile(agent_name);
    let override_profile = state.profiles.get(agent_name).cloned();
    let (mut merged, source, has_override) = match (built_in, override_profile) {
        (None, None) => return None,
        (Some(profile), None) => (profile, "built_in", false),
        (None, Some(profile)) => (profile, "persisted", true),
        (Some(mut built_in), Some(override_profile)) => {
            if override_profile.peer_name.is_some() {
                built_in.peer_name = override_profile.peer_name;
            }
            if override_profile.repo_path.is_some() {
                built_in.repo_path = override_profile.repo_path;
            }
            if override_profile.handoff_path.is_some() {
                built_in.handoff_path = override_profile.handoff_path;
            }
            if override_profile.notes.is_some() {
                built_in.notes = override_profile.notes;
            }
            if override_profile.created_at.is_some() {
                built_in.created_at = override_profile.created_at;
            }
            if override_profile.updated_at.is_some() {
                built_in.updated_at = override_profile.updated_at;
            }
            built_in.metadata.extend(override_profile.metadata);
            (built_in, "built_in+override", true)
        }
    };

    let mut value = match to_value(&mut merged) {
        Value::Object(object) => object,
        _ => JsonMap::new(),
    };
    value.insert("profile_source".to_owned(), json!(source));
    value.insert("has_persisted_override".to_owned(), json!(has_override));
    Some(Value::Object(value))
}

fn list_agent_profiles_locked(state: &State) -> Vec<Value> {
    let names: BTreeSet<String> = ["backend-agent".to_owned(), "client-agent".to_owned()]
        .into_iter()
        .chain(state.profiles.keys().cloned())
        .collect();
    names
        .iter()
        .filter_map(|agent_name| merged_profile(state, agent_name))
        .collect()
}

fn agent_status(state: &mut State, agent_name: &str) -> Value {
    let pending_messages = pending_message_count(state, agent_name);
    let task_status = state.task.status.clone();
    let agent = ensure_agent(state, agent_name).clone();
    let mut value = match to_value(&agent) {
        Value::Object(object) => object,
        _ => JsonMap::new(),
    };
    value.insert("pending_messages".to_owned(), json!(pending_messages));
    value.insert("task_status".to_owned(), json!(task_status));
    Value::Object(value)
}

fn pending_message_count(state: &State, agent_name: &str) -> usize {
    state
        .messages
        .iter()
        .filter(|message| message.to_agent == agent_name && !is_message_acked(message))
        .count()
}

fn pending_messages(state: &State, agent_name: &str) -> Vec<Message> {
    state
        .messages
        .iter()
        .filter(|message| message.to_agent == agent_name && !is_message_acked(message))
        .cloned()
        .collect()
}

fn first_pending_message_index(state: &State, agent_name: &str) -> Option<usize> {
    state
        .messages
        .iter()
        .position(|message| message.to_agent == agent_name && !is_message_acked(message))
}

fn peek_event(state: &mut State, agent_name: &str) -> Option<Value> {
    if let Some(index) = first_pending_message_index(state, agent_name) {
        let message = &mut state.messages[index];
        message.delivery_count += 1;
        message.last_delivered_at = Some(utc_now());
        message.last_delivered_to = Some(agent_name.to_owned());
        return Some(json!({
            "event_type": "handoff",
            "task_status": state.task.status,
            "message": message,
        }));
    }

    if state.task.status == "completed" {
        return Some(json!({
            "event_type": "task_completed",
            "task": state.task,
        }));
    }

    None
}

fn is_message_acked(message: &Message) -> bool {
    message.acked_at.is_some() || message.consumed_at.is_some()
}

fn profile_string(profile: &Option<Value>, field_name: &str) -> Option<String> {
    profile
        .as_ref()
        .and_then(|profile| profile.get(field_name))
        .and_then(Value::as_str)
        .map(str::to_owned)
}

fn effective_metadata(
    profile: &Option<Value>,
    metadata_override: Option<JsonMap<String, Value>>,
) -> JsonMap<String, Value> {
    let mut metadata = profile
        .as_ref()
        .and_then(|profile| profile.get("metadata"))
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    if let Some(metadata_override) = metadata_override {
        metadata.extend(metadata_override);
    }
    metadata
}

fn next_actions(agent_name: &str, handoff_path: Option<&str>) -> Vec<String> {
    let handoff_note = handoff_path.map_or_else(
        || "Your handoff file path is not configured yet.".to_owned(),
        |handoff_path| format!("Your local handoff file path is {handoff_path}."),
    );
    vec![
        format!("Treat this session as agent role '{agent_name}'."),
        handoff_note,
        format!(
            "Call pull_updates with wait=false before starting new work. If you expected a startup handoff and got none, inspect sync://agent/{agent_name}/mailbox and sync://agent/{agent_name}/history."
        ),
        "If a handoff event is returned, write message.content into the local handoff file and then call ack_handoff with message.message_id.".to_owned(),
        "After each iteration, write the local handoff file, then send_handoff with its full content.".to_owned(),
        "After send_handoff, call pull_updates with wait=true to wait for the peer response.".to_owned(),
        "If task_completed is returned, stop the loop instead of sending another unchanged handoff.".to_owned(),
    ]
}

fn event_types() -> Value {
    json!({
        "none": "No queued handoff is currently pending for this agent.",
        "handoff": "A peer handoff is available in message.content and remains pending until ack_handoff succeeds.",
        "task_completed": "The shared task was marked complete and the loop should stop.",
        "timeout": "No event arrived before the requested timeout elapsed.",
    })
}

fn resource_contents(uri: &str, payload: Value) -> Value {
    json!({
        "contents": [
            {
                "uri": uri,
                "mimeType": "application/json",
                "text": pretty_json(&payload),
            }
        ]
    })
}

fn pretty_json(value: &Value) -> String {
    serde_json::to_string_pretty(value).unwrap_or_else(|_| "{}".to_owned())
}

fn to_value<T: Serialize + ?Sized>(value: &T) -> Value {
    serde_json::to_value(value).unwrap_or(Value::Null)
}

fn default_task_status() -> String {
    "active".to_owned()
}

fn utc_now() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Micros, false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    fn temp_state_path() -> PathBuf {
        let dir = std::env::temp_dir()
            .join("agent-sync-mcp-tests")
            .join(Uuid::new_v4().to_string());
        fs::create_dir_all(&dir).unwrap();
        dir.join("sync-state.json")
    }

    #[test]
    fn handoff_stays_pending_until_ack() {
        let state_path = temp_state_path();
        let store = SyncStore::new(state_path.clone(), None).unwrap();
        store
            .register_agent(
                "client-agent",
                Some("backend-agent".to_owned()),
                None,
                None,
                None,
            )
            .unwrap();
        store
            .register_agent(
                "backend-agent",
                Some("client-agent".to_owned()),
                None,
                None,
                None,
            )
            .unwrap();

        let sent = store
            .send_handoff(
                "client-agent",
                "backend-agent",
                "persist until ack",
                None,
                Some("needs ack".to_owned()),
                None,
            )
            .unwrap();
        let first = store.poll_event("backend-agent").unwrap().unwrap();
        let second = store.poll_event("backend-agent").unwrap().unwrap();

        assert_eq!(first["event_type"], "handoff");
        assert_eq!(second["event_type"], "handoff");
        assert_eq!(first["message"]["message_id"], sent["message_id"]);
        assert_eq!(second["message"]["message_id"], sent["message_id"]);
        assert_eq!(
            store
                .ack_handoff("backend-agent", sent["message_id"].as_i64().unwrap())
                .unwrap()["acked_by"],
            "backend-agent"
        );
        assert!(store.poll_event("backend-agent").unwrap().is_none());

        let _ = fs::remove_dir_all(state_path.parent().unwrap());
    }

    #[test]
    fn builtin_profile_bootstrap_is_ready() {
        let state_path = temp_state_path();
        let store = SyncStore::new(state_path.clone(), None).unwrap();
        let bootstrap = store
            .bootstrap_agent_session("backend-agent", true, None, None, None, None)
            .unwrap();

        assert_eq!(bootstrap["profile_found"], true);
        assert_eq!(bootstrap["profile"]["profile_source"], "built_in");
        assert_eq!(bootstrap["readiness"], "ready");
        assert_eq!(
            bootstrap["effective_config"]["handoff_path"],
            "SYNC_BACKEND_AGENT_HANDOFF.md"
        );
        assert_eq!(
            bootstrap["registration"]["metadata"]["repo_kind"],
            "backend"
        );

        let _ = fs::remove_dir_all(state_path.parent().unwrap());
    }

    #[tokio::test]
    async fn wait_unblocks_on_task_completion() {
        let state_path = temp_state_path();
        let store = SyncStore::new(state_path.clone(), None).unwrap();
        store
            .register_agent(
                "client-agent",
                Some("backend-agent".to_owned()),
                None,
                None,
                None,
            )
            .unwrap();
        store
            .register_agent(
                "backend-agent",
                Some("client-agent".to_owned()),
                None,
                None,
                None,
            )
            .unwrap();

        let waiter_store = store.clone();
        let waiter = tokio::spawn(async move {
            waiter_store
                .wait_for_event("client-agent", Some(2.0))
                .await
                .unwrap()
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        store
            .mark_task_complete(
                "backend-agent",
                Some("No more work remains.".to_owned()),
                None,
            )
            .unwrap();

        let event = waiter.await.unwrap();
        assert_eq!(event["event_type"], "task_completed");
        assert_eq!(event["task"]["completed_by"], "backend-agent");

        let _ = fs::remove_dir_all(state_path.parent().unwrap());
    }
}
