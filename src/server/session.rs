use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use serde_json::Value;
use tokio::sync::Notify;
use uuid::Uuid;

type RequestCancellations = HashMap<String, Arc<Notify>>;

struct Session {
    protocol_version: String,
    last_activity: Instant,
    requests: RequestCancellations,
}

struct RegistryState {
    sessions: Mutex<HashMap<String, Session>>,
    max_sessions: usize,
    idle_timeout: Duration,
}

#[derive(Clone)]
pub(super) struct SessionRegistry {
    state: Arc<RegistryState>,
}

#[derive(Debug, Eq, PartialEq)]
pub(super) enum SessionAccessError {
    NotFound,
    VersionMismatch { expected: String },
    InvalidRequestId,
    DuplicateRequestId,
}

#[derive(Debug, Eq, PartialEq)]
pub(super) enum SessionCreateError {
    CapacityExceeded,
}

impl Default for SessionRegistry {
    fn default() -> Self {
        Self::new(128, Duration::from_secs(3600))
    }
}

impl SessionRegistry {
    pub(super) fn new(max_sessions: usize, idle_timeout: Duration) -> Self {
        Self {
            state: Arc::new(RegistryState {
                sessions: Mutex::new(HashMap::new()),
                max_sessions: max_sessions.max(1),
                idle_timeout,
            }),
        }
    }

    pub(super) fn create(&self, protocol_version: &str) -> Result<String, SessionCreateError> {
        let now = Instant::now();
        let mut sessions = self
            .state
            .sessions
            .lock()
            .expect("session registry mutex poisoned");
        reap_idle_locked(&mut sessions, self.state.idle_timeout, now);
        if sessions.len() >= self.state.max_sessions {
            return Err(SessionCreateError::CapacityExceeded);
        }
        let session_id = Uuid::new_v4().to_string();
        sessions.insert(
            session_id.clone(),
            Session {
                protocol_version: protocol_version.to_owned(),
                last_activity: now,
                requests: HashMap::new(),
            },
        );
        Ok(session_id)
    }

    pub(super) fn begin_request(
        &self,
        session_id: &str,
        protocol_version: &str,
        request_id: &Value,
    ) -> Result<InFlightRequest, SessionAccessError> {
        let request_key = request_key(request_id).ok_or(SessionAccessError::InvalidRequestId)?;
        let cancellation = Arc::new(Notify::new());
        let mut sessions = self
            .state
            .sessions
            .lock()
            .expect("session registry mutex poisoned");
        let session = validated_session(&mut sessions, session_id, protocol_version)?;
        session.last_activity = Instant::now();
        if session.requests.contains_key(&request_key) {
            return Err(SessionAccessError::DuplicateRequestId);
        }
        session
            .requests
            .insert(request_key.clone(), cancellation.clone());
        Ok(InFlightRequest {
            registry: self.clone(),
            session_id: session_id.to_owned(),
            request_key,
            cancellation,
        })
    }

    pub(super) fn validate(
        &self,
        session_id: &str,
        protocol_version: &str,
    ) -> Result<(), SessionAccessError> {
        let mut sessions = self
            .state
            .sessions
            .lock()
            .expect("session registry mutex poisoned");
        validated_session(&mut sessions, session_id, protocol_version)?.last_activity =
            Instant::now();
        Ok(())
    }

    pub(super) fn cancel(
        &self,
        session_id: &str,
        protocol_version: &str,
        request_id: &Value,
    ) -> Result<(), SessionAccessError> {
        let request_key = request_key(request_id).ok_or(SessionAccessError::InvalidRequestId)?;
        let mut sessions = self
            .state
            .sessions
            .lock()
            .expect("session registry mutex poisoned");
        let session = validated_session(&mut sessions, session_id, protocol_version)?;
        session.last_activity = Instant::now();
        if let Some(cancellation) = session.requests.get(&request_key) {
            cancellation.notify_one();
        }
        Ok(())
    }

    pub(super) fn terminate(
        &self,
        session_id: &str,
        protocol_version: &str,
    ) -> Result<(), SessionAccessError> {
        let mut sessions = self
            .state
            .sessions
            .lock()
            .expect("session registry mutex poisoned");
        validated_session(&mut sessions, session_id, protocol_version)?;
        cancel_session(sessions.remove(session_id));
        Ok(())
    }

    pub(super) fn reap_idle(&self) -> usize {
        let mut sessions = self
            .state
            .sessions
            .lock()
            .expect("session registry mutex poisoned");
        reap_idle_locked(&mut sessions, self.state.idle_timeout, Instant::now())
    }

    pub(super) fn terminate_all(&self) -> usize {
        let sessions = std::mem::take(
            &mut *self
                .state
                .sessions
                .lock()
                .expect("session registry mutex poisoned"),
        );
        let count = sessions.len();
        for session in sessions.into_values() {
            cancel_session(Some(session));
        }
        count
    }

    #[cfg(test)]
    pub(super) fn contains(&self, session_id: &str) -> bool {
        self.state
            .sessions
            .lock()
            .expect("session registry mutex poisoned")
            .contains_key(session_id)
    }

    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        self.state
            .sessions
            .lock()
            .expect("session registry mutex poisoned")
            .len()
    }

    #[cfg(test)]
    pub(super) fn has_in_flight_request(&self, session_id: &str, request_id: &Value) -> bool {
        request_key(request_id).is_some_and(|request_key| {
            self.state
                .sessions
                .lock()
                .expect("session registry mutex poisoned")
                .get(session_id)
                .is_some_and(|session| session.requests.contains_key(&request_key))
        })
    }
}

pub(super) struct InFlightRequest {
    registry: SessionRegistry,
    session_id: String,
    request_key: String,
    cancellation: Arc<Notify>,
}

impl InFlightRequest {
    pub(super) fn cancellation(&self) -> Arc<Notify> {
        self.cancellation.clone()
    }
}

impl Drop for InFlightRequest {
    fn drop(&mut self) {
        let mut sessions = self
            .registry
            .state
            .sessions
            .lock()
            .expect("session registry mutex poisoned");
        let Some(session) = sessions.get_mut(&self.session_id) else {
            return;
        };
        if session
            .requests
            .get(&self.request_key)
            .is_some_and(|current| Arc::ptr_eq(current, &self.cancellation))
        {
            session.requests.remove(&self.request_key);
            session.last_activity = Instant::now();
        }
    }
}

fn validated_session<'a>(
    sessions: &'a mut HashMap<String, Session>,
    session_id: &str,
    protocol_version: &str,
) -> Result<&'a mut Session, SessionAccessError> {
    let session = sessions
        .get_mut(session_id)
        .ok_or(SessionAccessError::NotFound)?;
    if session.protocol_version != protocol_version {
        return Err(SessionAccessError::VersionMismatch {
            expected: session.protocol_version.clone(),
        });
    }
    Ok(session)
}

fn reap_idle_locked(
    sessions: &mut HashMap<String, Session>,
    idle_timeout: Duration,
    now: Instant,
) -> usize {
    let expired_ids = sessions
        .iter()
        .filter(|(_, session)| {
            session.requests.is_empty() && now.duration_since(session.last_activity) >= idle_timeout
        })
        .map(|(session_id, _)| session_id.clone())
        .collect::<Vec<_>>();
    let count = expired_ids.len();
    for session_id in expired_ids {
        cancel_session(sessions.remove(&session_id));
    }
    count
}

fn cancel_session(session: Option<Session>) {
    if let Some(session) = session {
        for cancellation in session.requests.into_values() {
            cancellation.notify_one();
        }
    }
}

fn request_key(request_id: &Value) -> Option<String> {
    match request_id {
        Value::String(_) | Value::Number(_) => serde_json::to_string(request_id).ok(),
        _ => None,
    }
}
