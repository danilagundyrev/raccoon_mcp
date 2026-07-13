use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use serde_json::Value;
use tokio::sync::Notify;
use uuid::Uuid;

type RequestCancellations = HashMap<String, Arc<Notify>>;
struct Session {
    protocol_version: String,
    requests: RequestCancellations,
}
type Sessions = HashMap<String, Session>;

#[derive(Clone, Default)]
pub(super) struct SessionRegistry {
    sessions: Arc<Mutex<Sessions>>,
}

impl SessionRegistry {
    pub(super) fn create(&self, protocol_version: &str) -> String {
        let session_id = Uuid::new_v4().to_string();
        self.sessions
            .lock()
            .expect("session registry mutex poisoned")
            .insert(
                session_id.clone(),
                Session {
                    protocol_version: protocol_version.to_owned(),
                    requests: HashMap::new(),
                },
            );
        session_id
    }

    pub(super) fn contains(&self, session_id: &str) -> bool {
        self.sessions
            .lock()
            .expect("session registry mutex poisoned")
            .contains_key(session_id)
    }

    pub(super) fn protocol_version(&self, session_id: &str) -> Option<String> {
        self.sessions
            .lock()
            .expect("session registry mutex poisoned")
            .get(session_id)
            .map(|session| session.protocol_version.clone())
    }

    pub(super) fn terminate(&self, session_id: &str) -> bool {
        let session = self
            .sessions
            .lock()
            .expect("session registry mutex poisoned")
            .remove(session_id);
        if let Some(session) = session {
            for cancellation in session.requests.into_values() {
                cancellation.notify_one();
            }
            true
        } else {
            false
        }
    }

    pub(super) fn terminate_all(&self) -> usize {
        let sessions = std::mem::take(
            &mut *self
                .sessions
                .lock()
                .expect("session registry mutex poisoned"),
        );
        let count = sessions.len();
        for session in sessions.into_values() {
            for cancellation in session.requests.into_values() {
                cancellation.notify_one();
            }
        }
        count
    }

    pub(super) fn register(&self, session_id: &str, request_id: &Value) -> Option<InFlightRequest> {
        let request_key = request_key(request_id)?;
        let cancellation = Arc::new(Notify::new());
        self.sessions
            .lock()
            .expect("session registry mutex poisoned")
            .get_mut(session_id)?
            .requests
            .insert(request_key.clone(), cancellation.clone());
        Some(InFlightRequest {
            registry: self.clone(),
            session_id: session_id.to_owned(),
            request_key,
            cancellation,
        })
    }

    pub(super) fn cancel(&self, session_id: &str, request_id: &Value) {
        let Some(request_key) = request_key(request_id) else {
            return;
        };
        let cancellation = self
            .sessions
            .lock()
            .expect("session registry mutex poisoned")
            .get(session_id)
            .and_then(|session| session.requests.get(&request_key))
            .cloned();
        if let Some(cancellation) = cancellation {
            cancellation.notify_one();
        }
    }

    #[cfg(test)]
    pub(super) fn has_in_flight_request(&self, session_id: &str, request_id: &Value) -> bool {
        request_key(request_id).is_some_and(|request_key| {
            self.sessions
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
        }
    }
}

fn request_key(request_id: &Value) -> Option<String> {
    match request_id {
        Value::String(_) | Value::Number(_) => serde_json::to_string(request_id).ok(),
        _ => None,
    }
}
