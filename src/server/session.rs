use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use serde_json::Value;
use tokio::sync::Notify;
use uuid::Uuid;

type RequestCancellations = HashMap<String, Arc<Notify>>;
type Sessions = HashMap<String, RequestCancellations>;

#[derive(Clone, Default)]
pub(super) struct SessionRegistry {
    sessions: Arc<Mutex<Sessions>>,
}

impl SessionRegistry {
    pub(super) fn create(&self) -> String {
        let session_id = Uuid::new_v4().to_string();
        self.sessions
            .lock()
            .expect("session registry mutex poisoned")
            .insert(session_id.clone(), HashMap::new());
        session_id
    }

    pub(super) fn contains(&self, session_id: &str) -> bool {
        self.sessions
            .lock()
            .expect("session registry mutex poisoned")
            .contains_key(session_id)
    }

    pub(super) fn register(&self, session_id: &str, request_id: &Value) -> Option<InFlightRequest> {
        let request_key = request_key(request_id)?;
        let cancellation = Arc::new(Notify::new());
        self.sessions
            .lock()
            .expect("session registry mutex poisoned")
            .get_mut(session_id)?
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
            .and_then(|requests| requests.get(&request_key))
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
                .is_some_and(|requests| requests.contains_key(&request_key))
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
        let Some(requests) = sessions.get_mut(&self.session_id) else {
            return;
        };
        if requests
            .get(&self.request_key)
            .is_some_and(|current| Arc::ptr_eq(current, &self.cancellation))
        {
            requests.remove(&self.request_key);
        }
    }
}

fn request_key(request_id: &Value) -> Option<String> {
    match request_id {
        Value::String(_) | Value::Number(_) => serde_json::to_string(request_id).ok(),
        _ => None,
    }
}
