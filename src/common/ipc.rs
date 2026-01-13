use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct IpcEnvelope<T> {
    pub v: u8,
    pub t: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    pub p: T,
}

impl<T> IpcEnvelope<T> {
    pub fn new(msg_type: &str, payload: T) -> Self {
        Self {
            v: 1,
            t: msg_type.to_string(),
            id: Some(Uuid::new_v4().to_string()),
            p: payload,
        }
    }

    #[allow(dead_code)]
    pub fn response(req_id: &str, msg_type: &str, payload: T) -> Self {
        Self {
            v: 1,
            t: msg_type.to_string(),
            id: Some(req_id.to_string()),
            p: payload,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DependencyPayload {
    pub name: String,
    pub profile: String,
    pub version: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct StatusPayload {
    pub code: u16,
    pub message: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(untagged)]
pub enum SystemPayload {
    Dependency(DependencyPayload),
    Status(StatusPayload),
}
