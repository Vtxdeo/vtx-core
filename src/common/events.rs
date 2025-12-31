use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventContext {
    pub user_id: Option<String>,
    pub username: Option<String>,
    pub request_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VtxEvent {
    pub id: String,
    pub topic: String,
    pub source: String,
    pub payload: serde_json::Value,
    pub context: EventContext,
    pub occurred_at: u64,
}
