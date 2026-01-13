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

// 定义 Core 向 Omni 发出的请求
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SystemRequest {
    RequestDependency {
        name: String,    // e.g., "ffmpeg"
        profile: String, // e.g., "animator"
        version: String,
    },
    ReportStatus {
        code: u16,
        message: String,
    },
}
