use super::api;
use crate::common::events::{EventContext, VtxEvent};
use crate::common::json_guard::check_json_limits;
use crate::runtime::context::StreamContext;
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;

impl api::event_bus::Host for StreamContext {
    async fn publish_event(&mut self, topic: String, payload: String) -> Result<(), String> {
        const MAX_EVENT_PAYLOAD_BYTES: usize = 256 * 1024;
        const MAX_EVENT_JSON_DEPTH: usize = 20;

        check_json_limits(&payload, MAX_EVENT_PAYLOAD_BYTES, MAX_EVENT_JSON_DEPTH)
            .map_err(|e| format!("Invalid event payload: {}", e))?;

        let payload_json =
            serde_json::from_str(&payload).map_err(|_| "Invalid event payload".to_string())?;

        let source = match &self.plugin_id {
            Some(id) => format!("plugin.{}", id),
            None => "plugin.unknown".to_string(),
        };

        let context = self.current_user.as_ref().map(|user| EventContext {
            user_id: Some(user.user_id.clone()),
            username: Some(user.username.clone()),
            request_id: None,
        });

        let event = VtxEvent {
            id: Uuid::new_v4().to_string(),
            topic,
            source,
            payload: payload_json,
            context: context.unwrap_or(EventContext {
                user_id: None,
                username: None,
                request_id: None,
            }),
            occurred_at: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0),
        };

        self.event_bus.publish(event).await;
        Ok(())
    }
}
