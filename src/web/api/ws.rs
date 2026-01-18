use crate::runtime::bus::EventBus;
use crate::web::state::AppState;
use axum::extract::{
    ws::{Message, WebSocket, WebSocketUpgrade},
    Query, State,
};
use axum::response::IntoResponse;
use serde::Deserialize;
use std::sync::Arc;
use uuid::Uuid;

#[derive(Deserialize)]
pub struct WsQuery {
    pub topics: Option<String>,
}

pub async fn ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<Arc<AppState>>,
    Query(query): Query<WsQuery>,
) -> impl IntoResponse {
    let topics = parse_topics(query.topics);
    let event_bus = state.event_bus.clone();
    ws.on_upgrade(move |socket| handle_socket(socket, event_bus, topics))
}

fn parse_topics(raw: Option<String>) -> Vec<String> {
    let Some(raw) = raw else {
        return vec!["*".to_string()];
    };

    let mut topics: Vec<String> = raw
        .split(',')
        .map(|t| t.trim())
        .filter(|t| !t.is_empty())
        .map(|t| t.to_string())
        .collect();

    if topics.is_empty() {
        topics.push("*".to_string());
    }

    topics
}

async fn handle_socket(mut socket: WebSocket, event_bus: Arc<EventBus>, topics: Vec<String>) {
    let client_id = format!("ws-{}", Uuid::new_v4());
    let allowed_topics = topics.clone();
    let Some(mut rx) = event_bus
        .register_plugin(&client_id, &topics, &allowed_topics)
        .await
    else {
        return;
    };

    loop {
        tokio::select! {
            event = rx.recv() => {
                let Some(event) = event else {
                    break;
                };
                match serde_json::to_string(&event) {
                    Ok(text) => {
                        if socket.send(Message::Text(text.into())).await.is_err() {
                            break;
                        }
                    }
                    Err(err) => {
                        tracing::warn!("[WebSocket] Failed to serialize event: {}", err);
                    }
                }
            }
            msg = socket.recv() => {
                match msg {
                    Some(Ok(Message::Close(_))) | None => break,
                    Some(Ok(_)) => {}
                    Some(Err(_)) => break,
                }
            }
        }
    }

    event_bus.unregister_plugin(&client_id).await;
}
