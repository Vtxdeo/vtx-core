use crate::common::events::VtxEvent;
use std::collections::HashMap;
use std::collections::HashSet;
use tokio::sync::{mpsc, RwLock};

#[derive(Debug)]
pub struct EventBus {
    subscriptions: RwLock<HashMap<String, Vec<String>>>,
    queues: RwLock<HashMap<String, mpsc::Sender<VtxEvent>>>,
    queue_capacity: usize,
}

impl EventBus {
    pub fn new(queue_capacity: usize) -> Self {
        Self {
            subscriptions: RwLock::new(HashMap::new()),
            queues: RwLock::new(HashMap::new()),
            queue_capacity,
        }
    }

    pub async fn register_plugin(
        &self,
        plugin_id: &str,
        topics: &[String],
        allowed_topics: &[String],
    ) -> Option<mpsc::Receiver<VtxEvent>> {
        let mut receiver = None;
        let mut queues = self.queues.write().await;
        if !queues.contains_key(plugin_id) {
            let (tx, rx) = mpsc::channel(self.queue_capacity);
            queues.insert(plugin_id.to_string(), tx);
            receiver = Some(rx);
        }
        drop(queues);

        let mut subs = self.subscriptions.write().await;
        let allowed: std::collections::HashSet<String> = allowed_topics
            .iter()
            .map(|t| t.to_string())
            .collect();
        for topic in topics {
            if !allowed.contains(topic) {
                tracing::warn!(
                    "[EventBus] Subscription denied: {} -> {}",
                    plugin_id,
                    topic
                );
                continue;
            }
            let entry = subs.entry(topic.clone()).or_default();
            if !entry.iter().any(|id| id == plugin_id) {
                entry.push(plugin_id.to_string());
            }
        }
        receiver
    }

    pub async fn unregister_plugin(&self, plugin_id: &str) {
        let mut queues = self.queues.write().await;
        queues.remove(plugin_id);
        drop(queues);

        let mut subs = self.subscriptions.write().await;
        for ids in subs.values_mut() {
            ids.retain(|id| id != plugin_id);
        }
    }

    pub async fn publish(&self, event: VtxEvent) -> usize {
        let subs = self.subscriptions.read().await;
        let mut targets = HashSet::new();
        if let Some(topic_targets) = subs.get(&event.topic) {
            for id in topic_targets {
                targets.insert(id.clone());
            }
        }
        if let Some(wildcard_targets) = subs.get("*") {
            for id in wildcard_targets {
                targets.insert(id.clone());
            }
        }
        if targets.is_empty() {
            return 0;
        }

        let queues = self.queues.read().await;
        let mut delivered = 0usize;

        for plugin_id in targets {
            if let Some(tx) = queues.get(&plugin_id) {
                if tx.send(event.clone()).await.is_ok() {
                    delivered += 1;
                }
            }
        }

        delivered
    }
}
