use futures_util::StreamExt;
use tokio::io::{self, AsyncWriteExt, BufReader};
use tokio::sync::mpsc;
use tokio_util::codec::{FramedRead, LinesCodec};
use tracing::{error, info, warn};

use crate::common::events::SystemRequest;
use crate::common::ipc::{DependencyPayload, IpcEnvelope, StatusPayload, SystemPayload};
use crate::common::json_guard::check_json_limits;

pub struct IpcTransport;

impl IpcTransport {
    pub fn spawn(mut rx_outbound: mpsc::Receiver<SystemRequest>) {
        tokio::spawn(async move {
            let mut stdout = io::stdout();

            while let Some(req) = rx_outbound.recv().await {
                let envelope = match req {
                    SystemRequest::RequestDependency {
                        name,
                        profile,
                        version,
                    } => IpcEnvelope::new(
                        "SYS_REQ_DEPENDENCY",
                        SystemPayload::Dependency(DependencyPayload {
                            name,
                            profile,
                            version,
                        }),
                    ),
                    SystemRequest::ReportStatus { code, message } => IpcEnvelope::new(
                        "SYS_REPORT_STATUS",
                        SystemPayload::Status(StatusPayload { code, message }),
                    ),
                };

                match serde_json::to_string(&envelope) {
                    Ok(json) => {
                        if stdout.write_all(json.as_bytes()).await.is_ok()
                            && stdout.write_all(b"\n").await.is_ok()
                        {
                            let _ = stdout.flush().await;
                        }
                    }
                    Err(e) => {
                        error!("[IPC] Failed to serialize outbound message: {}", e);
                    }
                }
            }
        });

        tokio::spawn(async move {
            const MAX_IPC_LINE_BYTES: usize = 64 * 1024;
            const MAX_IPC_JSON_DEPTH: usize = 20;

            let stdin = io::stdin();
            let reader = BufReader::new(stdin);
            let mut lines =
                FramedRead::new(reader, LinesCodec::new_with_max_length(MAX_IPC_LINE_BYTES));

            while let Some(result) = lines.next().await {
                match result {
                    Ok(line) => {
                        if line.trim().is_empty() {
                            continue;
                        }

                        if let Err(reason) =
                            check_json_limits(&line, MAX_IPC_LINE_BYTES, MAX_IPC_JSON_DEPTH)
                        {
                            warn!("[IPC] Rejected inbound message: {}", reason);
                            continue;
                        }

                        match serde_json::from_str::<serde_json::Value>(&line) {
                            Ok(value) => {
                                let msg_type =
                                    value.get("t").and_then(|v| v.as_str()).unwrap_or("UNKNOWN");

                                match msg_type {
                                    "SYS_RESOURCE_READY" => {
                                        info!("[IPC] Resource ready signal received.");
                                    }
                                    "CTRL_SHUTDOWN" => {
                                        info!("[IPC] Shutdown signal received.");
                                    }
                                    _ => {
                                        warn!("[IPC] Unknown message type: {}", msg_type);
                                    }
                                }
                            }
                            Err(e) => {
                                error!("[IPC] Malformed JSON received: {}", e);
                            }
                        }
                    }
                    Err(e) => {
                        error!("[IPC] Failed to read inbound message: {}", e);
                    }
                }
            }
        });
    }
}
