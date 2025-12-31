use wasmtime::component::Resource;

use super::api;
use crate::common::buffer::{BufferType, RealBuffer};
use crate::runtime::context::{SecurityPolicy, StreamContext};

impl api::ffmpeg::Host for StreamContext {
    fn execute(
        &mut self,
        params: api::ffmpeg::TranscodeParams,
    ) -> Result<Resource<RealBuffer>, String> {
        if self.policy == SecurityPolicy::Restricted {
            tracing::warn!("[Security] VtxFfmpeg execution denied (Restricted mode).");
            return Err("Permission Denied: VtxFfmpeg requires root privileges".into());
        }

        // 读取超时配置
        let timeout_secs = self.vtx_ffmpeg.execution_timeout_secs;
        tracing::debug!(
            "[VtxFfmpeg] Request: Profile='{}', Input='{}', Timeout={}s",
            params.profile,
            params.input_id,
            timeout_secs
        );

        let binary_path = self
            .vtx_ffmpeg
            .get_binary(&params.profile)
            .ok_or_else(|| format!("Profile '{}' not found/installed on host", params.profile))?;

        let input_path = self
            .registry
            .get_path(&params.input_id)
            .ok_or_else(|| format!("Input video ID '{}' not found", params.input_id))?;

        let handle = tokio::runtime::Handle::try_current()
            .map_err(|e| format!("Failed to get tokio runtime: {}", e))?;

        let child_result = handle.block_on(async {
            let mut cmd = tokio::process::Command::new(binary_path);
            cmd.arg("-i").arg(input_path);
            cmd.args(&params.args);
            cmd.stdout(std::process::Stdio::piped());
            cmd.stderr(std::process::Stdio::inherit());
            cmd.stdin(std::process::Stdio::null());
            cmd.kill_on_drop(true);

            // TODO: 利用 timeout_secs 实现更复杂的超时控制逻辑

            cmd.spawn()
        });

        let mut child = child_result.map_err(|e| format!("Failed to spawn vtx-ffmpeg: {}", e))?;

        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| "Failed to capture vtx-ffmpeg stdout".to_string())?;

        let rb = RealBuffer {
            inner: BufferType::Pipe(stdout),
            path_hint: None,
            mime_override: Some("video/mp4".to_string()),
            process_handle: Some(child),
        };

        self.table
            .push(rb)
            .map_err(|e| format!("Resource Table Error: {}", e))
    }
}
