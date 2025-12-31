use wasmtime::component::Resource;
use crate::common::buffer::{BufferType, RealBuffer};
use crate::runtime::context::{SecurityPolicy, StreamContext};
use crate::runtime::host_impl::ffmpeg_policy::validate_ffmpeg_args;
use super::api;
use std::process::Stdio;

#[async_trait::async_trait]
impl api::ffmpeg::Host for StreamContext {
    async fn execute(
        &mut self,
        params: api::ffmpeg::TranscodeParams,
    ) -> Result<Resource<RealBuffer>, String> {
        if self.policy == SecurityPolicy::Plugin && !self.has_permission("ffmpeg:execute") {
            return Err("Permission Denied".into());
        }
        if self.policy == SecurityPolicy::Restricted {
            tracing::warn!("[Security] VtxFfmpeg execution denied (Restricted mode).");
            return Err("Permission Denied: VtxFfmpeg requires root privileges".into());
        }

        // 获取最佳匹配的二进制文件
        let binary = self
            .vtx_ffmpeg
            .get_binary(&params.profile)
            .ok_or_else(|| format!(
                "Profile '{}' (or compatible alternative) not installed on host",
                params.profile
            ))?;

        // [Fix] 统一返回类型为 (String, bool)
        let (input_arg, use_stdin_pipe) = if params.input_id == "pipe:0" {
            ("pipe:0".to_string(), true)
        } else {
            let path = self
                .registry
                .get_path(&params.input_id)
                .ok_or_else(|| format!("Input video ID '{}' not found", params.input_id))?;
            // 将 PathBuf 转为 String
            (path.to_string_lossy().to_string(), false)
        };

        let _timeout_secs = self.vtx_ffmpeg.execution_timeout_secs;

        tracing::info!(
            "[VtxFfmpeg] Spawn: Profile='{}' (Using: {} {}), Input='{}' (Pipe Mode: {})",
            params.profile,
            binary.profile,
            binary.version,
            params.input_id,
            use_stdin_pipe
        );

        let handle = tokio::runtime::Handle::try_current()
            .map_err(|e| format!("Failed to get tokio runtime: {}", e))?;

        validate_ffmpeg_args(&params.args)?;

        let binary_path = binary.path.clone();
        let child_result = handle.block_on(async {
            let mut cmd = tokio::process::Command::new(&binary_path);

            cmd.arg("-i").arg(&input_arg);
            cmd.args(&params.args);

            cmd.stdout(Stdio::piped());
            cmd.stderr(Stdio::inherit());
            cmd.kill_on_drop(true);

            if use_stdin_pipe {
                cmd.stdin(Stdio::piped());
            } else {
                cmd.stdin(Stdio::null());
            }

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
