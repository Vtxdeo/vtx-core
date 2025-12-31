use wasmtime::component::Resource;
use crate::common::buffer::{BufferType, RealBuffer};
use crate::runtime::context::{SecurityPolicy, StreamContext};
use super::api;

impl api::ffmpeg::Host for StreamContext {
    fn execute(
        &mut self,
        params: api::ffmpeg::TranscodeParams,
    ) -> Result<Resource<RealBuffer>, String> {
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

        let input_path = self
            .registry
            .get_path(&params.input_id)
            .ok_or_else(|| format!("Input video ID '{}' not found", params.input_id))?;

        let timeout_secs = self.vtx_ffmpeg.execution_timeout_secs;

        tracing::info!(
            "[VtxFfmpeg] Spawn: Profile='{}' (Using: {} {}), Input='{}'",
            params.profile,
            binary.profile,
            binary.version,
            params.input_id,
        );

        let handle = tokio::runtime::Handle::try_current()
            .map_err(|e| format!("Failed to get tokio runtime: {}", e))?;

        // 异步启动进程
        let binary_path = binary.path.clone();
        let child_result = handle.block_on(async {
            let mut cmd = tokio::process::Command::new(&binary_path);
            cmd.arg("-i").arg(input_path);
            cmd.args(&params.args);

            // 基础进程配置
            cmd.stdout(std::process::Stdio::piped());
            cmd.stderr(std::process::Stdio::inherit()); // 错误日志输出到控制台方便调试
            cmd.stdin(std::process::Stdio::null());
            cmd.kill_on_drop(true);

            // TODO: 未来在此处集成更复杂的进程池/信号量控制
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
            mime_override: Some("video/mp4".to_string()), // 默认 MIME，插件可通过其他方式覆盖
            process_handle: Some(child),
        };

        self.table
            .push(rb)
            .map_err(|e| format!("Resource Table Error: {}", e))
    }
}
