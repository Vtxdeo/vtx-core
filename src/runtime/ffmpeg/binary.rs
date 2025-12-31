use std::path::PathBuf;
use std::process::Command;

/// 描述一个已安装的 vtx-ffmpeg 二进制文件
#[derive(Debug, Clone)]
pub struct FfmpegBinary {
    /// 文件的绝对路径
    pub path: PathBuf,
    /// 解析出的版本号 (e.g., "v0.1.3")
    pub version: String,
    /// 完整的构建标识 (e.g., "vtx-v0.1.3-a5e16b0" 或 "system-ffmpeg-4.4.2")
    // [Fix] 允许死代码警告，保留字段用于 Debug 和潜在的日志需求
    #[allow(dead_code)]
    pub identity: String,
    /// 所属 Profile (e.g., "nano", "full", "system")
    pub profile: String,
}

/// 运行 `ffmpeg -version` 获取元数据
///
/// 兼容 vtx 定制版输出格式与标准 FFmpeg 输出格式
pub fn verify_binary(path: &PathBuf) -> anyhow::Result<(String, String)> {
    let output = Command::new(path)
        .arg("-version")
        .output()
        .map_err(|e| anyhow::anyhow!("Failed to execute: {}", e))?;

    if !output.status.success() {
        return Err(anyhow::anyhow!("Non-zero exit code"));
    }

    let stdout = String::from_utf8_lossy(&output.stdout);

    // 优先尝试解析 vtx 标识
    // 示例输出: "vtx-v0.1.3-a5e16b0 Copyright (c) ..."
    if let Some(vtx_id) = stdout.split_whitespace().find(|s| s.starts_with("vtx-")) {
        let version = vtx_id.split('-').nth(1).unwrap_or("0.0.0").to_string();
        return Ok((vtx_id.to_string(), version));
    }

    // 尝试解析标准 FFmpeg 版本
    // 示例输出: "ffmpeg version 4.4.2-0ubuntu0.22.04.1 Copyright (c) ..."
    if stdout.starts_with("ffmpeg version") {
        let parts: Vec<&str> = stdout.split_whitespace().collect();
        if parts.len() >= 3 {
            let version = parts[2].to_string(); // "4.4.2-..."
            let identity = format!("system-ffmpeg-{}", version);
            return Ok((identity, version));
        }
    }

    // 无法识别版本格式，返回默认兜底
    Ok(("unknown-build".to_string(), "0.0.0".to_string()))
}
