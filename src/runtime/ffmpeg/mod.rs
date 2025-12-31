// src/runtime/ffmpeg/mod.rs

use std::collections::HashMap;
use std::path::PathBuf;
use std::process::Command;
use tracing::{debug, error, info, warn};

/// 描述一个已安装的 vtx-ffmpeg 二进制文件
#[derive(Debug, Clone)]
pub struct FfmpegBinary {
    /// 文件的绝对路径
    pub path: PathBuf,
    /// 解析出的版本号 (e.g., "v0.1.3")
    pub version: String,
    /// 完整的构建标识 (e.g., "vtx-v0.1.3-a5e16b0")
    pub identity: String,
    /// 所属 Profile (e.g., "nano", "full")
    pub profile: String,
}

/// VtxFfmpeg 工具链管理器
///
/// 职责：扫描、验证并提供最佳匹配的 FFmpeg 二进制文件。
/// 支持 Profile 自动升级策略 (Fallback)：如果请求的轻量级 Profile 不存在，
/// 会尝试使用功能更全的 Profile 代替。
pub struct VtxFfmpegManager {
    /// Profile 映射表
    profiles: HashMap<String, FfmpegBinary>,
    binary_root: PathBuf,
    /// 子进程执行超时时间（秒）
    pub execution_timeout_secs: u64,
}

impl VtxFfmpegManager {
    /// 初始化管理器并执行首次扫描
    ///
    /// # 参数
    /// - `binary_root`: 存放 vtx-ffmpeg 二进制文件的目录路径
    /// - `execution_timeout_secs`: 子进程最大执行时长
    pub fn new(binary_root: PathBuf, execution_timeout_secs: u64) -> Self {
        let mut manager = Self {
            profiles: HashMap::new(),
            binary_root,
            execution_timeout_secs,
        };
        manager.scan();
        manager
    }

    /// 扫描并验证二进制文件
    fn scan(&mut self) {
        info!("[VtxFfmpeg] Scanning binaries in: {:?}", self.binary_root);

        if !self.binary_root.exists() {
            warn!("[VtxFfmpeg] Binary root not found: {:?}", self.binary_root);
            return;
        }

        let entries = match std::fs::read_dir(&self.binary_root) {
            Ok(e) => e,
            Err(e) => {
                error!("[VtxFfmpeg] Failed to read directory: {}", e);
                return;
            }
        };

        for entry in entries.flatten() {
            let path = entry.path();
            if is_executable(&path) {
                if let Some(filename) = path.file_name().and_then(|n| n.to_str()) {
                    if filename.starts_with("vtx-ffmpeg-") {
                        // 解析 Profile 名称
                        let profile_raw = &filename["vtx-ffmpeg-".len()..];
                        let profile = profile_raw.split('.').next().unwrap_or(profile_raw).to_string();

                        // 验证并获取版本信息
                        match verify_binary(&path) {
                            Ok((identity, version)) => {
                                let binary = FfmpegBinary {
                                    path: path.clone(),
                                    version,
                                    identity,
                                    profile: profile.clone(),
                                };
                                debug!("[VtxFfmpeg] Registered: {} ({})", binary.profile, binary.version);
                                self.profiles.insert(profile, binary);
                            }
                            Err(e) => {
                                warn!("[VtxFfmpeg] Skipped invalid binary '{}': {}", filename, e);
                            }
                        }
                    }
                }
            }
        }

        info!(
            "[VtxFfmpeg] Initialization complete. Available profiles: {:?}",
            self.profiles.keys().collect::<Vec<_>>()
        );
    }

    /// 获取最佳匹配的二进制文件
    ///
    /// 策略：
    /// 1. 精确匹配请求的 Profile。
    /// 2. 如果未找到，尝试按“通用能力链”升级查找 (nano -> micro -> mini -> full)。
    /// 3. 特殊 Profile (如 stream, transcode) 暂不自动回退，除非显式指定。
    pub fn get_binary(&self, requested_profile: &str) -> Option<&FfmpegBinary> {
        // 尝试精确匹配
        if let Some(bin) = self.profiles.get(requested_profile) {
            return Some(bin);
        }

        // 尝试智能回退 (仅针对通用层级)
        let fallback_chain = ["nano", "micro", "mini", "full"];

        // 只有当请求的 profile 在链条中时，才向后查找
        if let Some(start_idx) = fallback_chain.iter().position(|&p| p == requested_profile) {
            for &fallback_profile in fallback_chain.iter().skip(start_idx + 1) {
                if let Some(bin) = self.profiles.get(fallback_profile) {
                    debug!(
                        "[VtxFfmpeg] Fallback: '{}' not found, using '{}' instead.",
                        requested_profile, fallback_profile
                    );
                    return Some(bin);
                }
            }
        }

        warn!("[VtxFfmpeg] No suitable binary found for profile: {}", requested_profile);
        None
    }

}

/// 检查文件是否像是可执行文件
fn is_executable(path: &PathBuf) -> bool {
    path.is_file()
}

/// 运行 `ffmpeg -version` 获取元数据
fn verify_binary(path: &PathBuf) -> anyhow::Result<(String, String)> {
    let output = Command::new(path)
        .arg("-version")
        .output()
        .map_err(|e| anyhow::anyhow!("Failed to execute: {}", e))?;

    if !output.status.success() {
        return Err(anyhow::anyhow!("Non-zero exit code"));
    }

    let stdout = String::from_utf8_lossy(&output.stdout);

    let identity = stdout
        .split_whitespace()
        .find(|s| s.starts_with("vtx-"))
        .unwrap_or("unknown-version")
        .to_string();

    let version = identity.split('-').nth(1).unwrap_or("0.0.0").to_string();

    Ok((identity, version))
}
