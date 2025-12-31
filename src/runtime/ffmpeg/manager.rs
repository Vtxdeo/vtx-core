use std::collections::HashMap;
use std::path::PathBuf;
use tracing::{debug, error, info, warn};

use super::binary::{verify_binary, FfmpegBinary};

/// VtxFfmpeg 工具链管理器
///
/// 职责：扫描、验证并提供最佳匹配的 FFmpeg 二进制文件。
/// 支持 Profile 自动升级策略 (Fallback)：如果请求的轻量级 Profile 不存在，
/// 会尝试使用功能更全的 Profile 代替。
///
/// IO 说明：初始化时会执行多次文件系统扫描及子进程调用 (ffmpeg -version)，
/// 耗时取决于 binary_root 下文件数量及系统响应速度。
pub struct VtxFfmpegManager {
    /// Profile 映射表
    profiles: HashMap<String, FfmpegBinary>,
    binary_root: PathBuf,
    /// 子进程执行超时时间（秒）
    pub execution_timeout_secs: u64,
    /// 是否允许回退到系统 FFmpeg
    use_system_binary: bool,
    /// 缓存的系统级 FFmpeg（如果存在且已启用）
    system_binary: Option<FfmpegBinary>,
}

impl VtxFfmpegManager {
    /// 初始化管理器并执行首次扫描
    ///
    /// # 参数
    /// - `binary_root`: 存放 vtx-ffmpeg 二进制文件的目录路径
    /// - `execution_timeout_secs`: 子进程最大执行时长
    /// - `use_system_binary`: 是否允许调用系统 PATH 中的 ffmpeg
    pub fn new(
        binary_root: PathBuf,
        execution_timeout_secs: u64,
        use_system_binary: bool,
    ) -> Self {
        let mut manager = Self {
            profiles: HashMap::new(),
            binary_root,
            execution_timeout_secs,
            use_system_binary,
            system_binary: None,
        };
        manager.scan();
        manager
    }

    /// 扫描并验证二进制文件
    fn scan(&mut self) {
        info!("[VtxFfmpeg] Scanning binaries in: {:?}", self.binary_root);

        if !self.binary_root.exists() {
            warn!("[VtxFfmpeg] Binary root not found: {:?}", self.binary_root);
        } else {
            match std::fs::read_dir(&self.binary_root) {
                Ok(entries) => {
                    for entry in entries.flatten() {
                        let path = entry.path();
                        if is_executable(&path) {
                            if let Some(filename) = path.file_name().and_then(|n| n.to_str()) {
                                if filename.starts_with("vtx-ffmpeg-") {
                                    // 解析 Profile 名称
                                    let profile_raw = &filename["vtx-ffmpeg-".len()..];
                                    let profile = profile_raw
                                        .split('.')
                                        .next()
                                        .unwrap_or(profile_raw)
                                        .to_string();

                                    // 验证并获取版本信息
                                    match verify_binary(&path) {
                                        Ok((identity, version)) => {
                                            let binary = FfmpegBinary {
                                                path: path.clone(),
                                                version,
                                                identity,
                                                profile: profile.clone(),
                                            };
                                            debug!(
                                                "[VtxFfmpeg] Registered: {} ({})",
                                                binary.profile, binary.version
                                            );
                                            self.profiles.insert(profile, binary);
                                        }
                                        Err(e) => {
                                            warn!(
                                                "[VtxFfmpeg] Skipped invalid binary '{}': {}",
                                                filename, e
                                            );
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                Err(e) => {
                    error!("[VtxFfmpeg] Failed to read directory: {}", e);
                }
            }
        }

        // 如果配置允许，探测系统级 FFmpeg
        if self.use_system_binary {
            self.detect_system_binary();
        }

        info!(
            "[VtxFfmpeg] Initialization complete. Profiles: {:?}, System Fallback: {}",
            self.profiles.keys().collect::<Vec<_>>(),
            if self.system_binary.is_some() {
                "Available"
            } else {
                "None"
            }
        );
    }

    /// 探测系统 PATH 环境变量中的 ffmpeg
    ///
    /// 副作用：执行 `ffmpeg -version` 命令
    fn detect_system_binary(&mut self) {
        let system_path = PathBuf::from("ffmpeg");

        match verify_binary(&system_path) {
            Ok((identity, version)) => {
                let binary = FfmpegBinary {
                    path: system_path,
                    version: version.clone(),
                    identity,
                    profile: "system".to_string(),
                };
                info!("[VtxFfmpeg] System FFmpeg detected: {}", binary.version);
                self.system_binary = Some(binary);
            }
            Err(e) => {
                warn!(
                    "[VtxFfmpeg] 'use_system_binary' is enabled, but 'ffmpeg' command failed: {}",
                    e
                );
            }
        }
    }

    /// 获取最佳匹配的二进制文件
    ///
    /// 策略：
    /// 1. 精确匹配请求的 Profile。
    /// 2. 如果未找到，尝试按“通用能力链”升级查找 (nano -> micro -> mini -> full)。
    /// 3. 如果仍未找到且配置允许，回退使用系统级 FFmpeg (System-Fallback)。
    /// 4. 特殊 Profile (如 stream, transcode) 暂不自动回退，除非显式指定。
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

        // 尝试系统级回退
        if let Some(sys_bin) = &self.system_binary {
            warn!(
                "[VtxFfmpeg] Profile '{}' not found in local binaries. FALLBACK to System FFmpeg. This may have performance or compatibility side effects.",
                requested_profile
            );
            return Some(sys_bin);
        }

        warn!(
            "[VtxFfmpeg] No suitable binary found for profile: {}",
            requested_profile
        );
        None
    }
}

/// 检查文件是否像是可执行文件
fn is_executable(path: &PathBuf) -> bool {
    path.is_file()
}
