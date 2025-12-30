use std::collections::HashMap;
use std::path::PathBuf;
use tracing::{error, info, warn};

/// VtxFfmpeg 工具链管理器
///
/// 职责：扫描指定目录，建立 "Profile 名称 -> 二进制路径" 的映射关系。
/// 该管理器作为中间层，屏蔽了底层二进制文件的具体位置，允许插件通过抽象的 Profile 名称请求转码能力。
pub struct VtxFfmpegManager {
    /// Profile 映射表
    /// Key: Profile 名称 (例如 "mini", "remux")
    /// Value: 二进制文件的绝对路径
    profiles: HashMap<String, PathBuf>,
    /// 二进制文件所在的根目录
    binary_root: PathBuf,
    /// 新增：子进程执行超时时间（秒）
    /// 这是一个只读配置，供 Host Function 在启动进程时使用
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

    /// 扫描二进制目录并更新 Profile 索引
    fn scan(&mut self) {
        info!("[VtxFfmpeg] Scanning binaries in: {:?}", self.binary_root);

        if !self.binary_root.exists() {
            warn!(
                "[VtxFfmpeg] Binary root directory '{:?}' does not exist. No profiles will be loaded.",
                self.binary_root
            );
            return;
        }

        let entries = match std::fs::read_dir(&self.binary_root) {
            Ok(e) => e,
            Err(e) => {
                error!("[VtxFfmpeg] Failed to read directory: {}", e);
                return;
            }
        };

        let mut count = 0;
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_file() {
                if let Some(file_name) = path.file_name().and_then(|n| n.to_str()) {
                    // 强制检查前缀，确保只加载官方构建的 vtx-ffmpeg 工具
                    if file_name.starts_with("vtx-ffmpeg-") {
                        let profile_raw = &file_name["vtx-ffmpeg-".len()..];
                        // 兼容 Windows .exe 后缀，提取纯净的 Profile 名
                        let profile = profile_raw.split('.').next().unwrap_or(profile_raw);

                        // 转换为绝对路径以确保执行稳定性
                        if let Ok(abs_path) = std::fs::canonicalize(&path) {
                            self.profiles.insert(profile.to_string(), abs_path);
                            count += 1;
                        }
                    }
                }
            }
        }

        if count == 0 {
            warn!("[VtxFfmpeg] No valid 'vtx-ffmpeg-*' binaries found.");
        } else {
            info!(
                "[VtxFfmpeg] Initialization complete. Loaded {} profiles: {:?}",
                count,
                self.profiles.keys()
            );
        }
    }

    /// 根据 Profile 名称获取对应的二进制文件路径
    pub fn get_binary(&self, profile: &str) -> Option<&PathBuf> {
        self.profiles.get(profile)
    }
}
