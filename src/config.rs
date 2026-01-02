use config::{Config, Environment, File};
use serde::Deserialize;
use std::path::PathBuf;

/// 应用配置总结构
#[derive(Debug, Deserialize, Clone)]
pub struct Settings {
    pub server: ServerSettings,
    pub database: DatabaseSettings,
    pub plugins: PluginSettings,
    pub vtx_ffmpeg: VtxFfmpegSettings,
    pub job_queue: JobQueueSettings,
}

/// 服务相关配置（监听地址、端口、资源根目录）
#[derive(Debug, Deserialize, Clone)]
pub struct ServerSettings {
    pub host: String,
    pub port: u16,
    #[allow(dead_code)]
    pub asset_root: String,
}

/// 数据库配置（SQLite 文件路径或连接字符串）
#[derive(Debug, Deserialize, Clone)]
pub struct DatabaseSettings {
    pub url: String,
}

/// 插件配置（WASM 插件文件位置及运行时限制）
#[derive(Debug, Deserialize, Clone)]
pub struct PluginSettings {
    pub location: PathBuf,
    /// 单个插件实例允许使用的最大内存（单位：MB），默认 100MB
    pub max_memory_mb: u64,
    /// 单次 buffer 读取的最大大小（单位：MB），默认 16MB
    pub max_buffer_read_mb: u64,
    /// 指定用于鉴权的插件 ID
    /// 若设置，系统将直接调用该插件进行鉴权，不再遍历所有插件
    pub auth_provider: Option<String>,
}


/// VtxFfmpeg 中间层专用配置
///
/// 职责：定义媒体处理工具链的路径与运行时约束
#[derive(Debug, Deserialize, Clone)]
pub struct VtxFfmpegSettings {
    /// vtx-ffmpeg 二进制工具链的根目录
    /// 系统将自动扫描该目录下符合 vtx-ffmpeg-{profile} 命名规范的可执行文件
    pub binary_root: PathBuf,
    /// 子进程执行超时时间（单位：秒）
    /// 用于防止异常进程僵死占用系统资源，0 表示不限制（不推荐）
    pub execution_timeout_secs: u64,
    /// 是否允许回退到系统已安装的 FFmpeg
    ///
    /// 若启用且无法在 binary_root 找到合适的 Profile，
    /// 将尝试调用环境变量 PATH 中的 `ffmpeg` 命令作为兜底方案。
    #[serde(default)]
    pub use_system_binary: bool,
}

/// å¼‚æ­¥ä»»åŠ¡é˜Ÿåˆ—é…ç½®
#[derive(Debug, Deserialize, Clone)]
pub struct JobQueueSettings {
    /// æ‰«æé˜Ÿåˆ—çš„è½®è¯¢é—´éš”ï¼ˆmsï¼‰
    pub poll_interval_ms: u64,
    /// æœ€å¤§å¹¶å‘ worker æ•°
    pub max_concurrent: u32,
}

impl Settings {
    /// 加载配置：支持默认值、可选配置文件、环境变量覆盖
    pub fn new() -> anyhow::Result<Self> {
        let builder = Config::builder()
            // 默认值（代码内硬编码）
            .set_default("server.host", "0.0.0.0")?
            .set_default("server.port", 3000)?
            .set_default("server.asset_root", "./assets")?
            .set_default("database.url", "vtxdeo.db")?
            .set_default(
                "plugins.location",
                "target/wasm32-wasip1/release/vtx_plugin_auth_basic.vtx",
            )?
            // 默认限制 100MB 内存
            .set_default("plugins.max_memory_mb", 100)?
            // 默认限制单次读取 16MB
            .set_default("plugins.max_buffer_read_mb", 16)?
            // 默认不指定鉴权提供者
            .set_default::<&str, Option<String>>("plugins.auth_provider", None)?
            // VtxFfmpeg 默认配置
            .set_default("vtx_ffmpeg.binary_root", "./bin/ffmpeg")?
            .set_default("vtx_ffmpeg.execution_timeout_secs", 600)?
            // 默认关闭系统级回退，保证行为可预测
            .set_default("vtx_ffmpeg.use_system_binary", false)?
            // Job Queue é»˜è®¤é…ç½®
            .set_default("job_queue.poll_interval_ms", 1500)?
            .set_default("job_queue.max_concurrent", 1)?
            // 配置文件（可选，文件名为 config.{toml/json/yaml}）
            .add_source(File::with_name("config").required(false))
            // 环境变量支持（如 VTX_SERVER__PORT=8080）
            .add_source(Environment::with_prefix("VTX").separator("__"));

        let config = builder.build()?;
        Ok(config.try_deserialize()?)
    }
}
