use serde::Deserialize;
use config::{Config, File, Environment};
use std::path::PathBuf;

/// 应用配置总结构
#[derive(Debug, Deserialize, Clone)]
pub struct Settings {
    pub server: ServerSettings,
    pub database: DatabaseSettings,
    pub plugins: PluginSettings,
}

/// 服务相关配置（监听地址、端口、资源根目录）
#[derive(Debug, Deserialize, Clone)]
pub struct ServerSettings {
    pub host: String,
    pub port: u16,
    pub asset_root: String,
}

/// 数据库配置（SQLite 文件路径或连接字符串）
#[derive(Debug, Deserialize, Clone)]
pub struct DatabaseSettings {
    pub url: String,
}

/// 插件配置（WASM 插件文件位置）
#[derive(Debug, Deserialize, Clone)]
pub struct PluginSettings {
    pub location: PathBuf,
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
                "target/wasm32-wasip1/release/vtx_demo_plugin.wasm",
            )?

            // 配置文件（可选，文件名为 config.{toml/json/yaml}）
            .add_source(File::with_name("config").required(false))

            // 环境变量支持（如 VTX_SERVER__PORT=8080）
            .add_source(Environment::with_prefix("VTX").separator("__"));

        let config = builder.build()?;
        Ok(config.try_deserialize()?)
    }
}
