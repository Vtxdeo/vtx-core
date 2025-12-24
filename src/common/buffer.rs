use std::fs::File;
use std::io::Cursor;
use std::path::PathBuf;

/// 表示统一的缓冲区类型，支持多种数据来源
pub enum BufferType {
    /// 从本地文件系统读取的数据
    File(File),
    /// 内存中的字节缓冲区
    Memory(Cursor<Vec<u8>>),
}

/// 封装通用缓冲区及其元数据
/// 职责：持有底层数据源，并提供路径或 MIME 类型线索供协议层嗅探
pub struct RealBuffer {
    pub inner: BufferType,
    /// 路径线索：用于文件扩展名 MIME 嗅探
    pub path_hint: Option<PathBuf>,
    /// MIME 覆盖：插件可显式指定内容类型（如 "application/json"）
    pub mime_override: Option<String>,
}
