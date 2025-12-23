use std::fs::File;
use std::io::Cursor;

/// 表示统一的缓冲区类型，用于支持多种数据来源（文件或内存）
///
/// - `File`：从本地文件系统读取的数据
/// - `Memory`：内存中的字节缓冲区（通常用于临时数据或内存回放）
pub enum BufferType {
    File(File),
    Memory(Cursor<Vec<u8>>),
}

/// 封装通用缓冲区类型的结构体
///
/// 使用统一接口处理底层数据源，支持文件与内存数据复用场景
pub struct RealBuffer {
    pub inner: BufferType,
}
