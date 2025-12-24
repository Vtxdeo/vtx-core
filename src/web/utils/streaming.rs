use axum::{
    body::Body,
    http::{header, HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use std::io::{Read, Seek, SeekFrom};
use tokio::fs::File as TokioFile;
use tokio_util::io::ReaderStream;

use crate::common::buffer::{RealBuffer, BufferType};

/// 流式协议处理层，支持文件与内存缓冲区的响应构建
pub struct StreamProtocolLayer;

impl StreamProtocolLayer {
    /// 主入口，根据资源类型构建 HTTP 响应
    pub async fn process(buffer: RealBuffer, headers: &HeaderMap) -> Response {
        match buffer.inner {
            BufferType::File(file) => {
                Self::handle_file(file, buffer.path_hint, headers).await
            }
            BufferType::Memory(cursor) => {
                Self::handle_memory(cursor, buffer.mime_override, headers).await
            }
        }
    }

    /// 处理文件资源（支持 Range 请求与 MIME 推断）
    async fn handle_file(
        mut file: std::fs::File,
        path: Option<std::path::PathBuf>,
        headers: &HeaderMap,
    ) -> Response {
        let metadata = match file.metadata() {
            Ok(m) => m,
            Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
        };
        let file_size = metadata.len();

        // MIME 类型推断（优先使用路径，其次使用文件魔数）
        let content_type = if let Some(p) = path {
            mime_guess::from_path(p).first_raw().unwrap_or("video/mp4")
        } else {
            let mut magic = [0u8; 4];
            let current_pos = file.stream_position().unwrap_or(0);
            let _ = file.seek(SeekFrom::Start(0));
            let _ = file.read_exact(&mut magic);
            let _ = file.seek(SeekFrom::Start(current_pos));
            match &magic {
                b"\x1A\x45\xDF\xA3" => "video/webm",
                _ => "video/mp4",
            }
        };

        // ETag 生成：基于文件大小和修改时间
        let mtime = metadata
            .modified()
            .ok()
            .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
            .map(|d| d.as_secs())
            .unwrap_or(0);
        let etag = format!(r#""{:x}-{:x}""#, file_size, mtime);

        // 浏览器端缓存校验
        if headers
            .get(header::IF_NONE_MATCH)
            .and_then(|v| v.to_str().ok())
            == Some(&etag)
        {
            return StatusCode::NOT_MODIFIED.into_response();
        }

        // 解析 Range 请求头
        let range_header = headers.get(header::RANGE).and_then(|v| v.to_str().ok());
        let (start, end) = match range_header {
            Some(range) if range.starts_with("bytes=") => {
                let parts: Vec<&str> = range["bytes=".len()..].split('-').collect();
                let s = parts.get(0).and_then(|s| s.parse::<u64>().ok()).unwrap_or(0);
                let e = parts.get(1).and_then(|s| s.parse::<u64>().ok()).unwrap_or(file_size - 1);
                (s, e.min(file_size - 1))
            }
            _ => (0, file_size - 1),
        };

        // 范围校验
        if start > end {
            return (
                StatusCode::RANGE_NOT_SATISFIABLE,
                [(header::CONTENT_RANGE, format!("bytes */{}", file_size))],
            )
                .into_response();
        }

        // 定位文件起始读取位置
        if let Err(_) = file.seek(SeekFrom::Start(start)) {
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }

        let content_length = end - start + 1;

        // 转换为 tokio 异步文件流
        let tokio_file = TokioFile::from_std(file);
        let stream = ReaderStream::with_capacity(
            tokio::io::AsyncReadExt::take(tokio_file, content_length),
            64 * 1024,
        );

        Response::builder()
            .status(if range_header.is_some() {
                StatusCode::PARTIAL_CONTENT
            } else {
                StatusCode::OK
            })
            .header(header::CONTENT_TYPE, content_type)
            .header(header::ETAG, etag)
            .header(header::ACCEPT_RANGES, "bytes")
            .header(header::CONTENT_LENGTH, content_length.to_string())
            .header(
                header::CONTENT_RANGE,
                format!("bytes {}-{}/{}", start, end, file_size),
            )
            .body(Body::from_stream(stream))
            .unwrap()
            .into_response()
    }

    /// 处理内存缓冲区（如 JSON 响应）
    async fn handle_memory(
        cursor: std::io::Cursor<Vec<u8>>,
        mime: Option<String>,
        _headers: &HeaderMap,
    ) -> Response {
        Response::builder()
            .header(
                header::CONTENT_TYPE,
                mime.unwrap_or_else(|| "application/json".into()),
            )
            .body(Body::from(cursor.into_inner()))
            .unwrap()
            .into_response()
    }
}
