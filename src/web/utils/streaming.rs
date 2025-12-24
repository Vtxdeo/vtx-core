use axum::{
    body::Body,
    http::{header, HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use std::io::{Read, Seek, SeekFrom};
use tokio::fs::File as TokioFile;
use tokio_util::io::ReaderStream;

use crate::common::buffer::{BufferType, RealBuffer};

/// HTTP 响应构建器：支持文件与内存缓冲区的异步流式传输
pub struct StreamProtocolLayer;

impl StreamProtocolLayer {
    /// 主入口，根据 `RealBuffer` 类型动态构建响应内容
    pub async fn process(buffer: RealBuffer, headers: &HeaderMap) -> Response {
        match buffer.inner {
            BufferType::File(file) => Self::handle_file(file, buffer.path_hint, headers).await,
            BufferType::Memory(cursor) => {
                Self::handle_memory(cursor, buffer.mime_override, headers).await
            }
        }
    }

    /// 构建文件响应（支持断点续传 Range、ETag 缓存校验、MIME 推断等）
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

        // 空文件：直接返回 200 OK 响应，不使用 Range
        if file_size == 0 {
            return Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_LENGTH, "0")
                .body(Body::empty())
                .unwrap()
                .into_response();
        }

        // MIME 类型推断：优先使用文件路径，其次基于文件头魔数进行识别
        let content_type = if let Some(p) = path {
            mime_guess::from_path(p).first_raw().unwrap_or("video/mp4")
        } else {
            // 读取前 4 字节用于判断格式
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

        // ETag 构造（用于缓存命中）：基于文件大小 + 最后修改时间
        let mtime = metadata
            .modified()
            .ok()
            .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
            .map(|d| d.as_secs())
            .unwrap_or(0);
        let etag = format!(r#""{:x}-{:x}""#, file_size, mtime);

        // ETag 命中缓存，直接返回 304 Not Modified
        if headers
            .get(header::IF_NONE_MATCH)
            .and_then(|v| v.to_str().ok())
            == Some(&etag)
        {
            return StatusCode::NOT_MODIFIED.into_response();
        }

        // Range 解析（支持标准/后缀/开放范围）
        let range_header = headers.get(header::RANGE).and_then(|v| v.to_str().ok());
        let (start, end) = match range_header {
            Some(range) if range.starts_with("bytes=") => {
                let range_val = &range["bytes=".len()..];
                if let Some((s_str, e_str)) = range_val.split_once('-') {
                    if s_str.is_empty() {
                        // Case A: bytes=-500（取最后 500 字节）
                        let suffix_len = e_str.parse::<u64>().unwrap_or(0);
                        let start = file_size.saturating_sub(suffix_len);
                        (start, file_size - 1)
                    } else if e_str.is_empty() {
                        // Case B: bytes=100-（从 100 到末尾）
                        let s = s_str.parse::<u64>().unwrap_or(0);
                        (s, file_size - 1)
                    } else {
                        // Case C: bytes=100-200（常规范围）
                        let s = s_str.parse::<u64>().unwrap_or(0);
                        let e = e_str.parse::<u64>().unwrap_or(file_size - 1);
                        (s, e.min(file_size - 1))
                    }
                } else {
                    // 无效格式，回退至全文传输
                    (0, file_size - 1)
                }
            }
            _ => (0, file_size - 1),
        };

        // 范围合法性检查
        if start > end || start >= file_size {
            return (
                StatusCode::RANGE_NOT_SATISFIABLE,
                [(header::CONTENT_RANGE, format!("bytes */{}", file_size))],
            )
                .into_response();
        }

        // 定位至 Range 起始位置
        if let Err(_) = file.seek(SeekFrom::Start(start)) {
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }

        let content_length = end - start + 1;

        // 使用 tokio 异步流进行响应
        let tokio_file = TokioFile::from_std(file);
        let stream = ReaderStream::with_capacity(
            tokio::io::AsyncReadExt::take(tokio_file, content_length),
            64 * 1024, // 64KB 传输缓冲区
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

    /// 构建内存缓冲区响应（如 JSON、文本、HTML 等）
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
