use axum::{
    body::{Body, Bytes},
    http::{header, HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use futures_util::Stream;
use std::io::{Read, Seek, SeekFrom};
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::fs::File as TokioFile;
use tokio::process::{Child, ChildStdout};
use tokio_util::io::ReaderStream;

use crate::common::buffer::{BufferType, RealBuffer};

/// 管道流包装器
///
/// 职责：包装来自子进程的 stdout 流，并持有子进程句柄 (Child)。
/// 只要这个 Stream 还在被 Axum 轮询（即客户端还在下载），Child 就不会被 Drop，进程保持存活。
/// 一旦流结束或连接断开，ProcessStream 被 Drop，Child 随之 Drop，触发 kill_on_drop 机制清理进程。
struct ProcessStream {
    stream: ReaderStream<ChildStdout>,
    // 即使不使用它，也必须持有它以维持进程生命周期
    #[allow(dead_code)]
    _child: Option<Child>,
}

impl Stream for ProcessStream {
    type Item = std::io::Result<Bytes>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.stream).poll_next(cx)
    }
}

/// HTTP 响应构建器：支持文件、内存缓冲区及子进程管道的统一流式传输
pub struct StreamProtocolLayer;

impl StreamProtocolLayer {
    /// 主入口，根据 `RealBuffer` 类型动态构建响应内容
    pub async fn process(buffer: RealBuffer, headers: &HeaderMap, status_code: u16) -> Response {
        let status = StatusCode::from_u16(status_code).unwrap_or(StatusCode::OK);

        let RealBuffer {
            inner,
            path_hint,
            mime_override,
            process_handle,
        } = buffer;

        match inner {
            BufferType::File(file) => Self::handle_file(file, path_hint, headers, status).await,
            BufferType::Memory(cursor) => {
                Self::handle_memory(cursor, mime_override, headers, status).await
            }
            BufferType::Pipe(stdout) => {
                Self::handle_pipe(stdout, process_handle, mime_override, status).await
            }
        }
    }

    /// 构建管道流响应（用于实时转码等场景）
    async fn handle_pipe(
        stdout: ChildStdout,
        child: Option<Child>,
        mime: Option<String>,
        status: StatusCode,
    ) -> Response {
        // 创建保活流
        let stream = ProcessStream {
            stream: ReaderStream::new(stdout),
            _child: child,
        };

        Response::builder()
            .status(status)
            // 管道流通常是实时生成的，禁用缓存
            .header(header::CACHE_CONTROL, "no-cache")
            .header(
                header::CONTENT_TYPE,
                mime.unwrap_or_else(|| "video/mp4".into()), // 默认假定为视频流
            )
            // 实时流无法预知长度，不发送 Content-Length，Axum 会自动处理为 chunked 传输
            .body(Body::from_stream(stream))
            .unwrap()
            .into_response()
    }

    /// 构建文件响应（支持断点续传 Range、ETag 缓存校验、MIME 推断等）
    async fn handle_file(
        mut file: std::fs::File,
        path: Option<std::path::PathBuf>,
        headers: &HeaderMap,
        default_status: StatusCode,
    ) -> Response {
        let metadata = match file.metadata() {
            Ok(m) => m,
            Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
        };
        let file_size = metadata.len();

        // 空文件
        if file_size == 0 {
            return Response::builder()
                .status(default_status)
                .header(header::CONTENT_LENGTH, "0")
                .body(Body::empty())
                .unwrap()
                .into_response();
        }

        // MIME 类型推断
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

        // ETag 构造
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

        // Range 解析
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
        if file.seek(SeekFrom::Start(start)).is_err() {
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }

        let content_length = end - start + 1;

        // 使用 tokio 异步流进行响应
        let tokio_file = TokioFile::from_std(file);
        let stream = ReaderStream::with_capacity(
            tokio::io::AsyncReadExt::take(tokio_file, content_length),
            64 * 1024,
        );

        // 如果存在 Range 头，强制使用 206；否则使用传入的 status（默认为 200）
        let final_status = if range_header.is_some() {
            StatusCode::PARTIAL_CONTENT
        } else {
            default_status
        };

        Response::builder()
            .status(final_status)
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

    /// 构建内存缓冲区响应
    async fn handle_memory(
        cursor: std::io::Cursor<Vec<u8>>,
        mime: Option<String>,
        _headers: &HeaderMap,
        status: StatusCode,
    ) -> Response {
        Response::builder()
            .status(status)
            .header(
                header::CONTENT_TYPE,
                mime.unwrap_or_else(|| "application/json".into()),
            )
            .body(Body::from(cursor.into_inner()))
            .unwrap()
            .into_response()
    }
}
