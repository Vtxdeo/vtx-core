use axum::{
    body::Body,
    http::{header, HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use std::fs::File;
use std::io::{Seek, SeekFrom};
use tokio::fs::File as TokioFile;
use tokio_util::io::ReaderStream;

/// 视频流响应构造工具
pub struct VideoStreamer;

impl VideoStreamer {
    /// 根据 Range 请求头构造视频流响应
    ///
    /// 逻辑：
    /// 1. 解析 Range 头（bytes=start-end）。
    /// 2. Seek 文件指针到 start 位置。
    /// 3. 使用 `ReaderStream` 包装文件进行异步传输。
    ///
    /// 复杂度：O(1) - 仅操作文件指针。
    pub async fn stream(mut file: File, headers: &HeaderMap) -> Response {
        let file_size = file.metadata().map(|m| m.len()).unwrap_or(0);
        let range_header = headers.get(header::RANGE).and_then(|v| v.to_str().ok());

        let (start, end) = match range_header {
            Some(range) if range.starts_with("bytes=") => {
                let parts: Vec<&str> = range["bytes=".len()..].split('-').collect();
                let start = parts.get(0).and_then(|s| s.parse().ok()).unwrap_or(0);
                let end = parts
                    .get(1)
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(file_size - 1);
                (start, end)
            }
            _ => (0, file_size - 1),
        };

        if let Err(e) = file.seek(SeekFrom::Start(start)) {
            tracing::error!("[Stream] File seek failed: {}", e);
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }

        let content_length = end - start + 1;
        let tokio_file = TokioFile::from_std(file);

        // 64KB 缓冲区
        let stream = ReaderStream::with_capacity(
            tokio::io::AsyncReadExt::take(tokio_file, content_length),
            64 * 1024,
        );

        let status = if range_header.is_some() {
            StatusCode::PARTIAL_CONTENT
        } else {
            StatusCode::OK
        };

        Response::builder()
            .status(status)
            .header("Content-Type", "video/mp4")
            .header("Accept-Ranges", "bytes")
            .header("Content-Length", content_length.to_string())
            .header(
                "Content-Range",
                format!("bytes {}-{}/{}", start, end, file_size),
            )
            .body(Body::from_stream(stream))
            .unwrap()
            .into_response()
    }
}
