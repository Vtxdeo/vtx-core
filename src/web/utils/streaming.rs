use axum::{
    body::{Body, Bytes},
    http::{header, HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use futures_util::Stream;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::process::{Child, ChildStdout};
use tokio_util::io::ReaderStream;
use url::Url;

use crate::common::buffer::{BufferType, RealBuffer};
use crate::vtx_vfs::VtxVfsManager;

struct ProcessStream {
    stream: ReaderStream<ChildStdout>,
    #[allow(dead_code)]
    _child: Option<Child>,
}

impl Stream for ProcessStream {
    type Item = std::io::Result<Bytes>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.stream).poll_next(cx)
    }
}

pub struct StreamProtocolLayer;

impl StreamProtocolLayer {
    pub async fn process(
        buffer: RealBuffer,
        headers: &HeaderMap,
        status_code: u16,
        vfs: Arc<VtxVfsManager>,
    ) -> Response {
        let status = StatusCode::from_u16(status_code).unwrap_or(StatusCode::OK);

        let RealBuffer {
            inner,
            uri_hint,
            mime_override,
            process_handle,
        } = buffer;

        match inner {
            BufferType::Object { uri } => {
                Self::handle_object(uri, uri_hint, headers, status, vfs).await
            }
            BufferType::Memory(cursor) => {
                Self::handle_memory(cursor, mime_override, headers, status).await
            }
            BufferType::Pipe(stdout) => {
                Self::handle_pipe(stdout, process_handle, mime_override, status).await
            }
        }
    }

    async fn handle_pipe(
        stdout: ChildStdout,
        child: Option<Child>,
        mime: Option<String>,
        status: StatusCode,
    ) -> Response {
        let stream = ProcessStream {
            stream: ReaderStream::new(stdout),
            _child: child,
        };

        Response::builder()
            .status(status)
            .header(header::CACHE_CONTROL, "no-cache")
            .header(
                header::CONTENT_TYPE,
                mime.unwrap_or_else(|| "video/mp4".into()),
            )
            .body(Body::from_stream(stream))
            .unwrap()
            .into_response()
    }

    async fn handle_object(
        uri: String,
        uri_hint: Option<String>,
        headers: &HeaderMap,
        default_status: StatusCode,
        vfs: Arc<VtxVfsManager>,
    ) -> Response {
        let meta = match vfs.head(&uri).await {
            Ok(value) => value,
            Err(_) => return StatusCode::NOT_FOUND.into_response(),
        };
        let file_size = meta.size;

        if file_size == 0 {
            return Response::builder()
                .status(default_status)
                .header(header::CONTENT_LENGTH, "0")
                .body(Body::empty())
                .unwrap()
                .into_response();
        }

        let content_type = if let Some(hint) = uri_hint.as_ref() {
            let path = extract_path_for_mime(hint);
            mime_guess::from_path(&path)
                .first_raw()
                .unwrap_or("video/mp4")
        } else {
            "video/mp4"
        };

        let mtime = meta.last_modified.unwrap_or(0);
        let etag = if let Some(etag) = meta.etag.as_ref() {
            format!(r#""{}""#, etag)
        } else {
            format!(r#""{:x}-{:x}""#, file_size, mtime)
        };

        if headers
            .get(header::IF_NONE_MATCH)
            .and_then(|v| v.to_str().ok())
            == Some(&etag)
        {
            return StatusCode::NOT_MODIFIED.into_response();
        }

        let range_header = headers.get(header::RANGE).and_then(|v| v.to_str().ok());
        let (start, end) = match range_header {
            Some(range) if range.starts_with("bytes=") => {
                let range_val = &range["bytes=".len()..];
                if let Some((s_str, e_str)) = range_val.split_once('-') {
                    if s_str.is_empty() {
                        let suffix_len = e_str.parse::<u64>().unwrap_or(0);
                        let start = file_size.saturating_sub(suffix_len);
                        (start, file_size - 1)
                    } else if e_str.is_empty() {
                        let s = s_str.parse::<u64>().unwrap_or(0);
                        (s, file_size - 1)
                    } else {
                        let s = s_str.parse::<u64>().unwrap_or(0);
                        let e = e_str.parse::<u64>().unwrap_or(file_size - 1);
                        (s, e.min(file_size - 1))
                    }
                } else {
                    (0, file_size - 1)
                }
            }
            _ => (0, file_size - 1),
        };

        if start > end || start >= file_size {
            return (
                StatusCode::RANGE_NOT_SATISFIABLE,
                [(header::CONTENT_RANGE, format!("bytes */{}", file_size))],
            )
                .into_response();
        }

        let content_length = end - start + 1;
        let range = if range_header.is_some() {
            Some(start..=end)
        } else {
            None
        };

        let stream = match vfs.get_stream(&uri, range).await {
            Ok(value) => value,
            Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
        };

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

fn extract_path_for_mime(uri: &str) -> String {
    if let Ok(url) = Url::parse(uri) {
        return url.path().to_string();
    }
    uri.to_string()
}
