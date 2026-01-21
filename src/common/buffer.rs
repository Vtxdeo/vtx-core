use std::io::Cursor;
use tokio::process::{Child, ChildStdout};

/// Represents a unified buffer type that supports various data sources.
pub enum BufferType {
    /// Data read from the local file system.
    Object { uri: String },
    /// An in-memory byte buffer.
    Memory(Cursor<Vec<u8>>),
    /// An asynchronous pipe stream (derived from a vtx-ffmpeg child process).
    Pipe(ChildStdout),
}

/// Encapsulates a generic buffer along with its metadata.
/// Responsibilities: Holds the underlying data source and provides path or MIME type hints
/// for protocol layer sniffing.
pub struct RealBuffer {
    pub inner: BufferType,
    /// Path hint: Used for MIME sniffing based on file extensions.
    pub uri_hint: Option<String>,
    /// MIME override: Allows plugins to explicitly specify the content type (e.g., "application/json").
    pub mime_override: Option<String>,
    /// Process handle: Retained to ensure the process remains active.
    /// When RealBuffer is destroyed, the Child is dropped, implicitly triggering a kill signal to clean up the process.
    pub process_handle: Option<Child>,
}
