include!(concat!(env!("OUT_DIR"), "/host_bindings.rs"));

pub use vtx::api;

pub mod vtx_context;
pub mod vtx_event_bus;
pub mod vtx_ffmpeg;
pub mod vtx_ffmpeg_policy;
pub mod vtx_http_client;
pub mod vtx_ipc_transport;
pub mod vtx_sql;
pub mod vtx_sql_policy;
pub mod vtx_vfs;
