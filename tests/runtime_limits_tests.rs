use axum::extract::State;
use axum::http::StatusCode;
use axum::response::Redirect;
use axum::routing::get;
use axum::Router;
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tempfile::tempdir;
use tokio::net::TcpListener;
use tokio::time::{timeout, Duration};
use url::Url;
use wasmtime::StoreLimitsBuilder;

use vtx_core::common::events::{EventContext, VtxEvent};
use vtx_core::runtime::bus::EventBus;
use vtx_core::runtime::context::{SecurityPolicy, StreamContext, StreamContextConfig};
use vtx_core::runtime::ffmpeg::VtxFfmpegManager;
use vtx_core::runtime::vtx_host_impl::api;
use vtx_core::runtime::vtx_host_impl::api::vtx_http_client::Host as HttpHost;
use vtx_core::runtime::vtx_host_impl::api::vtx_vfs::Host as VfsHost;
use vtx_core::storage::VtxVideoRegistry;
use vtx_core::vtx_vfs::VtxVfsManager;

fn write_ffmpeg_stub(dir: &Path) -> PathBuf {
    let (name, contents) = if cfg!(windows) {
        ("ffmpeg.cmd", "@echo off\r\necho ffmpeg version 1.0\r\n")
    } else {
        (
            "ffmpeg",
            "#!/bin/sh\nprintf 'ffmpeg version 1.0\\n'\nexit 0\n",
        )
    };
    let path = dir.join(name);
    std::fs::write(&path, contents).expect("ffmpeg stub");

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = std::fs::metadata(&path)
            .expect("ffmpeg metadata")
            .permissions();
        perms.set_mode(0o755);
        std::fs::set_permissions(&path, perms).expect("ffmpeg perms");
    }

    path
}

fn make_context(
    max_buffer_read_bytes: u64,
    http_allowlist: Vec<api::vtx_types::HttpAllowRule>,
) -> (tempfile::TempDir, StreamContext) {
    let temp_dir = tempdir().expect("tempdir");
    let db_path = temp_dir.path().join("vtx.db");
    let registry = VtxVideoRegistry::new(db_path.to_string_lossy().as_ref(), 1).expect("registry");

    let ffmpeg_path = write_ffmpeg_stub(temp_dir.path());
    std::env::set_var("VTX_FFMPEG_BIN", &ffmpeg_path);

    let vtx_ffmpeg = Arc::new(VtxFfmpegManager::new(30).expect("vtx_ffmpeg"));
    let vfs = Arc::new(VtxVfsManager::new().expect("vfs"));
    let event_bus = Arc::new(EventBus::new(8));
    let limits = StoreLimitsBuilder::new()
        .instances(1)
        .memory_size(8 * 1024 * 1024)
        .build();

    let ctx = StreamContext::new_secure(StreamContextConfig {
        registry,
        vtx_ffmpeg,
        limiter: limits,
        policy: SecurityPolicy::Root,
        plugin_id: None,
        max_buffer_read_bytes,
        current_user: None,
        event_bus,
        permissions: HashSet::new(),
        http_allowlist,
        vfs,
    });

    (temp_dir, ctx)
}

fn http_rule(
    host: &str,
    port: u16,
    path: &str,
    follow_redirects: Option<bool>,
    redirect_policy: Option<&str>,
    max_response_bytes: Option<u64>,
) -> api::vtx_types::HttpAllowRule {
    api::vtx_types::HttpAllowRule {
        scheme: "http".to_string(),
        host: host.to_string(),
        port: Some(port),
        path: Some(path.to_string()),
        methods: Some(vec!["GET".to_string()]),
        allow_headers: None,
        follow_redirects,
        redirect_policy: redirect_policy.map(|value| value.to_string()),
        max_request_bytes: None,
        max_response_bytes,
    }
}

fn build_event(topic: &str) -> VtxEvent {
    VtxEvent {
        id: "evt-1".to_string(),
        topic: topic.to_string(),
        source: "test".to_string(),
        payload: serde_json::json!({ "ok": true }),
        context: EventContext {
            user_id: None,
            username: None,
            request_id: None,
        },
        occurred_at: 0,
    }
}

#[tokio::test]
async fn hot_reload_event_switches_subscriptions() {
    let bus = EventBus::new(8);
    let mut rx_a = bus
        .register_plugin(
            "plugin",
            &[String::from("topic.a")],
            &[String::from("topic.a")],
        )
        .await;

    assert_eq!(bus.publish(build_event("topic.a")).await, 1);
    let received = timeout(Duration::from_millis(200), rx_a.recv())
        .await
        .expect("event timeout")
        .expect("event");
    assert_eq!(received.topic, "topic.a");

    let mut rx_b = bus
        .register_plugin(
            "plugin",
            &[String::from("topic.b")],
            &[String::from("topic.b")],
        )
        .await;

    assert_eq!(bus.publish(build_event("topic.a")).await, 0);
    assert_eq!(bus.publish(build_event("topic.b")).await, 1);

    let received = timeout(Duration::from_millis(200), rx_b.recv())
        .await
        .expect("event timeout")
        .expect("event");
    assert_eq!(received.topic, "topic.b");
}

#[tokio::test]
async fn vfs_read_range_respects_limit() {
    let (temp_dir, mut ctx) = make_context(4, Vec::new());
    let file_path = temp_dir.path().join("payload.bin");
    std::fs::write(&file_path, b"0123456789").expect("write");
    let uri = Url::from_file_path(&file_path)
        .expect("file uri")
        .to_string();

    let bytes = ctx.read_range(uri, 0, 10).await.expect("read_range");
    assert_eq!(bytes, b"0123");
}

#[derive(Clone)]
struct RedirectState {
    base_url: String,
}

#[tokio::test]
async fn http_response_limit_rejects_large_body() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    let app = Router::new().route("/big", get(|| async { (StatusCode::OK, "0123456789") }));

    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve");
    });

    let rule = http_rule("127.0.0.1", addr.port(), "/big", None, None, Some(4));
    let (_temp, mut ctx) = make_context(0, vec![rule]);

    let request = api::vtx_types::HttpClientRequest {
        url: format!("http://{}/big", addr),
        method: "GET".to_string(),
        headers: Vec::new(),
        body: None,
    };

    let err = ctx.request(request).await.expect_err("response limit");
    assert_eq!(err, "Response body exceeded max-response-bytes");

    server.abort();
}

#[tokio::test]
async fn http_redirect_allowlist_allows_matching_target() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    let base_url = format!("http://{}", addr);
    let state = RedirectState {
        base_url: base_url.clone(),
    };

    let app = Router::new()
        .route(
            "/start-allowed",
            get(|State(state): State<RedirectState>| async move {
                Redirect::temporary(&format!("{}/allowed", state.base_url))
            }),
        )
        .route(
            "/allowed",
            get(|| async { ([("x-redirected", "yes")], "") }),
        )
        .with_state(state);

    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve");
    });

    let start_rule = http_rule(
        "127.0.0.1",
        addr.port(),
        "/start-allowed",
        Some(true),
        Some("allowlist"),
        None,
    );
    let allowed_rule = http_rule("127.0.0.1", addr.port(), "/allowed", None, None, None);
    let (_temp, mut ctx) = make_context(0, vec![start_rule, allowed_rule]);

    let request = api::vtx_types::HttpClientRequest {
        url: format!("{}/start-allowed", base_url),
        method: "GET".to_string(),
        headers: Vec::new(),
        body: None,
    };

    let response = ctx.request(request).await.expect("redirect");
    assert_eq!(response.status, 200);
    let redirected = response
        .headers
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case("x-redirected"))
        .map(|(_, value)| value.as_str());
    assert_eq!(redirected, Some("yes"));

    server.abort();
}

#[tokio::test]
async fn http_redirect_allowlist_blocks_unmatched_target() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    let base_url = format!("http://{}", addr);
    let state = RedirectState {
        base_url: base_url.clone(),
    };

    let app = Router::new()
        .route(
            "/start-blocked",
            get(|State(state): State<RedirectState>| async move {
                Redirect::temporary(&format!("{}/blocked", state.base_url))
            }),
        )
        .route("/blocked", get(|| async { (StatusCode::OK, "blocked") }))
        .with_state(state);

    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve");
    });

    let start_rule = http_rule(
        "127.0.0.1",
        addr.port(),
        "/start-blocked",
        Some(true),
        Some("allowlist"),
        None,
    );
    let (_temp, mut ctx) = make_context(0, vec![start_rule]);

    let request = api::vtx_types::HttpClientRequest {
        url: format!("{}/start-blocked", base_url),
        method: "GET".to_string(),
        headers: Vec::new(),
        body: None,
    };

    let response = ctx.request(request).await.expect("redirect");
    assert_eq!(response.status, 307);
    let redirected = response
        .headers
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case("x-redirected"))
        .map(|(_, value)| value.as_str());
    assert_eq!(redirected, None);

    server.abort();
}
