use axum::{
    body::Body,
    http::{Request, StatusCode},
    routing::{delete, get, post},
    Router,
};
use futures_util::StreamExt;
use http_body_util::BodyExt;
use serde_json::Value;
use std::sync::Arc;
use tempfile::tempdir;
use tokio::time::{timeout, Duration};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use tower::util::ServiceExt;
use uuid::Uuid;
use vtx_core::{
    common::events::{EventContext, VtxEvent},
    config::Settings,
    runtime::{
        bus::EventBus, context::StreamContext, ffmpeg::VtxFfmpegManager,
        manager::{PluginManager, PluginManagerConfig},
    },
    storage::VideoRegistry,
    web::{
        api::{admin, ws},
        state::AppState,
    },
};
use wasmtime::component::Linker;

async fn make_state() -> (Arc<AppState>, tempfile::TempDir) {
    let temp_dir = tempdir().expect("tempdir");
    let db_path = temp_dir.path().join("vtx.db");
    let registry = VideoRegistry::new(db_path.to_string_lossy().as_ref(), 1).expect("registry");

    let mut wasm_config = wasmtime::Config::new();
    wasm_config.wasm_component_model(true);
    wasm_config.async_support(true);
    let engine = wasmtime::Engine::new(&wasm_config).expect("engine");
    let linker = Linker::<StreamContext>::new(&engine);

    let vtx_ffmpeg = Arc::new(VtxFfmpegManager::new(
        temp_dir.path().join("ffmpeg"),
        30,
        false,
    ));
    let event_bus = Arc::new(EventBus::new(8));

    let plugin_manager = PluginManager::new(PluginManagerConfig {
        engine: engine.clone(),
        plugin_dir: temp_dir.path().join("plugins"),
        registry: registry.clone(),
        linker,
        auth_provider: None,
        vtx_ffmpeg: vtx_ffmpeg.clone(),
        max_buffer_read_bytes: 4 * 1024 * 1024,
        max_memory_bytes: 32 * 1024 * 1024,
        event_bus: event_bus.clone(),
    })
    .await
    .expect("plugin_manager");

    let config = Settings::new().expect("settings");
    let state = Arc::new(AppState {
        engine,
        plugin_manager,
        registry,
        config,
        vtx_ffmpeg,
        event_bus,
    });

    (state, temp_dir)
}

async fn read_json(response: axum::response::Response) -> (StatusCode, Value) {
    let status = response.status();
    let body = response
        .into_body()
        .collect()
        .await
        .expect("body")
        .to_bytes();
    let value: Value = serde_json::from_slice(&body).expect("json");
    (status, value)
}

#[tokio::test]
async fn health_route_returns_ok() {
    let app = Router::new().route("/health", get(|| async { "OK" }));
    let response = app
        .oneshot(
            Request::builder()
                .uri("/health")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("response");

    assert_eq!(response.status(), StatusCode::OK);
    let body = response
        .into_body()
        .collect()
        .await
        .expect("body")
        .to_bytes();
    assert_eq!(body.as_ref(), b"OK");
}

#[tokio::test]
async fn admin_scan_roots_flow() {
    let (state, _temp_dir) = make_state().await;

    let app = Router::new()
        .nest(
            "/admin",
            Router::new()
                .route("/scan-roots", get(admin::list_scan_roots_handler))
                .route("/scan-roots", post(admin::add_scan_root_handler))
                .route("/scan-roots", delete(admin::remove_scan_root_handler)),
        )
        .with_state(state);

    let root_dir = tempdir().expect("root");
    let body = serde_json::json!({ "path": root_dir.path() });

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/admin/scan-roots")
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .expect("response");

    let (status, payload) = read_json(response).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(payload["status"], "success");

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/admin/scan-roots")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("response");
    let (status, payload) = read_json(response).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(payload["count"], 1);

    let response = app
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/admin/scan-roots")
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .expect("response");
    let (status, payload) = read_json(response).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(payload["status"], "success");
}

#[tokio::test]
async fn admin_scan_directory_returns_count() {
    let (state, _temp_dir) = make_state().await;

    let app = Router::new()
        .nest(
            "/admin",
            Router::new().route("/scan", post(admin::scan_handler)),
        )
        .with_state(state.clone());

    let scan_root = tempdir().expect("scan_root");
    let video1 = scan_root.path().join("video1.mp4");
    let video2 = scan_root.path().join("video2.mkv");
    std::fs::write(&video1, "x").expect("write");
    std::fs::write(&video2, "x").expect("write");

    state
        .registry
        .add_scan_root(&scan_root.path().to_path_buf())
        .expect("add scan root");

    let body = serde_json::json!({ "path": scan_root.path() });
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/admin/scan")
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .expect("response");

    let (status, payload) = read_json(response).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(payload["scanned_count"], 2);
}

#[tokio::test]
async fn admin_list_plugins_empty() {
    let (state, _temp_dir) = make_state().await;

    let app = Router::new()
        .nest(
            "/admin",
            Router::new().route("/plugins", get(admin::list_plugins_handler)),
        )
        .with_state(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/admin/plugins")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("response");

    let (status, payload) = read_json(response).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(payload["count"], 0);
}

#[tokio::test]
async fn admin_jobs_flow() {
    let (state, _temp_dir) = make_state().await;

    let user = vtx_core::runtime::host_impl::api::auth_types::UserContext {
        user_id: "u1".to_string(),
        username: "tester".to_string(),
        groups: Vec::new(),
        metadata: "{}".to_string(),
    };

    let app = Router::new()
        .nest(
            "/admin",
            Router::new()
                .route("/jobs", post(admin::submit_job_handler))
                .route("/jobs", get(admin::list_jobs_handler))
                .route("/jobs/:id", get(admin::get_job_handler))
                .route("/jobs/:id/cancel", post(admin::cancel_job_handler))
                .layer(axum::Extension(user)),
        )
        .with_state(state);

    let submit_body = serde_json::json!({
        "job_type": "noop",
        "payload": {},
        "max_retries": 0,
        "payload_version": 1
    });

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/admin/jobs")
                .header("content-type", "application/json")
                .body(Body::from(submit_body.to_string()))
                .unwrap(),
        )
        .await
        .expect("response");

    let (status, payload) = read_json(response).await;
    assert_eq!(status, StatusCode::OK);
    let job_id = payload["data"]["job_id"]
        .as_str()
        .expect("job_id")
        .to_string();

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/admin/jobs?limit=10")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("response");
    let (status, payload) = read_json(response).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(payload["count"], 1);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/admin/jobs/{}", job_id))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("response");
    let (status, payload) = read_json(response).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(payload["data"]["id"], job_id);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/admin/jobs/{}/cancel", job_id))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("response");
    let (status, payload) = read_json(response).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(payload["data"]["job_id"], job_id);
}

#[tokio::test]
async fn admin_jobs_not_found_returns_error_code() {
    let (state, _temp_dir) = make_state().await;

    let user = vtx_core::runtime::host_impl::api::auth_types::UserContext {
        user_id: "u1".to_string(),
        username: "tester".to_string(),
        groups: Vec::new(),
        metadata: "{}".to_string(),
    };

    let app = Router::new()
        .nest(
            "/admin",
            Router::new()
                .route("/jobs/:id", get(admin::get_job_handler))
                .layer(axum::Extension(user)),
        )
        .with_state(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/admin/jobs/missing")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("response");

    let (status, payload) = read_json(response).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(payload["code"], "VTX-ADM-404");
}

#[tokio::test]
async fn admin_scan_rejects_when_roots_missing() {
    let (state, _temp_dir) = make_state().await;

    let app = Router::new()
        .nest(
            "/admin",
            Router::new().route("/scan", post(admin::scan_handler)),
        )
        .with_state(state);

    let scan_root = tempdir().expect("scan_root");
    let body = serde_json::json!({ "path": scan_root.path() });
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/admin/scan")
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .expect("response");

    let (status, payload) = read_json(response).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(payload["code"], "VTX-ADM-400");
}

#[tokio::test]
async fn admin_ws_events_accepts_connection() {
    let (state, _temp_dir) = make_state().await;
    let app = Router::new()
        .route("/admin/ws/events", get(ws::ws_handler))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");

    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve");
    });

    let url = format!("ws://{}/admin/ws/events?topics=*", addr);
    let (mut socket, _) = connect_async(url).await.expect("connect");
    socket.close(None).await.expect("close");

    server.abort();
}

#[tokio::test]
async fn admin_ws_events_delivers_published_event() {
    let (state, _temp_dir) = make_state().await;
    let event_bus = state.event_bus.clone();

    let app = Router::new()
        .route("/admin/ws/events", get(ws::ws_handler))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");

    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve");
    });

    let url = format!("ws://{}/admin/ws/events?topics=video.scan", addr);
    let (mut socket, _) = connect_async(url).await.expect("connect");

    let event = VtxEvent {
        id: Uuid::new_v4().to_string(),
        topic: "video.scan".to_string(),
        source: "test".to_string(),
        payload: serde_json::json!({ "count": 1 }),
        context: EventContext {
            user_id: None,
            username: None,
            request_id: None,
        },
        occurred_at: 0,
    };

    let delivered = event_bus.publish(event.clone()).await;
    assert_eq!(delivered, 1);

    let message = timeout(Duration::from_secs(2), socket.next())
        .await
        .expect("timeout")
        .expect("message")
        .expect("ws");

    match message {
        Message::Text(text) => {
            let payload: Value = serde_json::from_str(&text).expect("json");
            assert_eq!(payload["id"], event.id);
            assert_eq!(payload["topic"], "video.scan");
        }
        other => panic!("unexpected message: {:?}", other),
    }

    socket.close(None).await.expect("close");
    server.abort();
}

#[tokio::test]
async fn admin_scan_rejects_file_path() {
    let (state, _temp_dir) = make_state().await;

    let app = Router::new()
        .nest(
            "/admin",
            Router::new().route("/scan", post(admin::scan_handler)),
        )
        .with_state(state);

    let scan_root = tempdir().expect("scan_root");
    let file_path = scan_root.path().join("file.txt");
    std::fs::write(&file_path, "x").expect("write");

    let body = serde_json::json!({ "path": file_path });
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/admin/scan")
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .expect("response");

    let (status, payload) = read_json(response).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(payload["code"], "VTX-ADM-400");
}

#[tokio::test]
async fn admin_jobs_rejects_missing_group() {
    let (state, _temp_dir) = make_state().await;

    let user = vtx_core::runtime::host_impl::api::auth_types::UserContext {
        user_id: "u1".to_string(),
        username: "tester".to_string(),
        groups: vec!["user".to_string()],
        metadata: "{}".to_string(),
    };

    let app = Router::new()
        .nest(
            "/admin",
            Router::new()
                .route("/jobs", post(admin::submit_job_handler))
                .layer(axum::Extension(user)),
        )
        .with_state(state);

    let submit_body = serde_json::json!({
        "job_type": "scan-directory",
        "payload": { "path": "C:/media" },
        "max_retries": 0,
        "payload_version": 1
    });

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/admin/jobs")
                .header("content-type", "application/json")
                .body(Body::from(submit_body.to_string()))
                .unwrap(),
        )
        .await
        .expect("response");

    let (status, payload) = read_json(response).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(payload["code"], "VTX-ADM-400");
}

#[tokio::test]
async fn admin_cancel_missing_job_returns_not_found() {
    let (state, _temp_dir) = make_state().await;

    let app = Router::new()
        .nest(
            "/admin",
            Router::new().route("/jobs/:id/cancel", post(admin::cancel_job_handler)),
        )
        .with_state(state);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/admin/jobs/missing/cancel")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("response");

    let (status, payload) = read_json(response).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(payload["code"], "VTX-ADM-404");
}

#[tokio::test]
async fn admin_scan_rejects_path_outside_roots() {
    let (state, _temp_dir) = make_state().await;

    let app = Router::new()
        .nest(
            "/admin",
            Router::new().route("/scan", post(admin::scan_handler)),
        )
        .with_state(state.clone());

    let allowed_root = tempdir().expect("allowed_root");
    state
        .registry
        .add_scan_root(&allowed_root.path().to_path_buf())
        .expect("add scan root");

    let outside_root = tempdir().expect("outside_root");
    let body = serde_json::json!({ "path": outside_root.path() });
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/admin/scan")
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .expect("response");

    let (status, payload) = read_json(response).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(payload["code"], "VTX-ADM-400");
}
