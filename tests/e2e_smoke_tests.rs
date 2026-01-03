use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command, Stdio};
use std::sync::{Mutex, OnceLock};
use std::thread;
use std::time::{Duration, Instant};
use tempfile::tempdir;

fn pick_free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .expect("bind")
        .local_addr()
        .expect("addr")
        .port()
}

fn wait_for_health_with_child(
    child: &mut Child,
    host: &str,
    port: u16,
    timeout: Duration,
) -> Result<(), String> {
    let deadline = Instant::now() + timeout;
    let addr = format!("{}:{}", host, port);
    while Instant::now() < deadline {
        if TcpStream::connect(&addr).is_ok() {
            return Ok(());
        }
        if let Ok(Some(status)) = child.try_wait() {
            let mut stderr = String::new();
            let mut stdout = String::new();
            if let Some(mut stderr_pipe) = child.stderr.take() {
                let _ = stderr_pipe.read_to_string(&mut stderr);
            }
            if let Some(mut stdout_pipe) = child.stdout.take() {
                let _ = stdout_pipe.read_to_string(&mut stdout);
            }
            return Err(format!(
                "server exited early ({}): stdout='{}' stderr='{}'",
                status,
                stdout.trim(),
                stderr.trim()
            ));
        }
        thread::sleep(Duration::from_millis(50));
    }
    let _ = child.kill();
    let _ = child.wait();
    let mut stderr = String::new();
    let mut stdout = String::new();
    if let Some(mut stderr_pipe) = child.stderr.take() {
        let _ = stderr_pipe.read_to_string(&mut stderr);
    }
    if let Some(mut stdout_pipe) = child.stdout.take() {
        let _ = stdout_pipe.read_to_string(&mut stdout);
    }
    Err(format!(
        "server not ready: stdout='{}' stderr='{}'",
        stdout.trim(),
        stderr.trim()
    ))
}

fn http_get(host: &str, port: u16, path: &str) -> String {
    let mut stream = TcpStream::connect(format!("{}:{}", host, port)).expect("connect");
    let request = format!(
        "GET {} HTTP/1.1\r\nHost: {}:{}\r\nConnection: close\r\n\r\n",
        path, host, port
    );
    stream.write_all(request.as_bytes()).expect("write");
    stream.flush().expect("flush");

    let mut response = String::new();
    stream.read_to_string(&mut response).expect("read");
    response
}

fn parse_response(response: &str) -> (u16, String) {
    let mut lines = response.lines();
    let status_line = lines.next().unwrap_or_default();
    let status_code = status_line
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse::<u16>().ok())
        .unwrap_or(0);

    let body = response.split("\r\n\r\n").nth(1).unwrap_or("").to_string();

    (status_code, body)
}

struct ChildGuard {
    child: Child,
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn spawn_server() -> (ChildGuard, String, u16, tempfile::TempDir) {
    let port = pick_free_port();
    let host = "127.0.0.1";
    let temp_dir = tempdir().expect("tempdir");
    let plugin_dir = temp_dir.path().join("plugins");
    let ffmpeg_dir = temp_dir.path().join("ffmpeg");
    let db_path = temp_dir.path().join("vtxdeo.db");
    let config_path = temp_dir.path().join("config.toml");
    let normalize = |path: &std::path::Path| path.to_string_lossy().replace('\\', "/");
    let config_contents = format!(
        "server.host = \"{host}\"\nserver.port = {port}\n\
database.url = \"{db}\"\n\
plugins.location = \"{plugins}\"\n\
vtx_ffmpeg.binary_root = \"{ffmpeg}\"\n",
        host = host,
        port = port,
        db = normalize(&db_path),
        plugins = normalize(&plugin_dir),
        ffmpeg = normalize(&ffmpeg_dir)
    );
    std::fs::write(&config_path, config_contents).expect("write config");

    let mut child = Command::new(env!("CARGO_BIN_EXE_vtx-core"))
        .current_dir(temp_dir.path())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn");

    if let Err(message) =
        wait_for_health_with_child(&mut child, host, port, Duration::from_secs(30))
    {
        let guard = ChildGuard { child };
        drop(guard);
        panic!("{}", message);
    }

    (ChildGuard { child }, host.to_string(), port, temp_dir)
}

fn e2e_lock() -> std::sync::MutexGuard<'static, ()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(|e| e.into_inner())
}

#[test]
fn e2e_health_smoke() {
    let _lock = e2e_lock();
    let (_guard, host, port, _temp_dir) = spawn_server();

    let response = http_get(&host, port, "/health");
    let (status, body) = parse_response(&response);
    assert_eq!(status, 200);
    assert_eq!(body, "OK");
}

#[test]
fn e2e_admin_requires_auth() {
    let _lock = e2e_lock();
    let (_guard, host, port, _temp_dir) = spawn_server();

    let response = http_get(&host, port, "/admin/scan-roots");
    let (status, _body) = parse_response(&response);
    assert_eq!(status, 401);
}

#[test]
fn e2e_gateway_plugin_not_found() {
    let _lock = e2e_lock();
    let (_guard, host, port, _temp_dir) = spawn_server();

    let response = http_get(&host, port, "/no-such-route");
    let (status, body) = parse_response(&response);
    assert_eq!(status, 404);
    assert!(body.contains("\"code\":\"VTX-PLG-404\""));
}

#[test]
fn e2e_admin_ws_requires_auth() {
    let _lock = e2e_lock();
    let (_guard, host, port, _temp_dir) = spawn_server();

    let response = http_get(&host, port, "/admin/ws/events");
    let (status, _body) = parse_response(&response);
    assert_eq!(status, 401);
}
