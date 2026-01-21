use std::collections::HashSet;
use std::io::{Cursor, Seek, SeekFrom};

use futures_util::StreamExt;
use reqwest::redirect::Policy;
use reqwest::{header, Client, Method};
use url::Url;
use wasmtime::component::Resource;

use crate::common::buffer::{BufferType, RealBuffer};
use crate::runtime::context::{SecurityPolicy, StreamContext};

use super::api;

impl api::vtx_http_client::Host for StreamContext {
    async fn request(
        &mut self,
        req: api::vtx_types::HttpClientRequest,
    ) -> Result<api::vtx_types::HttpClientResponse, String> {
        if self.policy == SecurityPolicy::Restricted {
            return Err("Permission Denied".into());
        }

        let url = Url::parse(&req.url).map_err(|e| format!("Invalid URL: {}", e))?;
        let method = req.method.to_ascii_uppercase();

        let rule = match_allow_rule(&self.http_allowlist, &url, &method, &req.headers)
            .ok_or_else(|| "Permission Denied: HTTP not allowed".to_string())?;

        let body = if let Some(resource) = req.body {
            Some(read_buffer_resource(self, resource, rule.max_request_bytes).await?)
        } else {
            None
        };

        let client = Client::builder()
            .redirect(build_redirect_policy(&rule, self.http_allowlist.clone()))
            .build()
            .map_err(|e| format!("HTTP client init failed: {}", e))?;

        let mut request = client.request(
            Method::from_bytes(method.as_bytes())
                .map_err(|_| format!("Invalid HTTP method: {}", method))?,
            url,
        );

        if !req.headers.is_empty() {
            let mut header_map = header::HeaderMap::new();
            for (name, value) in req.headers {
                let header_name = header::HeaderName::from_bytes(name.as_bytes())
                    .map_err(|_| format!("Invalid header name: {}", name))?;
                let header_value = header::HeaderValue::from_str(&value)
                    .map_err(|_| format!("Invalid header value for {}.", name))?;
                header_map.append(header_name, header_value);
            }
            request = request.headers(header_map);
        }

        if let Some(body_bytes) = body {
            request = request.body(body_bytes);
        }

        let response = request
            .send()
            .await
            .map_err(|e| format!("HTTP request failed: {}", e))?;

        let status = response.status().as_u16();
        let headers = response
            .headers()
            .iter()
            .filter_map(|(k, v)| v.to_str().ok().map(|val| (k.to_string(), val.to_string())))
            .collect::<Vec<_>>();

        let mut body_bytes = Vec::new();
        let mut total_read = 0u64;
        let mut stream = response.bytes_stream();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|e| format!("Failed to read response body: {}", e))?;
            if let Some(limit) = rule.max_response_bytes {
                let next_total = total_read.saturating_add(chunk.len() as u64);
                if next_total > limit {
                    return Err("Response body exceeded max-response-bytes".into());
                }
                total_read = next_total;
            }
            body_bytes.extend_from_slice(&chunk);
        }

        let body = if body_bytes.is_empty() {
            None
        } else {
            let buffer = RealBuffer {
                inner: BufferType::Memory(Cursor::new(std::mem::take(&mut body_bytes))),
                uri_hint: None,
                mime_override: None,
                process_handle: None,
            };
            Some(
                self.table
                    .push(buffer)
                    .map_err(|e| format!("Response buffer allocation failed: {}", e))?,
            )
        };

        Ok(api::vtx_types::HttpClientResponse {
            status,
            headers,
            body,
        })
    }
}

fn match_allow_rule(
    rules: &[api::vtx_types::HttpAllowRule],
    url: &Url,
    method: &str,
    headers: &[(String, String)],
) -> Option<api::vtx_types::HttpAllowRule> {
    for rule in rules {
        if !scheme_matches(rule, url) {
            continue;
        }
        if !host_matches(rule, url) {
            continue;
        }
        if !port_matches(rule, url) {
            continue;
        }
        if !path_matches(rule, url) {
            continue;
        }
        if !method_matches(rule, method) {
            continue;
        }
        if !headers_allowed(rule, headers) {
            continue;
        }
        return Some(rule.clone());
    }
    None
}

fn scheme_matches(rule: &api::vtx_types::HttpAllowRule, url: &Url) -> bool {
    let scheme = rule.scheme.to_ascii_lowercase();
    scheme == url.scheme().to_ascii_lowercase()
}

fn host_matches(rule: &api::vtx_types::HttpAllowRule, url: &Url) -> bool {
    let host = match url.host_str() {
        Some(host) => host.to_ascii_lowercase(),
        None => return false,
    };
    rule.host.to_ascii_lowercase() == host
}

fn port_matches(rule: &api::vtx_types::HttpAllowRule, url: &Url) -> bool {
    let default_port = match url.scheme() {
        "https" => Some(443),
        "http" => Some(80),
        _ => None,
    };
    let url_port = url.port_or_known_default();
    match rule.port {
        Some(port) => Some(port) == url_port,
        None => url_port == default_port,
    }
}

fn path_matches(rule: &api::vtx_types::HttpAllowRule, url: &Url) -> bool {
    let path = url.path();
    match rule.path.as_deref() {
        None => true,
        Some(rule_path) => {
            if rule_path == "/*" {
                return true;
            }
            if let Some(prefix) = rule_path.strip_suffix("/*") {
                return path.starts_with(prefix);
            }
            path == rule_path
        }
    }
}

fn method_matches(rule: &api::vtx_types::HttpAllowRule, method: &str) -> bool {
    match &rule.methods {
        None => true,
        Some(methods) => methods.iter().any(|m| m.to_ascii_uppercase() == method),
    }
}

fn headers_allowed(rule: &api::vtx_types::HttpAllowRule, headers: &[(String, String)]) -> bool {
    let Some(allowed) = &rule.allow_headers else {
        return true;
    };
    if allowed.is_empty() {
        return headers.is_empty();
    }
    let allowed_set = allowed
        .iter()
        .map(|h| h.to_ascii_lowercase())
        .collect::<HashSet<_>>();
    headers
        .iter()
        .all(|(name, _)| allowed_set.contains(&name.to_ascii_lowercase()))
}

fn build_redirect_policy(
    rule: &api::vtx_types::HttpAllowRule,
    rules: Vec<api::vtx_types::HttpAllowRule>,
) -> Policy {
    if rule.follow_redirects != Some(true) {
        return Policy::none();
    }

    match rule.redirect_policy.as_deref().unwrap_or("same-origin") {
        "allowlist" => Policy::custom(move |attempt| {
            if let Ok(url) = Url::parse(attempt.url().as_str()) {
                if match_allow_rule(&rules, &url, "GET", &[]).is_some() {
                    return attempt.follow();
                }
            }
            attempt.stop()
        }),
        _ => Policy::custom(move |attempt| {
            let previous = attempt.previous().last();
            let Some(prev) = previous else {
                return attempt.stop();
            };
            if prev.host_str() == attempt.url().host_str()
                && prev.scheme() == attempt.url().scheme()
                && prev.port_or_known_default() == attempt.url().port_or_known_default()
            {
                return attempt.follow();
            }
            attempt.stop()
        }),
    }
}

async fn read_buffer_resource(
    ctx: &mut StreamContext,
    resource: Resource<RealBuffer>,
    max_bytes: Option<u64>,
) -> Result<Vec<u8>, String> {
    use std::io::Read;

    let rb = ctx
        .table
        .get_mut(&resource)
        .map_err(|_| "Invalid buffer handle".to_string())?;

    let result = match &mut rb.inner {
        BufferType::Memory(cursor) => {
            let len = cursor.get_ref().len() as u64;
            if let Some(limit) = max_bytes {
                if len > limit {
                    return Err("Request body exceeded max-request-bytes".into());
                }
            }
            cursor
                .seek(SeekFrom::Start(0))
                .map_err(|e| format!("Buffer seek failed: {}", e))?;
            let mut data = Vec::with_capacity(len as usize);
            cursor
                .read_to_end(&mut data)
                .map_err(|e| format!("Buffer read failed: {}", e))?;
            Ok(data)
        }
        BufferType::Object { uri } => {
            let meta = ctx
                .vfs
                .head(uri)
                .await
                .map_err(|e| format!("VFS metadata failed: {}", e))?;
            if let Some(limit) = max_bytes {
                if meta.size > limit {
                    return Err("Request body exceeded max-request-bytes".into());
                }
            }
            Ok(ctx
                .vfs
                .read_range(uri, 0, meta.size)
                .await
                .map(|bytes| bytes.to_vec())
                .map_err(|e| format!("VFS read failed: {}", e))?)
        }
        BufferType::Pipe(_) => Err("Pipe body is not supported".into()),
    };

    let _ = ctx.table.delete(resource);
    result
}
