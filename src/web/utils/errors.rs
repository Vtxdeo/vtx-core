use serde_json::Value;

pub const CODE_INTERNAL: &str = "VTX-CORE-500";
pub const CODE_BAD_REQUEST: &str = "VTX-CORE-400";
pub const CODE_NOT_FOUND: &str = "VTX-CORE-404";
pub const CODE_FORBIDDEN: &str = "VTX-CORE-403";
pub const CODE_UNAUTHORIZED: &str = "VTX-CORE-401";
pub const CODE_CONFLICT: &str = "VTX-CORE-409";

pub const CODE_ADMIN_INTERNAL: &str = "VTX-ADM-500";
pub const CODE_ADMIN_BAD_REQUEST: &str = "VTX-ADM-400";
pub const CODE_ADMIN_NOT_FOUND: &str = "VTX-ADM-404";

pub const CODE_PLUGIN_INTERNAL: &str = "VTX-PLG-500";
pub const CODE_PLUGIN_NOT_FOUND: &str = "VTX-PLG-404";

pub fn internal_error_json(details: &str) -> Value {
    error_json(CODE_INTERNAL, "Internal error", Some(details))
}

pub fn internal_error_message(details: &str) -> String {
    public_message("Internal error", details)
}

pub fn bad_request_json(details: &str) -> Value {
    error_json(CODE_BAD_REQUEST, "Invalid request", Some(details))
}

pub fn not_found_json(details: &str) -> Value {
    error_json(CODE_NOT_FOUND, "Not found", Some(details))
}

pub fn admin_internal_error_json(details: &str) -> Value {
    error_json(CODE_ADMIN_INTERNAL, "Internal error", Some(details))
}

pub fn admin_bad_request_json(details: &str) -> Value {
    error_json(CODE_ADMIN_BAD_REQUEST, "Invalid request", Some(details))
}

pub fn admin_not_found_json(details: &str) -> Value {
    error_json(CODE_ADMIN_NOT_FOUND, "Not found", Some(details))
}

pub fn plugin_internal_error_json(details: &str) -> Value {
    error_json(CODE_PLUGIN_INTERNAL, "Internal error", Some(details))
}

pub fn plugin_not_found_json(details: &str) -> Value {
    error_json(CODE_PLUGIN_NOT_FOUND, "Not found", Some(details))
}

pub fn error_json(code: &str, safe_message: &str, details: Option<&str>) -> Value {
    let message = if cfg!(debug_assertions) {
        details.unwrap_or(safe_message)
    } else {
        safe_message
    };
    serde_json::json!({
        "status": "error",
        "code": code,
        "message": message
    })
}

pub fn public_message(safe_message: &str, details: &str) -> String {
    if cfg!(debug_assertions) {
        format!("{}: {}", safe_message, details)
    } else {
        safe_message.to_string()
    }
}
