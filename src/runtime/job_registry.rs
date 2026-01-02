use serde_json::Value;

pub struct JobDefinition {
    pub job_type: &'static str,
    pub required_group: Option<&'static str>,
    pub schema_version: i64,
}

const JOB_DEFINITIONS: &[JobDefinition] = &[
    JobDefinition {
        job_type: "noop",
        required_group: None,
        schema_version: 1,
    },
    JobDefinition {
        job_type: "scan-directory",
        required_group: Some("admin"),
        schema_version: 1,
    },
];

pub fn get_job_definition(job_type: &str) -> Option<&'static JobDefinition> {
    JOB_DEFINITIONS.iter().find(|def| def.job_type == job_type)
}

fn validate_job_payload(job_type: &str, payload: &Value) -> Result<(), String> {
    match job_type {
        "noop" => Ok(()),
        "scan-directory" => {
            let path = payload
                .get("path")
                .and_then(|value| value.as_str())
                .map(|value| value.trim())
                .unwrap_or("");
            if path.is_empty() {
                return Err("payload.path is required".into());
            }
            if path.len() > 2048 {
                return Err("payload.path is too long".into());
            }
            Ok(())
        }
        _ => Err("unsupported job_type".into()),
    }
}

fn migrate_payload(
    job_type: &str,
    payload: &Value,
    from_version: i64,
    to_version: i64,
) -> Result<Value, String> {
    if from_version == to_version {
        return Ok(payload.clone());
    }
    match (job_type, from_version, to_version) {
        ("scan-directory", 0, 1) => {
            let mut upgraded = payload.clone();
            if upgraded.get("path").is_none() {
                if let Some(directory) = upgraded.get("directory").cloned() {
                    upgraded["path"] = directory;
                }
            }
            Ok(upgraded)
        }
        _ => Err("payload migration not supported".into()),
    }
}

pub fn normalize_payload(
    job_type: &str,
    payload: &Value,
    payload_version: i64,
) -> Result<(Value, i64), String> {
    let definition =
        get_job_definition(job_type).ok_or_else(|| "unsupported job_type".to_string())?;
    if payload_version > definition.schema_version {
        return Err("unsupported payload version".into());
    }
    let normalized = if payload_version < definition.schema_version {
        migrate_payload(job_type, payload, payload_version, definition.schema_version)?
    } else {
        payload.clone()
    };
    validate_job_payload(job_type, &normalized)?;
    Ok((normalized, definition.schema_version))
}

pub fn validate_job_submission(
    job_type: &str,
    payload: &Value,
    user_groups: Option<&[String]>,
    payload_version: i64,
) -> Result<(), String> {
    let definition =
        get_job_definition(job_type).ok_or_else(|| "unsupported job_type".to_string())?;
    if let (Some(required_group), Some(groups)) = (definition.required_group, user_groups) {
        let allowed = groups.iter().any(|group| group == required_group);
        if !allowed {
            return Err("permission denied".into());
        }
    }
    let _ = normalize_payload(job_type, payload, payload_version)?;
    Ok(())
}
