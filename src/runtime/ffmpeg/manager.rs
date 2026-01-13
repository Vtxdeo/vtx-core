use std::env;
use std::path::{Path, PathBuf};
use tracing::info;

use super::binary::{verify_binary, FfmpegBinary};

#[derive(Debug)]
pub enum FatalError {
    EnvironmentBroken(String),
}

impl std::fmt::Display for FatalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FatalError::EnvironmentBroken(message) => write!(f, "{}", message),
        }
    }
}

impl std::error::Error for FatalError {}

pub struct VtxFfmpegManager {
    binary: FfmpegBinary,
    pub execution_timeout_secs: u64,
}

impl VtxFfmpegManager {
    pub fn new(execution_timeout_secs: u64) -> Result<Self, FatalError> {
        let path = resolve_ffmpeg_path()?;
        if !is_executable(&path) {
            return Err(FatalError::EnvironmentBroken(format!(
                "VTX_FFMPEG_BIN points to a missing or non-executable file: {}",
                path.display()
            )));
        }

        let (identity, version) = verify_binary(&path).map_err(|e| {
            FatalError::EnvironmentBroken(format!(
                "VTX_FFMPEG_BIN failed verification ({}): {}",
                path.display(),
                e
            ))
        })?;

        let binary = FfmpegBinary {
            path,
            version,
            identity,
            profile: "env".to_string(),
        };

        info!("[VtxFfmpeg] Using FFmpeg from VTX_FFMPEG_BIN.");

        Ok(Self {
            binary,
            execution_timeout_secs,
        })
    }

    pub fn get_binary(&self, requested_profile: &str) -> Option<&FfmpegBinary> {
        let _ = requested_profile;
        Some(&self.binary)
    }
}

fn resolve_ffmpeg_path() -> Result<PathBuf, FatalError> {
    let raw = env::var("VTX_FFMPEG_BIN").map_err(|_| {
        FatalError::EnvironmentBroken("VTX_FFMPEG_BIN is not set".to_string())
    })?;
    Ok(PathBuf::from(raw))
}

fn is_executable(path: &Path) -> bool {
    path.is_file()
}
