use super::api::ffmpeg::FfmpegOption;

pub fn validate_ffmpeg_options(options: &[FfmpegOption]) -> Result<(), String> {
    for option in options {
        if let Some(value) = option.value.as_deref() {
            validate_ffmpeg_value(value)?;
            continue;
        }

        if let Some((_, value)) = option.key.split_once('=') {
            validate_ffmpeg_value(value)?;
        }
    }

    Ok(())
}

fn validate_ffmpeg_value(value: &str) -> Result<(), String> {
    let lower = value.to_ascii_lowercase();

    if lower.starts_with("pipe:")
        || lower.starts_with("http:")
        || lower.starts_with("https:")
        || lower.contains("://")
    {
        return Err("Permission Denied: external protocols are not allowed".into());
    }

    Ok(())
}
