pub fn validate_ffmpeg_args(args: &[String]) -> Result<(), String> {
    let mut output_idx = None;
    let mut expect_value: Option<String> = None;

    for (idx, arg) in args.iter().enumerate() {
        let lower = arg.to_ascii_lowercase();

        if let Some(opt) = expect_value.take() {
            validate_ffmpeg_value(&opt, arg)?;
            continue;
        }

        if lower == "-" || lower == "pipe:1" {
            if output_idx.is_some() {
                return Err("Permission Denied: multiple outputs are not allowed".into());
            }
            output_idx = Some(idx);
            continue;
        }

        if !arg.starts_with('-') {
            return Err("Permission Denied: output must be pipe:1 or '-'".into());
        }

        if lower == "-i" || lower.starts_with("-i=") {
            return Err("Permission Denied: input must be bound to registry resource".into());
        }

        if lower == "-filter_script" || lower == "-filter_complex_script" {
            return Err("Permission Denied: filter scripts are not allowed".into());
        }

        if is_flag_option(&lower) {
            continue;
        }

        if let Some((key, value)) = split_option_value(&lower, arg) {
            validate_ffmpeg_value(&key, value)?;
            continue;
        }

        if is_value_option(&lower) {
            expect_value = Some(lower);
            continue;
        }

        return Err("Permission Denied: unsupported ffmpeg option".into());
    }

    if let Some(opt) = expect_value {
        return Err(format!("Permission Denied: missing value for {}", opt));
    }

    match output_idx {
        Some(idx) if idx + 1 == args.len() => Ok(()),
        Some(_) => Err("Permission Denied: output must be the last argument".into()),
        None => Err("Permission Denied: output must be pipe:1 or '-'".into()),
    }
}

fn is_flag_option(option: &str) -> bool {
    matches!(
        option,
        "-y" | "-n"
            | "-an"
            | "-vn"
            | "-sn"
            | "-dn"
            | "-shortest"
            | "-hide_banner"
            | "-nostdin"
            | "-stats"
    )
}

fn is_value_option(option: &str) -> bool {
    matches!(
        option,
        "-f" | "-c"
            | "-c:v"
            | "-c:a"
            | "-c:s"
            | "-b"
            | "-b:v"
            | "-b:a"
            | "-r"
            | "-vf"
            | "-af"
            | "-t"
            | "-ss"
            | "-to"
            | "-s"
            | "-s:v"
            | "-aspect"
            | "-preset"
            | "-crf"
            | "-pix_fmt"
            | "-profile:v"
            | "-level"
            | "-movflags"
            | "-map"
            | "-map_metadata"
            | "-metadata"
            | "-threads"
            | "-filter_complex"
            | "-maxrate"
            | "-minrate"
            | "-bufsize"
            | "-g"
            | "-keyint_min"
            | "-sc_threshold"
            | "-q:v"
            | "-q:a"
            | "-vcodec"
            | "-acodec"
            | "-loglevel"
    )
}

fn split_option_value<'a>(lower: &str, arg: &'a str) -> Option<(String, &'a str)> {
    if !lower.contains('=') {
        return None;
    }

    let mut parts = lower.splitn(2, '=');
    let key = parts.next().unwrap_or("");
    let _value_lower = parts.next().unwrap_or("");

    if !is_value_option(key) {
        return None;
    }

    let mut raw_parts = arg.splitn(2, '=');
    let _raw_key = raw_parts.next()?;
    let raw_value = raw_parts.next()?;
    Some((key.to_string(), raw_value))
}

fn validate_ffmpeg_value(option: &str, value: &str) -> Result<(), String> {
    let lower = value.to_ascii_lowercase();

    if lower.starts_with("pipe:") || lower.starts_with("file:") || lower.contains("://") {
        return Err("Permission Denied: external protocols are not allowed".into());
    }

    if lower.contains("movie=")
        || lower.contains("amovie=")
        || lower.contains("subtitles=")
        || lower.contains("ass=")
        || lower.contains("srt=")
        || lower.contains("concat:")
    {
        return Err("Permission Denied: file-based filters are not allowed".into());
    }

    if option == "-filter_complex" && lower.contains("movie=") {
        return Err("Permission Denied: file-based filters are not allowed".into());
    }

    Ok(())
}
