use std::path::Path;

pub fn content_type_for_path(path: &str) -> &'static str {
    let ext = Path::new(path)
        .extension()
        .and_then(|value| value.to_str())
        .unwrap_or("")
        .to_ascii_lowercase();

    match ext.as_str() {
        "mp4" | "m4v" => "video/mp4",
        "webm" => "video/webm",
        "mkv" => "video/x-matroska",
        "mov" => "video/quicktime",
        "avi" => "video/x-msvideo",
        "m3u8" => "application/vnd.apple.mpegurl",
        "ts" => "video/mp2t",
        "mp3" => "audio/mpeg",
        "wav" => "audio/wav",
        "flac" => "audio/flac",
        "ogg" | "oga" | "opus" => "audio/ogg",
        "jpg" | "jpeg" => "image/jpeg",
        "png" => "image/png",
        "gif" => "image/gif",
        "webp" => "image/webp",
        "json" => "application/json",
        "txt" => "text/plain; charset=utf-8",
        _ => "video/mp4",
    }
}

#[cfg(test)]
mod tests {
    use super::content_type_for_path;

    #[test]
    fn returns_expected_content_types() {
        assert_eq!(content_type_for_path("movie.mp4"), "video/mp4");
        assert_eq!(content_type_for_path("movie.M4V"), "video/mp4");
        assert_eq!(content_type_for_path("clip.webm"), "video/webm");
        assert_eq!(content_type_for_path("audio.mp3"), "audio/mpeg");
        assert_eq!(content_type_for_path("image.jpeg"), "image/jpeg");
        assert_eq!(
            content_type_for_path("playlist.m3u8"),
            "application/vnd.apple.mpegurl"
        );
    }

    #[test]
    fn falls_back_for_unknown_extensions() {
        assert_eq!(content_type_for_path("archive.bin"), "video/mp4");
        assert_eq!(content_type_for_path("no_extension"), "video/mp4");
    }
}
