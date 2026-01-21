use std::path::Path;

const FFMPEG_VIDEO_EXTENSIONS: &[&str] = &[
    "3g2", "3gp", "3gp2", "3gpp", "3gpp2", "aaf", "amv", "asf", "asx", "avi", "avs", "bik", "bk2",
    "drc", "dv", "dif", "dvr-ms", "f4v", "f4p", "f4b", "flv", "fli", "flc", "gxf", "ivf", "m1v",
    "m2v", "m2p", "m2t", "m2ts", "mts", "m4v", "mj2", "mjpeg", "mjpg", "mkv", "mk3d", "mks", "mov",
    "mp4", "mpeg", "mpg", "mpe", "mpv", "mxf", "nsv", "nut", "ogm", "ogv", "qt", "rm", "rmvb",
    "roq", "smk", "swf", "ts", "vob", "webm", "wmv", "wtv", "y4m", "yuv", "h264", "264", "h265",
    "265", "hevc", "av1", "vp8", "vp9", "vc1", "r3d", "ism", "ismv",
];

pub fn content_type_for_path(path: &str) -> &'static str {
    let ext = Path::new(path)
        .extension()
        .and_then(|value| value.to_str())
        .unwrap_or("")
        .to_ascii_lowercase();

    match ext.as_str() {
        "mp4" | "m4v" | "f4v" => "video/mp4",
        "webm" => "video/webm",
        "mkv" | "mk3d" | "mks" => "video/x-matroska",
        "mov" | "qt" => "video/quicktime",
        "avi" => "video/x-msvideo",
        "flv" => "video/x-flv",
        "wmv" => "video/x-ms-wmv",
        "asf" | "asx" => "video/x-ms-asf",
        "3gp" | "3gpp" => "video/3gpp",
        "3g2" | "3gp2" | "3gpp2" => "video/3gpp2",
        "mpeg" | "mpg" | "mpe" | "mpv" | "m1v" | "m2v" | "m2p" | "vob" | "dv" => "video/mpeg",
        "m3u8" => "application/vnd.apple.mpegurl",
        "ts" | "m2ts" | "mts" | "m2t" => "video/mp2t",
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
        "vtx" => "application/vnd.vtx",
        ext if FFMPEG_VIDEO_EXTENSIONS.contains(&ext) => "video/x-ffmpeg",
        _ => "application/octet-stream",
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
        assert_eq!(content_type_for_path("movie.m2ts"), "video/mp2t");
        assert_eq!(content_type_for_path("audio.mp3"), "audio/mpeg");
        assert_eq!(content_type_for_path("image.jpeg"), "image/jpeg");
        assert_eq!(
            content_type_for_path("playlist.m3u8"),
            "application/vnd.apple.mpegurl"
        );
        assert_eq!(content_type_for_path("plugin.vtx"), "application/vnd.vtx");
    }

    #[test]
    fn falls_back_for_unknown_extensions() {
        assert_eq!(
            content_type_for_path("archive.bin"),
            "application/octet-stream"
        );
        assert_eq!(
            content_type_for_path("no_extension"),
            "application/octet-stream"
        );
    }
}
