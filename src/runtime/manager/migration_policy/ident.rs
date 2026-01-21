pub(super) fn plugin_prefix(plugin_id: &str) -> String {
    format!("vtx_plugin_{}_", plugin_id)
}

pub(super) fn normalize_name(prefix: &str, name: &str) -> Result<String, String> {
    let name = name.trim();
    if name.contains('.') {
        return Err("Migration SQL not allowed".into());
    }
    let normalized = if name.starts_with("vtx_plugin_") {
        if !name.starts_with(prefix) {
            return Err("Migration SQL not allowed".into());
        }
        name.to_string()
    } else {
        format!("{}{}", prefix, name)
    };

    if !is_valid_identifier(&normalized) {
        return Err("Migration SQL not allowed".into());
    }

    Ok(normalized)
}

pub(super) fn is_ident_start(b: u8) -> bool {
    b.is_ascii_alphabetic() || b == b'_'
}

pub(super) fn is_ident_char(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
}

fn is_valid_identifier(name: &str) -> bool {
    !name.is_empty() && name.bytes().all(is_ident_char)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_name_prefix_and_validation() {
        let prefix = plugin_prefix("p1");
        assert_eq!(
            normalize_name(&prefix, "items").expect("normalize"),
            "vtx_plugin_p1_items"
        );
        assert_eq!(
            normalize_name(&prefix, "vtx_plugin_p1_items").expect("normalize"),
            "vtx_plugin_p1_items"
        );
        assert!(normalize_name(&prefix, "vtx_plugin_other_items").is_err());
        assert!(normalize_name(&prefix, "bad.name").is_err());
        assert!(normalize_name(&prefix, "bad-name").is_err());
    }
}
