use std::collections::HashSet;

pub fn normalize_declared_resources(
    plugin_id: &str,
    declared: Vec<String>,
) -> Result<Vec<String>, String> {
    let prefix = plugin_prefix(plugin_id);
    let mut out = Vec::with_capacity(declared.len());
    let mut seen = HashSet::new();

    for name in declared {
        let normalized = normalize_name(&prefix, &name)?;
        if seen.insert(normalized.clone()) {
            out.push(normalized);
        }
    }

    Ok(out)
}

pub fn validate_and_rewrite_migration(
    plugin_id: &str,
    declared_tables: &HashSet<String>,
    statement: &str,
) -> Result<String, String> {
    ensure_single_statement(statement)?;

    let tokens = tokenize(statement)?;
    if tokens.is_empty() {
        return Err("Migration SQL is empty".into());
    }

    let first = tokens
        .iter()
        .find(|t| t.kind == TokenKind::Word)
        .map(|t| t.value.to_ascii_lowercase())
        .ok_or("Migration SQL is empty")?;

    let allowed = ["create", "alter", "drop"];
    if !allowed.contains(&first.as_str()) {
        return Err("Migration SQL not allowed".into());
    }

    let prefix = plugin_prefix(plugin_id);
    let mut replacements: Vec<(usize, usize, String)> = Vec::new();

    let mut i = 0;
    while i < tokens.len() {
        let tok = &tokens[i];
        if tok.kind != TokenKind::Word {
            i += 1;
            continue;
        }
        let word = tok.value.to_ascii_lowercase();

        if word == "create" {
            let (kind_idx, kind_word) = next_word(&tokens, i + 1)?;
            if kind_word == "table" {
                let table_idx = next_table_ident(&tokens, kind_idx + 1)?;
                let replacement = rewrite_ident(&tokens[table_idx], &prefix, declared_tables)?;
                replacements.push(replacement);
                i = table_idx + 1;
                continue;
            }
            if kind_word == "index" || kind_word == "unique" {
                let mut idx = kind_idx;
                if kind_word == "unique" {
                    let (next_idx, next_word) = next_word(&tokens, idx + 1)?;
                    if next_word != "index" {
                        return Err("Migration SQL not allowed".into());
                    }
                    idx = next_idx;
                }
                let index_idx = next_identifier(&tokens, idx + 1)?;
                let replacement = rewrite_index_ident(&tokens[index_idx], &prefix)?;
                replacements.push(replacement);

                let (on_idx, on_word) = next_word(&tokens, index_idx + 1)?;
                if on_word != "on" {
                    return Err("Migration SQL not allowed".into());
                }
                let table_idx = next_identifier(&tokens, on_idx + 1)?;
                let replacement = rewrite_ident(&tokens[table_idx], &prefix, declared_tables)?;
                replacements.push(replacement);
                i = table_idx + 1;
                continue;
            }
            return Err("Migration SQL not allowed".into());
        }

        if word == "alter" {
            let (table_idx, table_word) = next_word(&tokens, i + 1)?;
            if table_word != "table" {
                return Err("Migration SQL not allowed".into());
            }
            let ident_idx = next_identifier(&tokens, table_idx + 1)?;
            let replacement = rewrite_ident(&tokens[ident_idx], &prefix, declared_tables)?;
            replacements.push(replacement);
            i = ident_idx + 1;
            continue;
        }

        if word == "drop" {
            let (kind_idx, kind_word) = next_word(&tokens, i + 1)?;
            if kind_word != "index" {
                return Err("Migration SQL not allowed".into());
            }
            let index_idx = next_identifier(&tokens, kind_idx + 1)?;
            let replacement = rewrite_index_ident(&tokens[index_idx], &prefix)?;
            replacements.push(replacement);
            i = index_idx + 1;
            continue;
        }

        i += 1;
    }

    if replacements.is_empty() {
        return Err("Migration SQL not allowed".into());
    }

    Ok(apply_replacements(statement, &replacements))
}

fn plugin_prefix(plugin_id: &str) -> String {
    format!("vtx_plugin_{}_", plugin_id)
}

fn normalize_name(prefix: &str, name: &str) -> Result<String, String> {
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

fn rewrite_ident(
    token: &Token,
    prefix: &str,
    declared_tables: &HashSet<String>,
) -> Result<(usize, usize, String), String> {
    let normalized = normalize_name(prefix, &token.value)?;
    if !declared_tables.contains(&normalized) {
        return Err("Migration SQL not allowed".into());
    }
    let replacement = token.wrap(&normalized);
    Ok((token.start, token.end, replacement))
}

fn rewrite_index_ident(token: &Token, prefix: &str) -> Result<(usize, usize, String), String> {
    let normalized = normalize_name(prefix, &token.value)?;
    let replacement = token.wrap(&normalized);
    Ok((token.start, token.end, replacement))
}

fn apply_replacements(statement: &str, replacements: &[(usize, usize, String)]) -> String {
    let mut replacements = replacements.to_vec();
    replacements.sort_by_key(|r| r.0);

    let mut out = String::with_capacity(statement.len());
    let mut last = 0;
    for (start, end, rep) in replacements {
        if start >= last {
            out.push_str(&statement[last..start]);
            out.push_str(&rep);
            last = end;
        }
    }
    out.push_str(&statement[last..]);
    out
}

fn ensure_single_statement(statement: &str) -> Result<(), String> {
    let bytes = statement.as_bytes();
    let mut i = 0;
    let mut in_single = false;
    let mut in_double = false;
    let mut in_line_comment = false;
    let mut in_block_comment = false;

    while i < bytes.len() {
        let b = bytes[i];
        if in_line_comment {
            if b == b'\n' {
                in_line_comment = false;
            }
            i += 1;
            continue;
        }
        if in_block_comment {
            if b == b'*' && i + 1 < bytes.len() && bytes[i + 1] == b'/' {
                in_block_comment = false;
                i += 2;
                continue;
            }
            i += 1;
            continue;
        }
        if in_single {
            if b == b'\'' {
                if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                    i += 2;
                    continue;
                }
                in_single = false;
            }
            i += 1;
            continue;
        }
        if in_double {
            if b == b'"' {
                if i + 1 < bytes.len() && bytes[i + 1] == b'"' {
                    i += 2;
                    continue;
                }
                in_double = false;
            }
            i += 1;
            continue;
        }

        if b == b'-' && i + 1 < bytes.len() && bytes[i + 1] == b'-' {
            in_line_comment = true;
            i += 2;
            continue;
        }
        if b == b'/' && i + 1 < bytes.len() && bytes[i + 1] == b'*' {
            in_block_comment = true;
            i += 2;
            continue;
        }
        if b == b'\'' {
            in_single = true;
            i += 1;
            continue;
        }
        if b == b'"' {
            in_double = true;
            i += 1;
            continue;
        }
        if b == b';' {
            let rest = &statement[i + 1..];
            if rest.trim().is_empty() {
                return Ok(());
            }
            return Err("Migration SQL not allowed".into());
        }
        i += 1;
    }

    Ok(())
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum TokenKind {
    Word,
    Other,
}

struct Token {
    kind: TokenKind,
    start: usize,
    end: usize,
    value: String,
    quote: Option<char>,
}

impl Token {
    fn wrap(&self, value: &str) -> String {
        match self.quote {
            Some('[') => format!("[{}]", value),
            Some(q) => format!("{}{}{}", q, value, q),
            None => value.to_string(),
        }
    }
}

fn tokenize(statement: &str) -> Result<Vec<Token>, String> {
    let bytes = statement.as_bytes();
    let mut tokens = Vec::new();
    let mut i = 0;

    while i < bytes.len() {
        let b = bytes[i];
        if b.is_ascii_whitespace() {
            i += 1;
            continue;
        }
        if b == b'-' && i + 1 < bytes.len() && bytes[i + 1] == b'-' {
            while i < bytes.len() && bytes[i] != b'\n' {
                i += 1;
            }
            continue;
        }
        if b == b'/' && i + 1 < bytes.len() && bytes[i + 1] == b'*' {
            i += 2;
            while i + 1 < bytes.len() {
                if bytes[i] == b'*' && bytes[i + 1] == b'/' {
                    i += 2;
                    break;
                }
                i += 1;
            }
            continue;
        }
        if b == b'\'' {
            i += 1;
            while i < bytes.len() {
                if bytes[i] == b'\'' {
                    if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                        i += 2;
                        continue;
                    }
                    i += 1;
                    break;
                }
                i += 1;
            }
            continue;
        }

        if b == b'"' || b == b'`' || b == b'[' {
            let start = i;
            let quote = b;
            let end_quote = if quote == b'[' { b']' } else { quote };
            i += 1;
            let name_start = i;
            while i < bytes.len() && bytes[i] != end_quote {
                i += 1;
            }
            let name = statement[name_start..i].to_string();
            if i < bytes.len() {
                i += 1;
            }
            tokens.push(Token {
                kind: TokenKind::Word,
                start,
                end: i,
                value: name,
                quote: Some(quote as char),
            });
            continue;
        }

        if is_ident_start(b) {
            let start = i;
            i += 1;
            while i < bytes.len() && is_ident_char(bytes[i]) {
                i += 1;
            }
            let name = statement[start..i].to_string();
            tokens.push(Token {
                kind: TokenKind::Word,
                start,
                end: i,
                value: name,
                quote: None,
            });
            continue;
        }

        tokens.push(Token {
            kind: TokenKind::Other,
            start: i,
            end: i + 1,
            value: statement[i..i + 1].to_string(),
            quote: None,
        });
        i += 1;
    }

    Ok(tokens)
}

fn next_word(tokens: &[Token], start: usize) -> Result<(usize, String), String> {
    for i in start..tokens.len() {
        if tokens[i].kind == TokenKind::Word {
            let word = tokens[i].value.to_ascii_lowercase();
            if word == "if" || word == "not" || word == "exists" || word == "temporary" {
                continue;
            }
            return Ok((i, word));
        }
    }
    Err("Migration SQL not allowed".into())
}

fn next_identifier(tokens: &[Token], start: usize) -> Result<usize, String> {
    for i in start..tokens.len() {
        if tokens[i].kind == TokenKind::Word {
            return Ok(i);
        }
    }
    Err("Migration SQL not allowed".into())
}

fn next_table_ident(tokens: &[Token], start: usize) -> Result<usize, String> {
    let mut idx = start;
    while idx < tokens.len() {
        if tokens[idx].kind == TokenKind::Word {
            let word = tokens[idx].value.to_ascii_lowercase();
            if word == "if" || word == "not" || word == "exists" {
                idx += 1;
                continue;
            }
            return Ok(idx);
        }
        idx += 1;
    }
    Err("Migration SQL not allowed".into())
}

fn is_ident_start(b: u8) -> bool {
    b.is_ascii_alphabetic() || b == b'_'
}

fn is_ident_char(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
}

fn is_valid_identifier(name: &str) -> bool {
    !name.is_empty() && name.bytes().all(is_ident_char)
}
