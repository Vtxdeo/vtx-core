use crate::runtime::context::{SecurityPolicy, StreamContext};
use std::collections::HashSet;

pub fn enforce_sql_policy(
    ctx: &StreamContext,
    statement: &str,
    stmt: &rusqlite::Statement<'_>,
) -> Result<(), String> {
    if ctx.policy == SecurityPolicy::Root {
        return Ok(());
    }

    ensure_single_statement(statement)?;

    let leading = first_keyword(statement).ok_or("SQL Error: empty statement")?;
    let allowed = ["select", "with", "insert", "update", "delete", "replace"];
    if !allowed.contains(&leading.as_str()) {
        return Err("Permission Denied".into());
    }

    let is_readonly = stmt.readonly();
    if ctx.policy == SecurityPolicy::Restricted && !is_readonly {
        return Err("Permission Denied".into());
    }

    let plugin_id = ctx
        .plugin_id
        .as_ref()
        .ok_or("Permission Denied".to_string())?;

    let allowed_tables = ctx
        .registry
        .list_plugin_resources(plugin_id, "TABLE")
        .map_err(|e| e.to_string())?;
    let allowed_set: HashSet<String> = allowed_tables
        .into_iter()
        .map(|t| t.to_ascii_lowercase())
        .collect();

    let used_tables = extract_table_names(statement);
    if !is_readonly && used_tables.is_empty() {
        return Err("Permission Denied".into());
    }

    for table in used_tables {
        let name = table.to_ascii_lowercase();
        if name.starts_with("sys_") || name.contains('.') {
            return Err("Permission Denied".into());
        }
        if !allowed_set.contains(&name) {
            return Err("Permission Denied".into());
        }
    }

    Ok(())
}

fn ensure_single_statement(statement: &str) -> Result<(), String> {
    let bytes = statement.as_bytes();
    let mut i = 0;
    let mut in_single = false;
    let mut in_double = false;
    let mut in_backtick = false;
    let mut in_bracket = false;
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
        if in_backtick {
            if b == b'`' {
                if i + 1 < bytes.len() && bytes[i + 1] == b'`' {
                    i += 2;
                    continue;
                }
                in_backtick = false;
            }
            i += 1;
            continue;
        }
        if in_bracket {
            if b == b']' {
                in_bracket = false;
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
        if b == b'`' {
            in_backtick = true;
            i += 1;
            continue;
        }
        if b == b'[' {
            in_bracket = true;
            i += 1;
            continue;
        }

        if b == b';' {
            let rest = &statement[i + 1..];
            if has_non_ws_or_comment(rest) {
                return Err("Permission Denied".into());
            }
            return Ok(());
        }

        i += 1;
    }

    Ok(())
}

fn has_non_ws_or_comment(mut input: &str) -> bool {
    loop {
        let trimmed = input.trim_start();
        if trimmed.is_empty() {
            return false;
        }
        if trimmed.starts_with("--") {
            if let Some(idx) = trimmed.find('\n') {
                input = &trimmed[idx + 1..];
                continue;
            }
            return false;
        }
        if trimmed.starts_with("/*") {
            if let Some(idx) = trimmed.find("*/") {
                input = &trimmed[idx + 2..];
                continue;
            }
            return false;
        }
        return true;
    }
}

fn first_keyword(statement: &str) -> Option<String> {
    let mut it = SqlTokenizer::new(statement);
    while let Some(token) = it.next() {
        if !token.is_empty() {
            return Some(token.to_ascii_lowercase());
        }
    }
    None
}

fn extract_table_names(statement: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    let mut it = SqlTokenizer::new(statement);
    while let Some(token) = it.next() {
        tokens.push(token);
    }

    let mut tables = Vec::new();
    let keywords = ["from", "join", "update", "into"];

    for i in 0..tokens.len() {
        let token = &tokens[i];
        let lower = token.to_ascii_lowercase();
        if !keywords.contains(&lower.as_str()) {
            continue;
        }
        if let Some(next) = tokens.get(i + 1) {
            let next_lower = next.to_ascii_lowercase();
            if next_lower == "select" || next_lower == "with" {
                continue;
            }
            tables.push(next.clone());
        }
    }

    tables
}

struct SqlTokenizer<'a> {
    input: &'a [u8],
    pos: usize,
}

impl<'a> SqlTokenizer<'a> {
    fn new(input: &'a str) -> Self {
        Self {
            input: input.as_bytes(),
            pos: 0,
        }
    }

    fn next(&mut self) -> Option<String> {
        self.skip_ws_and_comments();
        if self.pos >= self.input.len() {
            return None;
        }

        let b = self.input[self.pos];
        if is_ident_start(b) {
            let start = self.pos;
            self.pos += 1;
            while self.pos < self.input.len() && is_ident_char(self.input[self.pos]) {
                self.pos += 1;
            }
            return Some(String::from_utf8_lossy(&self.input[start..self.pos]).into_owned());
        }

        if b == b'"' || b == b'`' || b == b'[' {
            return self.read_quoted_ident();
        }

        if b == b'\'' {
            self.read_string_literal();
            return self.next();
        }

        self.pos += 1;
        self.next()
    }

    fn skip_ws_and_comments(&mut self) {
        loop {
            while self.pos < self.input.len() && self.input[self.pos].is_ascii_whitespace() {
                self.pos += 1;
            }
            if self.pos + 1 < self.input.len()
                && self.input[self.pos] == b'-'
                && self.input[self.pos + 1] == b'-'
            {
                self.pos += 2;
                while self.pos < self.input.len() && self.input[self.pos] != b'\n' {
                    self.pos += 1;
                }
                continue;
            }
            if self.pos + 1 < self.input.len()
                && self.input[self.pos] == b'/'
                && self.input[self.pos + 1] == b'*'
            {
                self.pos += 2;
                while self.pos + 1 < self.input.len() {
                    if self.input[self.pos] == b'*' && self.input[self.pos + 1] == b'/' {
                        self.pos += 2;
                        break;
                    }
                    self.pos += 1;
                }
                continue;
            }
            break;
        }
    }

    fn read_quoted_ident(&mut self) -> Option<String> {
        let quote = self.input[self.pos];
        let end_quote = if quote == b'[' { b']' } else { quote };
        self.pos += 1;
        let start = self.pos;
        while self.pos < self.input.len() {
            if self.input[self.pos] == end_quote {
                let ident = String::from_utf8_lossy(&self.input[start..self.pos]).into_owned();
                self.pos += 1;
                return Some(ident);
            }
            self.pos += 1;
        }
        Some(String::from_utf8_lossy(&self.input[start..]).into_owned())
    }

    fn read_string_literal(&mut self) {
        self.pos += 1;
        while self.pos < self.input.len() {
            if self.input[self.pos] == b'\'' {
                if self.pos + 1 < self.input.len() && self.input[self.pos + 1] == b'\'' {
                    self.pos += 2;
                    continue;
                }
                self.pos += 1;
                break;
            }
            self.pos += 1;
        }
    }
}

fn is_ident_start(b: u8) -> bool {
    b.is_ascii_alphabetic() || b == b'_'
}

fn is_ident_char(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_' || b == b'.'
}
