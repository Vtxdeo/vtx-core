use super::ident::{is_ident_char, is_ident_start};

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(super) enum TokenKind {
    Word,
    Other,
}

pub(super) struct Token {
    pub(super) kind: TokenKind,
    pub(super) start: usize,
    pub(super) end: usize,
    pub(super) value: String,
    pub(super) quote: Option<char>,
}

impl Token {
    pub(super) fn wrap(&self, value: &str) -> String {
        match self.quote {
            Some('[') => format!("[{}]", value),
            Some(q) => format!("{}{}{}", q, value, q),
            None => value.to_string(),
        }
    }
}

pub(super) fn tokenize(statement: &str) -> Result<Vec<Token>, String> {
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

pub(super) fn next_word(tokens: &[Token], start: usize) -> Result<(usize, String), String> {
    for (i, token) in tokens.iter().enumerate().skip(start) {
        if token.kind == TokenKind::Word {
            let word = token.value.to_ascii_lowercase();
            if word == "if" || word == "not" || word == "exists" || word == "temporary" {
                continue;
            }
            return Ok((i, word));
        }
    }
    Err("Migration SQL not allowed".into())
}

pub(super) fn next_identifier(tokens: &[Token], start: usize) -> Result<usize, String> {
    for (i, token) in tokens.iter().enumerate().skip(start) {
        if token.kind == TokenKind::Word {
            return Ok(i);
        }
    }
    Err("Migration SQL not allowed".into())
}

pub(super) fn next_table_ident(tokens: &[Token], start: usize) -> Result<usize, String> {
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

pub(super) fn ensure_single_statement(statement: &str) -> Result<(), String> {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ensure_single_statement_semicolon_rules() {
        assert!(ensure_single_statement("CREATE TABLE t (id INT);").is_ok());
        assert!(ensure_single_statement("CREATE TABLE t (id INT);   ").is_ok());
        assert!(ensure_single_statement("CREATE TABLE t (note TEXT DEFAULT ';');").is_ok());
        assert!(ensure_single_statement("CREATE TABLE t (id INT) -- ;\n").is_ok());
        assert!(ensure_single_statement("/* ; */ CREATE TABLE t (id INT)").is_ok());
        assert!(ensure_single_statement("CREATE TABLE t (id INT); DROP TABLE t;").is_err());
        assert!(ensure_single_statement("CREATE TABLE t (id INT); -- trailing").is_err());
    }

    #[test]
    fn tokenize_handles_quoted_identifiers() {
        let tokens = tokenize("CREATE TABLE \"t\" (id INT)").expect("tokenize");
        let quoted = tokens
            .iter()
            .find(|t| t.value == "t" && t.quote == Some('"'))
            .expect("quoted token");
        assert_eq!(quoted.kind, TokenKind::Word);

        let tokens = tokenize("CREATE TABLE [t] (id INT)").expect("tokenize");
        let quoted = tokens
            .iter()
            .find(|t| t.value == "t" && t.quote == Some('['))
            .expect("quoted token");
        assert_eq!(quoted.kind, TokenKind::Word);

        let tokens = tokenize("CREATE TABLE `t` (id INT)").expect("tokenize");
        let quoted = tokens
            .iter()
            .find(|t| t.value == "t" && t.quote == Some('`'))
            .expect("quoted token");
        assert_eq!(quoted.kind, TokenKind::Word);
    }

    #[test]
    fn next_word_and_table_ident_skip_optional_keywords() {
        let tokens = tokenize("CREATE TEMPORARY TABLE IF NOT EXISTS t (id INT)").expect("tokenize");
        let create_idx = tokens
            .iter()
            .position(|t| t.value.eq_ignore_ascii_case("create"))
            .expect("create");
        let (_idx, word) = next_word(&tokens, create_idx + 1).expect("next word");
        assert_eq!(word, "table");
        let table_idx = next_table_ident(&tokens, create_idx + 1).expect("table ident");
        assert!(tokens[table_idx].value.eq_ignore_ascii_case("temporary"));
    }

    #[test]
    fn next_table_ident_skips_if_not_exists() {
        let tokens = tokenize("CREATE TABLE IF NOT EXISTS t (id INT)").expect("tokenize");
        let table_idx = tokens
            .iter()
            .position(|t| t.value.eq_ignore_ascii_case("table"))
            .expect("table");
        let table_idx = next_table_ident(&tokens, table_idx + 1).expect("table ident");
        assert_eq!(tokens[table_idx].value, "t");
    }
}
