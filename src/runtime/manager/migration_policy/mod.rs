use std::collections::HashSet;

mod ident;
mod rewrite;
mod token;

use ident::{normalize_name, plugin_prefix};
use rewrite::{apply_replacements, rewrite_ident, rewrite_index_ident};
use token::{
    ensure_single_statement, next_identifier, next_table_ident, next_word, tokenize, TokenKind,
};

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
