use std::collections::HashSet;

use super::ident::normalize_name;
use super::token::Token;

pub(super) fn rewrite_ident(
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

pub(super) fn rewrite_index_ident(
    token: &Token,
    prefix: &str,
) -> Result<(usize, usize, String), String> {
    let normalized = normalize_name(prefix, &token.value)?;
    let replacement = token.wrap(&normalized);
    Ok((token.start, token.end, replacement))
}

pub(super) fn apply_replacements(
    statement: &str,
    replacements: &[(usize, usize, String)],
) -> String {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::manager::migration_policy::token::tokenize;

    #[test]
    fn apply_replacements_inserts_wrapped_identifier() {
        let statement = "CREATE TABLE foo (id INT)";
        let tokens = tokenize(statement).expect("tokenize");
        let foo = tokens.iter().find(|t| t.value == "foo").expect("foo");
        let replacement = (foo.start, foo.end, foo.wrap("bar"));
        let rewritten = apply_replacements(statement, &[replacement]);
        assert_eq!(rewritten, "CREATE TABLE bar (id INT)");
    }
}
