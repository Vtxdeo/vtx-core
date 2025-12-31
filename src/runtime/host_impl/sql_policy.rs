use crate::runtime::context::{SecurityPolicy, StreamContext};
use sqlparser::ast::{
    FromTable, ObjectName, Query, SetExpr, Statement, TableFactor, TableWithJoins,
};
use sqlparser::dialect::SQLiteDialect;
use sqlparser::parser::Parser;
use std::collections::HashSet;

pub fn enforce_sql_policy(
    ctx: &StreamContext,
    statement: &str,
    stmt: &rusqlite::Statement<'_>,
) -> Result<(), String> {
    if ctx.policy == SecurityPolicy::Root {
        return Ok(());
    }

    let dialect = SQLiteDialect {};
    let parsed = Parser::parse_sql(&dialect, statement)
        .map_err(|_| "Permission Denied".to_string())?;
    if parsed.len() != 1 {
        return Err("Permission Denied".into());
    }
    let ast = &parsed[0];
    if !is_allowed_statement(ast) {
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

    let mut used_tables = Vec::new();
    collect_tables_from_statement(ast, &mut used_tables)?;
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

fn is_allowed_statement(stmt: &Statement) -> bool {
    matches!(
        stmt,
        Statement::Query(_)
            | Statement::Insert { .. }
            | Statement::Update { .. }
            | Statement::Delete { .. }
    )
}

fn collect_tables_from_statement(
    stmt: &Statement,
    out: &mut Vec<String>,
) -> Result<(), String> {
    match stmt {
        Statement::Query(query) => collect_tables_from_query(query, out),
        Statement::Insert {
            table_name, source, ..
        } => {
            out.push(object_name_to_string(table_name)?);
            if let Some(source) = source {
                collect_tables_from_query(source, out)?;
            }
            Ok(())
        }
        Statement::Update { table, from, .. } => {
            collect_tables_from_table_with_joins(table, out)?;
            if let Some(from) = from {
                collect_tables_from_table_with_joins(from, out)?;
            }
            Ok(())
        }
        Statement::Delete {
            tables,
            from,
            using,
            ..
        } => {
            for table in tables {
                out.push(object_name_to_string(table)?);
            }
            match from {
                FromTable::WithFromKeyword(from) | FromTable::WithoutKeyword(from) => {
                    for table in from {
                        collect_tables_from_table_with_joins(table, out)?;
                    }
                }
            }
            if let Some(using) = using {
                for table in using {
                    collect_tables_from_table_with_joins(table, out)?;
                }
            }
            Ok(())
        }
        _ => Err("Permission Denied".into()),
    }
}

fn collect_tables_from_query(
    query: &Query,
    out: &mut Vec<String>,
) -> Result<(), String> {
    if let Some(with) = &query.with {
        for cte in &with.cte_tables {
            collect_tables_from_query(&cte.query, out)?;
        }
    }
    collect_tables_from_setexpr(&query.body, out)
}

fn collect_tables_from_setexpr(
    expr: &SetExpr,
    out: &mut Vec<String>,
) -> Result<(), String> {
    match expr {
        SetExpr::Select(select) => {
            for table in &select.from {
                collect_tables_from_table_with_joins(table, out)?;
            }
            Ok(())
        }
        SetExpr::SetOperation { left, right, .. } => {
            collect_tables_from_setexpr(left, out)?;
            collect_tables_from_setexpr(right, out)
        }
        SetExpr::Query(query) => collect_tables_from_query(query, out),
        SetExpr::Values(_) => Ok(()),
        _ => Err("Permission Denied".into()),
    }
}

fn collect_tables_from_table_with_joins(
    table: &TableWithJoins,
    out: &mut Vec<String>,
) -> Result<(), String> {
    collect_tables_from_table_factor(&table.relation, out)?;
    for join in &table.joins {
        collect_tables_from_table_factor(&join.relation, out)?;
    }
    Ok(())
}

fn collect_tables_from_table_factor(
    table: &TableFactor,
    out: &mut Vec<String>,
) -> Result<(), String> {
    match table {
        TableFactor::Table { name, .. } => {
            out.push(object_name_to_string(name)?);
            Ok(())
        }
        TableFactor::Derived { subquery, .. } => collect_tables_from_query(subquery, out),
        TableFactor::NestedJoin { table_with_joins, .. } => {
            collect_tables_from_table_with_joins(table_with_joins, out)
        }
        _ => Err("Permission Denied".into()),
    }
}

fn object_name_to_string(name: &ObjectName) -> Result<String, String> {
    if name.0.is_empty() {
        return Err("Permission Denied".into());
    }
    if name.0.len() > 1 {
        return Err("Permission Denied".into());
    }
    Ok(name.0[0].value.clone())
}
