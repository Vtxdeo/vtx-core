use super::api;
use crate::runtime::context::{SecurityPolicy, StreamContext};
use crate::runtime::host_impl::sql_policy::enforce_sql_policy;
use rusqlite::types::ToSql;
use serde_json::{Map, Value};

#[async_trait::async_trait]
impl api::sql::Host for StreamContext {
    async fn execute(
        &mut self,
        statement: String,
        params: Vec<api::sql::DbValue>,
    ) -> Result<u64, String> {
        if self.policy == SecurityPolicy::Plugin
            && !self.permissions.iter().any(|p| p == "sql:write")
        {
            return Err("Permission Denied".into());
        }
        let conn = self.registry.get_conn().map_err(|e| e.to_string())?;

        let sql_params = convert_params(&params);
        let param_refs: Vec<&dyn ToSql> = sql_params.iter().map(|b| b.as_ref()).collect();

        {
            let stmt = conn.prepare(&statement).map_err(|e| e.to_string())?;
            enforce_sql_policy(self, &statement, &stmt)?;
        }

        conn.execute(&statement, param_refs.as_slice())
            .map(|rows| rows as u64)
            .map_err(|e| format!("SQL Error: {}", e))
    }

    async fn query_json(
        &mut self,
        statement: String,
        params: Vec<api::sql::DbValue>,
    ) -> Result<String, String> {
        let conn = self.registry.get_conn().map_err(|e| e.to_string())?;

        let sql_params = convert_params(&params);
        let param_refs: Vec<&dyn ToSql> = sql_params.iter().map(|b| b.as_ref()).collect();

        let mut stmt = conn.prepare(&statement).map_err(|e| e.to_string())?;
        enforce_sql_policy(self, &statement, &stmt)?;

        let col_names: Vec<String> = stmt.column_names().iter().map(|s| s.to_string()).collect();
        let mut rows = stmt
            .query(param_refs.as_slice())
            .map_err(|e| e.to_string())?;

        let mut rows_json = Vec::new();
        while let Ok(Some(row)) = rows.next() {
            let mut obj = Map::new();
            for (i, col_name) in col_names.iter().enumerate() {
                let json_val = match row.get_ref(i).unwrap() {
                    rusqlite::types::ValueRef::Null => Value::Null,
                    rusqlite::types::ValueRef::Integer(n) => Value::Number(n.into()),
                    rusqlite::types::ValueRef::Real(f) => serde_json::Number::from_f64(f)
                        .map(Value::Number)
                        .unwrap_or(Value::Null),
                    rusqlite::types::ValueRef::Text(t) => {
                        Value::String(String::from_utf8_lossy(t).into_owned())
                    }
                    rusqlite::types::ValueRef::Blob(_) => Value::String("<BLOB>".into()),
                };
                obj.insert(col_name.clone(), json_val);
            }
            rows_json.push(Value::Object(obj));
        }

        serde_json::to_string(&rows_json).map_err(|e| e.to_string())
    }
}

/// 工具函数：将插件传入的参数类型转换为 rusqlite 支持的 ToSql trait 对象
fn convert_params(params: &[api::sql::DbValue]) -> Vec<Box<dyn ToSql>> {
    params
        .iter()
        .map(|p| match p {
            api::sql::DbValue::Text(s) => Box::new(s.clone()) as Box<dyn ToSql>,
            api::sql::DbValue::Integer(i) => Box::new(*i),
            api::sql::DbValue::Real(f) => Box::new(*f),
            api::sql::DbValue::NullVal => Box::new(rusqlite::types::Null),
        })
        .collect()
}
