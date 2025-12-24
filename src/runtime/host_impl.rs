use rusqlite::types::ToSql;
use std::io::{Cursor, Read, Seek, SeekFrom};
use wasmtime::component::Resource;

use crate::common::buffer::{BufferType, RealBuffer};
use crate::runtime::context::StreamContext;

// 生成 WIT 绑定
wasmtime::component::bindgen!({
    path: "../vtx-sdk/wit/vtx.wit",
    world: "plugin",
    with: {
        "vtx:api/stream-io/buffer": crate::common::buffer::RealBuffer,
    }
});

pub use vtx::api;

/// 实现插件与宿主之间的 Stream IO 文件读写能力
impl vtx::api::stream_io::Host for StreamContext {
    fn open_file(&mut self, uuid: String) -> Result<Resource<RealBuffer>, String> {
        let file_path = self.registry.get_path(&uuid).ok_or("UUID not found")?;
        let file = std::fs::File::open(&file_path).map_err(|e| e.to_string())?;
        let rb = RealBuffer {
            inner: BufferType::File(file),
        };
        Ok(self.table.push(rb).map_err(|e| e.to_string())?)
    }

    fn create_memory_buffer(&mut self, data: Vec<u8>) -> Resource<RealBuffer> {
        let cursor = Cursor::new(data);
        let rb = RealBuffer {
            inner: BufferType::Memory(cursor),
        };
        self.table
            .push(rb)
            .expect("Failed to allocate buffer resource")
    }
}

/// 实现 buffer 的读取、大小获取与销毁
impl vtx::api::stream_io::HostBuffer for StreamContext {
    fn size(&mut self, resource: Resource<RealBuffer>) -> u64 {
        let buffer = self.table.get(&resource).expect("Invalid buffer handle");
        match &buffer.inner {
            BufferType::File(f) => f.metadata().map(|m| m.len()).unwrap_or(0),
            BufferType::Memory(c) => c.get_ref().len() as u64,
        }
    }

    fn read(&mut self, resource: Resource<RealBuffer>, offset: u64, max_bytes: u64) -> Vec<u8> {
        let buffer = self
            .table
            .get_mut(&resource)
            .expect("Invalid buffer handle");
        let mut chunk = vec![0u8; max_bytes as usize];

        let read_len = match &mut buffer.inner {
            BufferType::File(f) => {
                f.seek(SeekFrom::Start(offset)).ok();
                f.read(&mut chunk).unwrap_or(0)
            }
            BufferType::Memory(c) => {
                c.seek(SeekFrom::Start(offset)).ok();
                c.read(&mut chunk).unwrap_or(0)
            }
        };

        chunk.truncate(read_len);
        chunk
    }

    fn drop(&mut self, resource: Resource<RealBuffer>) -> wasmtime::Result<()> {
        self.table.delete(resource)?;
        Ok(())
    }
}

/// 实现 SQL 执行接口：支持参数绑定与 JSON 查询
impl vtx::api::sql::Host for StreamContext {
    fn execute(
        &mut self,
        statement: String,
        params: Vec<vtx::api::sql::DbValue>,
    ) -> Result<u64, String> {
        let conn = self.registry.get_conn().map_err(|e| e.to_string())?;

        let sql_params: Vec<Box<dyn ToSql>> = params
            .iter()
            .map(|p| match p {
                api::sql::DbValue::Text(s) => Box::new(s.clone()) as Box<dyn ToSql>,
                api::sql::DbValue::Integer(i) => Box::new(*i),
                api::sql::DbValue::Real(f) => Box::new(*f),
                api::sql::DbValue::NullVal => Box::new(rusqlite::types::Null),
            })
            .collect();
        let param_refs: Vec<&dyn ToSql> = sql_params.iter().map(|b| b.as_ref()).collect();

        conn.execute(&statement, param_refs.as_slice())
            .map(|rows| rows as u64)
            .map_err(|e| format!("SQL Execution Error: {}", e))
    }

    fn query_json(
        &mut self,
        statement: String,
        params: Vec<vtx::api::sql::DbValue>,
    ) -> Result<String, String> {
        let conn = self.registry.get_conn().map_err(|e| e.to_string())?;

        let sql_params: Vec<Box<dyn ToSql>> = params
            .iter()
            .map(|p| match p {
                api::sql::DbValue::Text(s) => Box::new(s.clone()) as Box<dyn ToSql>,
                api::sql::DbValue::Integer(i) => Box::new(*i),
                api::sql::DbValue::Real(f) => Box::new(*f),
                api::sql::DbValue::NullVal => Box::new(rusqlite::types::Null),
            })
            .collect();
        let param_refs: Vec<&dyn ToSql> = sql_params.iter().map(|b| b.as_ref()).collect();

        let mut stmt = conn.prepare(&statement).map_err(|e| e.to_string())?;
        let col_names: Vec<String> = stmt.column_names().iter().map(|s| s.to_string()).collect();

        let mut rows_json = Vec::new();
        let mut rows = stmt
            .query(param_refs.as_slice())
            .map_err(|e| e.to_string())?;

        // 数据量边界：该查询结果会被序列化为 JSON 字符串返回内存中，
        // 大量数据可能导致 OOM，插件端应负责使用 LIMIT 分页。
        while let Some(row) = rows.next().map_err(|e| e.to_string())? {
            let mut obj = serde_json::Map::new();
            for (i, col_name) in col_names.iter().enumerate() {
                let val_ref = row.get_ref(i).unwrap();
                let json_val = match val_ref {
                    rusqlite::types::ValueRef::Null => serde_json::Value::Null,
                    rusqlite::types::ValueRef::Integer(n) => serde_json::json!(n),
                    rusqlite::types::ValueRef::Real(f) => serde_json::json!(f),
                    rusqlite::types::ValueRef::Text(t) => {
                        serde_json::json!(String::from_utf8_lossy(t))
                    }
                    rusqlite::types::ValueRef::Blob(_) => {
                        serde_json::Value::String("<BLOB>".into())
                    }
                };
                obj.insert(col_name.clone(), json_val);
            }
            rows_json.push(serde_json::Value::Object(obj));
        }

        Ok(serde_json::to_string(&rows_json).unwrap())
    }
}
