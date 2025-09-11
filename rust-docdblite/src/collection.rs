//! Collection implementation for document operations

use rusqlite::{params, Connection};
use serde_json::{Map, Value};
use std::collections::HashMap;

use crate::db_config::DbConfig;
use crate::db_ctx::SimpleDbCtx;
use crate::db_value_type::DbValueType;
use crate::error::{DocDbError, Result};
use crate::object_id::ObjectId;

/// A collection of documents
pub struct Collection {
    ctx: SimpleDbCtx,
    pub name: String,
    documents_table: String,
    data_table: String,
}

impl Collection {
    /// Create a new collection
    pub fn new(config: DbConfig, name: String) -> Result<Self> {
        let name = name.trim().to_lowercase();
        let ctx = SimpleDbCtx::new(config, name.clone())?;
        let documents_table = name.clone();
        let data_table = format!("{}_data", name);

        let collection = Self {
            ctx,
            name,
            documents_table,
            data_table,
        };

        // Create tables
        collection.create_tables()?;

        Ok(collection)
    }

    /// Create the necessary database tables
    fn create_tables(&self) -> Result<()> {
        self.ctx.with_connection(|conn| {
            // Create documents table
            conn.execute(
                &format!(
                    "CREATE TABLE IF NOT EXISTS {} (
                        uuid TEXT(36) PRIMARY KEY
                    )",
                    self.documents_table
                ),
                [],
            )?;

            // Create document data table
            conn.execute(
                &format!(
                    "CREATE TABLE IF NOT EXISTS {} (
                        uuid TEXT(36) PRIMARY KEY,
                        doc_id TEXT(36) NOT NULL,
                        parent_uuid TEXT(36),
                        key TEXT NOT NULL,
                        type INTEGER NOT NULL,
                        value
                    )",
                    self.data_table
                ),
                [],
            )?;

            Ok(())
        })
    }

    /// Insert a document into the collection
    pub fn insert_one(&self, document: &Value, uuid: Option<ObjectId>) -> Result<ObjectId> {
        let doc_id = uuid.unwrap_or_else(ObjectId::new);
        
        self.ctx.with_connection(|conn| {
            let tx = conn.transaction()?;
            
            // Insert document ID
            tx.execute(
                &format!("INSERT INTO {} (uuid) VALUES (?)", self.documents_table),
                params![doc_id.to_string()],
            )?;

            // Recursively insert document data
            self.insert_json_recursive(&tx, document, &doc_id, None)?;

            tx.commit()?;
            Ok(doc_id)
        })
    }

    /// Recursively insert JSON data
    fn insert_json_recursive(
        &self,
        conn: &Connection,
        node: &Value,
        doc_id: &ObjectId,
        parent_uuid: Option<&ObjectId>,
    ) -> Result<()> {
        match node {
            Value::Object(obj) => {
                for (key, value) in obj {
                    self.insert_key_value(conn, key, value, doc_id, parent_uuid)?;
                }
            }
            Value::Array(arr) => {
                for (index, value) in arr.iter().enumerate() {
                    let key = index.to_string();
                    self.insert_key_value(conn, &key, value, doc_id, parent_uuid)?;
                }
            }
            _ => {
                return Err(DocDbError::TypeConversion(
                    "Root node must be object or array".to_string(),
                ));
            }
        }
        Ok(())
    }

    /// Insert a key-value pair
    fn insert_key_value(
        &self,
        conn: &Connection,
        key: &str,
        value: &Value,
        doc_id: &ObjectId,
        parent_uuid: Option<&ObjectId>,
    ) -> Result<()> {
        let child_uuid = ObjectId::new();
        let value_type = DbValueType::from_json_value(value);
        let db_value = self.map_json_value_to_db_value(value, value_type)?;

        conn.execute(
            &format!(
                "INSERT INTO {} (uuid, doc_id, parent_uuid, key, type, value) VALUES (?, ?, ?, ?, ?, ?)",
                self.data_table
            ),
            params![
                child_uuid.to_string(),
                doc_id.to_string(),
                parent_uuid.map(|u| u.to_string()),
                key,
                value_type.as_i32(),
                db_value
            ],
        )?;

        // If it's a container type, recurse
        if matches!(value_type, DbValueType::Object | DbValueType::Array) {
            self.insert_json_recursive(conn, value, doc_id, Some(&child_uuid))?;
        }

        Ok(())
    }

    /// Map a JSON value to a database value
    fn map_json_value_to_db_value(&self, value: &Value, value_type: DbValueType) -> Result<Option<String>> {
        match value_type {
            DbValueType::String => Ok(Some(value.as_str().unwrap().to_string())),
            DbValueType::Integer => Ok(Some(value.as_i64().unwrap().to_string())),
            DbValueType::Float => Ok(Some(value.as_f64().unwrap().to_string())),
            DbValueType::Boolean => Ok(Some(value.as_bool().unwrap().to_string())),
            DbValueType::Null => Ok(None),
            DbValueType::Object | DbValueType::Array => Ok(None), // Container types have no direct value
            DbValueType::DateTime => Err(DocDbError::TypeConversion(
                "DateTime not yet supported".to_string(),
            )),
        }
    }

    /// Find a document by its ID
    pub fn find_one(&self, uuid: &ObjectId) -> Result<Option<Value>> {
        self.ctx.with_connection(|conn| {
            let mut stmt = conn.prepare(&format!(
                "SELECT uuid, doc_id, parent_uuid, key, type, value FROM {} WHERE doc_id = ?",
                self.data_table
            ))?;

            let rows: Vec<(String, String, Option<String>, String, i32, Option<String>)> = stmt
                .query_map(params![uuid.to_string()], |row| {
                    Ok((
                        row.get(0)?, // uuid
                        row.get(1)?, // doc_id
                        row.get(2)?, // parent_uuid
                        row.get(3)?, // key
                        row.get(4)?, // type
                        row.get(5)?, // value
                    ))
                })?
                .collect::<std::result::Result<Vec<_>, _>>()?;

            if rows.is_empty() {
                return Ok(None);
            }

            let document = self.reconstruct_document(&rows)?;
            Ok(Some(document))
        })
    }

    /// Reconstruct a document from database rows
    fn reconstruct_document(&self, rows: &[(String, String, Option<String>, String, i32, Option<String>)]) -> Result<Value> {
        // Build a map of UUID to node info for easier lookup
        let mut nodes: HashMap<String, (String, Option<String>, String, DbValueType, Option<String>)> = HashMap::new();
        
        for row in rows {
            let (uuid, _doc_id, parent_uuid, key, type_val, value) = row;
            let value_type = DbValueType::from_i32(*type_val)
                .ok_or_else(|| DocDbError::TypeConversion(format!("Invalid type: {}", type_val)))?;
            
            nodes.insert(uuid.clone(), (uuid.clone(), parent_uuid.clone(), key.clone(), value_type, value.clone()));
        }

        // Find root nodes (those with no parent)
        let mut root_nodes = Vec::new();
        for (uuid, (_uuid, parent_uuid, key, value_type, value)) in &nodes {
            if parent_uuid.is_none() {
                root_nodes.push((uuid.clone(), key.clone(), *value_type, value.clone()));
            }
        }

        // Build the document recursively
        let mut result = Value::Object(Map::new());
        
        for (uuid, key, _value_type, _value) in root_nodes {
            let node_value = self.build_node_recursive(&uuid, &nodes)?;
            if let Value::Object(ref mut obj) = result {
                obj.insert(key, node_value);
            }
        }

        Ok(result)
    }

    /// Recursively build a node and its children
    fn build_node_recursive(
        &self,
        uuid: &str,
        nodes: &HashMap<String, (String, Option<String>, String, DbValueType, Option<String>)>
    ) -> Result<Value> {
        if let Some((_, _, _, value_type, value)) = nodes.get(uuid) {
            match value_type {
                DbValueType::Object => {
                    let mut obj = Map::new();
                    
                    // Find children of this object
                    for (child_uuid, (_, parent_uuid, key, _, _)) in nodes {
                        if let Some(parent) = parent_uuid {
                            if parent == uuid {
                                let child_value = self.build_node_recursive(child_uuid, nodes)?;
                                obj.insert(key.clone(), child_value);
                            }
                        }
                    }
                    
                    Ok(Value::Object(obj))
                }
                DbValueType::Array => {
                    let mut arr_map: HashMap<usize, Value> = HashMap::new();
                    
                    // Find children of this array
                    for (child_uuid, (_, parent_uuid, key, _, _)) in nodes {
                        if let Some(parent) = parent_uuid {
                            if parent == uuid {
                                let index: usize = key.parse()
                                    .map_err(|_| DocDbError::TypeConversion(format!("Invalid array index: {}", key)))?;
                                let child_value = self.build_node_recursive(child_uuid, nodes)?;
                                arr_map.insert(index, child_value);
                            }
                        }
                    }
                    
                    // Convert HashMap to Vec, preserving order
                    let mut max_index = 0;
                    for &index in arr_map.keys() {
                        if index > max_index {
                            max_index = index;
                        }
                    }
                    
                    let mut arr = vec![Value::Null; max_index + 1];
                    for (index, value) in arr_map {
                        arr[index] = value;
                    }
                    
                    Ok(Value::Array(arr))
                }
                _ => {
                    // Leaf value
                    self.convert_db_value_to_json(value, *value_type)
                }
            }
        } else {
            Err(DocDbError::TypeConversion(format!("Node not found: {}", uuid)))
        }
    }

    /// Convert database value back to JSON value
    fn convert_db_value_to_json(&self, value: &Option<String>, value_type: DbValueType) -> Result<Value> {
        match value_type {
            DbValueType::Null => Ok(Value::Null),
            DbValueType::String => Ok(Value::String(value.as_ref().unwrap().clone())),
            DbValueType::Integer => {
                let int_val: i64 = value.as_ref().unwrap().parse()
                    .map_err(|_| DocDbError::TypeConversion("Invalid integer".to_string()))?;
                Ok(Value::Number(int_val.into()))
            }
            DbValueType::Float => {
                let float_val: f64 = value.as_ref().unwrap().parse()
                    .map_err(|_| DocDbError::TypeConversion("Invalid float".to_string()))?;
                Ok(Value::Number(serde_json::Number::from_f64(float_val)
                    .ok_or_else(|| DocDbError::TypeConversion("Invalid float value".to_string()))?))
            }
            DbValueType::Boolean => {
                let bool_val: bool = value.as_ref().unwrap().parse()
                    .map_err(|_| DocDbError::TypeConversion("Invalid boolean".to_string()))?;
                Ok(Value::Bool(bool_val))
            }
            DbValueType::Object => Ok(Value::Object(Map::new())),
            DbValueType::Array => Ok(Value::Array(vec![])),
            DbValueType::DateTime => Err(DocDbError::TypeConversion(
                "DateTime not yet supported".to_string(),
            )),
        }
    }

    /// Count documents matching a filter
    pub fn count_documents(&self, filter: &HashMap<String, Value>) -> Result<i64> {
        let where_clause = self.build_where_clause(filter)?;
        
        self.ctx.with_connection(|conn| {
            let sql = format!("SELECT DISTINCT COUNT(doc_id) FROM {} WHERE {}", self.data_table, where_clause);
            let count: i64 = conn.query_row(&sql, [], |row| row.get(0))?;
            Ok(count)
        })
    }

    /// Delete a document matching the filter
    pub fn delete_one(&self, filter: &HashMap<String, Value>) -> Result<()> {
        let where_clause = self.build_where_clause(filter)?;
        
        self.ctx.with_connection(|conn| {
            let tx = conn.transaction()?;
            
            // Get the document ID
            let sql = format!("SELECT doc_id FROM {} WHERE {}", self.data_table, where_clause);
            let doc_id: String = tx.query_row(&sql, [], |row| row.get(0))?;

            // Delete from data table
            tx.execute(
                &format!("DELETE FROM {} WHERE doc_id = ?", self.data_table),
                params![doc_id],
            )?;

            // Delete from documents table
            tx.execute(
                &format!("DELETE FROM {} WHERE uuid = ?", self.documents_table),
                params![doc_id],
            )?;

            tx.commit()?;
            Ok(())
        })
    }

    /// Build WHERE clause from filter
    fn build_where_clause(&self, filter: &HashMap<String, Value>) -> Result<String> {
        if filter.is_empty() {
            return Err(DocDbError::InvalidFilter("Empty filter not allowed".to_string()));
        }

        let mut clauses = Vec::new();
        
        for (key, value) in filter {
            let actual_key = if key == "_id" { "doc_id" } else { key };
            
            match value {
                Value::String(s) => {
                    clauses.push(format!("key='{}' AND value='{}'", actual_key, s));
                }
                Value::Number(n) => {
                    clauses.push(format!("key='{}' AND value='{}'", actual_key, n));
                }
                Value::Bool(b) => {
                    clauses.push(format!("key='{}' AND value='{}'", actual_key, b));
                }
                _ => {
                    return Err(DocDbError::InvalidFilter(format!(
                        "Unsupported filter value type for key: {}", key
                    )));
                }
            }
        }

        Ok(clauses.join(" AND "))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tempfile::TempDir;

    #[test]
    fn test_collection_operations() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let config = DbConfig::new(temp_dir.path());
        let collection = Collection::new(config, "test".to_string())?;

        // Test insert and find
        let doc = json!({"key": "value", "number": 42});
        let doc_id = collection.insert_one(&doc, None)?;
        
        let retrieved = collection.find_one(&doc_id)?;
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap(), doc);

        Ok(())
    }

    #[test]
    fn test_complex_document() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let config = DbConfig::new(temp_dir.path());
        let collection = Collection::new(config, "test".to_string())?;

        let doc = json!({
            "name": "test",
            "nested": {
                "array": [1, 2, 3],
                "object": {
                    "value": true
                }
            }
        });

        let doc_id = collection.insert_one(&doc, None)?;
        let retrieved = collection.find_one(&doc_id)?;
        
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap(), doc);

        Ok(())
    }
}