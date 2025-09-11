//! Main DocDbLite client implementation

use std::collections::HashMap;

use crate::collection::Collection;
use crate::db_config::DbConfig;
use crate::db_ctx::SimpleDbCtx;
use crate::error::Result;
use crate::object_id::ObjectId;

/// The main DocDbLite client
pub struct DocDbLite {
    config: DbConfig,
    system_ctx: SimpleDbCtx,
    collections: HashMap<String, Collection>,
}

impl DocDbLite {
    /// Create a new DocDbLite instance
    pub fn new(config: Option<DbConfig>) -> Result<Self> {
        let config = config.unwrap_or_default();
        let system_ctx = SimpleDbCtx::new(config.clone(), "system".to_string())?;
        
        let db = Self {
            config,
            system_ctx,
            collections: HashMap::new(),
        };

        db.create_collections_table()?;
        Ok(db)
    }

    /// Create the system collections table
    fn create_collections_table(&self) -> Result<()> {
        self.system_ctx.with_connection(|conn| {
            conn.execute(
                "CREATE TABLE IF NOT EXISTS collections (
                    uuid TEXT PRIMARY KEY,
                    name TEXT NOT NULL
                )",
                [],
            )?;
            Ok(())
        })
    }

    /// Add a collection to the database
    pub fn add_collection(&mut self, name: &str) -> Result<&Collection> {
        let name = name.trim().to_lowercase();
        
        // Check if collection already exists in memory
        if self.collections.contains_key(&name) {
            return Ok(self.collections.get(&name).unwrap());
        }

        // Create new collection
        let collection_id = ObjectId::new();
        
        // Register collection in system database
        self.system_ctx.with_connection(|conn| {
            conn.execute(
                "INSERT INTO collections (uuid, name) VALUES (?, ?)",
                rusqlite::params![collection_id.to_string(), name],
            )?;
            Ok(())
        })?;

        // Create the collection
        let collection = Collection::new(self.config.clone(), name.clone())?;
        self.collections.insert(name.clone(), collection);
        
        Ok(self.collections.get(&name).unwrap())
    }

    /// Get a collection by name
    pub fn get_collection(&self, name: &str) -> Option<&Collection> {
        let name = name.trim().to_lowercase();
        self.collections.get(&name)
    }

    /// List all collection names
    pub fn list_collections(&self) -> Result<Vec<String>> {
        self.system_ctx.with_connection(|conn| {
            let mut stmt = conn.prepare("SELECT name FROM collections")?;
            let names: Vec<String> = stmt
                .query_map([], |row| {
                    Ok(row.get::<_, String>(0)?)
                })?
                .collect::<std::result::Result<Vec<_>, _>>()?;
            Ok(names)
        })
    }

    /// Drop a collection
    pub fn drop_collection(&mut self, name: &str) -> Result<bool> {
        let name = name.trim().to_lowercase();
        
        // Remove from system database
        let deleted = self.system_ctx.with_connection(|conn| {
            let count = conn.execute(
                "DELETE FROM collections WHERE name = ?",
                rusqlite::params![name],
            )?;
            Ok(count > 0)
        })?;

        if deleted {
            // Remove from memory
            self.collections.remove(&name);
            
            // TODO: Drop the actual database file
            // This would require additional cleanup logic
        }

        Ok(deleted)
    }

    /// Get database configuration
    pub fn config(&self) -> &DbConfig {
        &self.config
    }
}

impl Default for DocDbLite {
    fn default() -> Self {
        Self::new(None).expect("Failed to create default DocDbLite instance")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tempfile::TempDir;

    #[test]
    fn test_doc_db_lite_creation() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let config = DbConfig::new(temp_dir.path());
        let _db = DocDbLite::new(Some(config))?;
        Ok(())
    }

    #[test]
    fn test_add_collection() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let config = DbConfig::new(temp_dir.path());
        let mut db = DocDbLite::new(Some(config))?;

        let _collection = db.add_collection("test")?;
        
        // Adding the same collection should work
        let _collection2 = db.add_collection("test")?;
        
        // Verify it exists
        assert!(db.get_collection("test").is_some());

        Ok(())
    }

    #[test]
    fn test_list_collections() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let config = DbConfig::new(temp_dir.path());
        let mut db = DocDbLite::new(Some(config))?;

        db.add_collection("test1")?;
        db.add_collection("test2")?;

        let collections = db.list_collections()?;
        assert_eq!(collections.len(), 2);
        assert!(collections.contains(&"test1".to_string()));
        assert!(collections.contains(&"test2".to_string()));

        Ok(())
    }

    #[test]
    fn test_document_operations() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let config = DbConfig::new(temp_dir.path());
        let mut db = DocDbLite::new(Some(config))?;

        let collection = db.add_collection("test")?;
        
        // Insert a document
        let doc = json!({"name": "test", "value": 42});
        let doc_id = collection.insert_one(&doc, None)?;

        // Find the document
        let retrieved = collection.find_one(&doc_id)?;
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap(), doc);

        Ok(())
    }
}