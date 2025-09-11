//! Database context management with connection pooling

use rusqlite::{Connection, OpenFlags};
use std::collections::VecDeque;
use std::path::Path;
use std::sync::{Arc, Mutex, Condvar};
use std::time::Duration;

use crate::db_config::DbConfig;
use crate::error::Result;

/// A database context that manages SQLite connections with pooling
pub struct DbCtx {
    pool: Arc<ConnectionPool>,
}

/// Internal connection pool implementation
struct ConnectionPool {
    config: DbConfig,
    database_name: String,
    connections: Mutex<VecDeque<Connection>>,
    condvar: Condvar,
}

/// A connection guard that returns the connection to the pool when dropped
pub struct ConnectionGuard {
    connection: Option<Connection>,
    pool: Arc<ConnectionPool>,
}

impl DbCtx {
    /// Create a new database context
    pub fn new(config: DbConfig, database_name: String) -> Result<Self> {
        let pool = Arc::new(ConnectionPool::new(config, database_name)?);
        Ok(Self { pool })
    }

    /// Get a connection from the pool
    pub fn get_connection(&self) -> Result<ConnectionGuard> {
        self.pool.get_connection()
    }
}

impl ConnectionPool {
    fn new(config: DbConfig, database_name: String) -> Result<Self> {
        // Ensure directory exists
        std::fs::create_dir_all(&config.dir)?;
        
        // Create the database file path
        let db_path = config.dir.join(format!("{}.sqlite", database_name));
        
        let pool = Self {
            config: config.clone(),
            database_name,
            connections: Mutex::new(VecDeque::new()),
            condvar: Condvar::new(),
        };

        // Initialize the connection pool
        let mut connections = pool.connections.lock().unwrap();
        for _ in 0..config.connection_pool_size {
            let conn = pool.create_connection(&db_path)?;
            connections.push_back(conn);
        }
        drop(connections);

        Ok(pool)
    }

    fn create_connection(&self, db_path: &Path) -> Result<Connection> {
        let conn = Connection::open_with_flags(
            db_path,
            OpenFlags::SQLITE_OPEN_READ_WRITE 
                | OpenFlags::SQLITE_OPEN_CREATE 
                | OpenFlags::SQLITE_OPEN_URI 
                | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )?;

        // Set timeout
        conn.busy_timeout(Duration::from_millis(self.config.timeout_ms as u64))?;

        // Enable WAL mode for better concurrency (this returns the new mode)
        let _: String = conn.query_row("PRAGMA journal_mode=WAL", [], |row| row.get(0))?;
        
        // Set other pragmas - these don't return values when setting
        conn.execute("PRAGMA synchronous=NORMAL", [])?;
        conn.execute("PRAGMA temp_store=MEMORY", [])?;
        conn.execute(&format!("PRAGMA cache_size={}", self.config.cached_statements), [])?;

        Ok(conn)
    }

    fn get_connection(&self) -> Result<ConnectionGuard> {
        let mut connections = self.connections.lock().unwrap();
        
        // Wait for a connection to become available
        while connections.is_empty() {
            connections = self.condvar.wait(connections).unwrap();
        }
        
        let connection = connections.pop_front().unwrap();
        
        Ok(ConnectionGuard {
            connection: Some(connection),
            pool: Arc::new(Self {
                config: self.config.clone(),
                database_name: self.database_name.clone(),
                connections: Mutex::new(VecDeque::new()), // This is a workaround
                condvar: Condvar::new(),
            }),
        })
    }

    fn return_connection(&self, connection: Connection) {
        let mut connections = self.connections.lock().unwrap();
        connections.push_back(connection);
        self.condvar.notify_one();
    }
}

impl ConnectionGuard {
    /// Get a reference to the connection
    pub fn connection(&self) -> &Connection {
        self.connection.as_ref().unwrap()
    }

    /// Get a mutable reference to the connection
    pub fn connection_mut(&mut self) -> &mut Connection {
        self.connection.as_mut().unwrap()
    }
}

impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        if let Some(connection) = self.connection.take() {
            self.pool.return_connection(connection);
        }
    }
}

/// Simplified connection pool for easier use
/// This is a simpler implementation that doesn't require the complex pooling
#[derive(Clone)]
pub struct SimpleDbCtx {
    config: DbConfig,
    database_name: String,
}

impl SimpleDbCtx {
    /// Create a new simple database context
    pub fn new(config: DbConfig, database_name: String) -> Result<Self> {
        // Ensure directory exists
        std::fs::create_dir_all(&config.dir)?;
        
        Ok(Self {
            config,
            database_name,
        })
    }

    /// Execute a function with a database connection
    pub fn with_connection<F, R>(&self, f: F) -> Result<R>
    where
        F: FnOnce(&mut Connection) -> Result<R>,
    {
        let db_path = self.config.dir.join(format!("{}.sqlite", self.database_name));
        
        let mut conn = Connection::open_with_flags(
            db_path,
            OpenFlags::SQLITE_OPEN_READ_WRITE 
                | OpenFlags::SQLITE_OPEN_CREATE 
                | OpenFlags::SQLITE_OPEN_URI,
        )?;

        // Set timeout
        conn.busy_timeout(Duration::from_millis(self.config.timeout_ms as u64))?;

        // Enable WAL mode for better concurrency (this returns the new mode)
        let _: String = conn.query_row("PRAGMA journal_mode=WAL", [], |row| row.get(0))?;
        
        // Set other pragmas - these don't return values when setting
        conn.execute("PRAGMA synchronous=NORMAL", [])?;
        conn.execute("PRAGMA temp_store=MEMORY", [])?;
        conn.execute(&format!("PRAGMA cache_size={}", self.config.cached_statements), [])?;

        f(&mut conn)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_simple_db_ctx() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let config = DbConfig::new(temp_dir.path());
        let ctx = SimpleDbCtx::new(config, "test".to_string())?;

        ctx.with_connection(|conn| {
            conn.execute("CREATE TABLE test (id INTEGER PRIMARY KEY)", [])?;
            conn.execute("INSERT INTO test DEFAULT VALUES", [])?;
            
            let count: i64 = conn.query_row("SELECT COUNT(*) FROM test", [], |row| {
                row.get(0)
            })?;
            
            assert_eq!(count, 1);
            Ok(())
        })?;

        Ok(())
    }
}