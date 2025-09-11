//! Database configuration

use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// Configuration for the document database
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DbConfig {
    /// Directory where database files are stored
    pub dir: PathBuf,
    
    /// Maximum number of nesting levels for JSON documents
    pub max_nesting_levels: usize,
    
    /// Connection pool size for each database
    pub connection_pool_size: usize,
    
    /// Number of prepared statements to cache
    pub cached_statements: usize,
    
    /// Database busy timeout in milliseconds
    pub timeout_ms: u32,
}

impl DbConfig {
    /// Create a new configuration with the specified directory
    pub fn new<P: Into<PathBuf>>(dir: P) -> Self {
        Self {
            dir: dir.into(),
            max_nesting_levels: 100,
            connection_pool_size: 10,
            cached_statements: 128,
            timeout_ms: 5000,
        }
    }
    
    /// Create configuration with default directory (~/.docdblite)
    pub fn with_default_dir() -> Self {
        let default_dir = dirs::home_dir()
            .map(|mut p| {
                p.push(".docdblite");
                p
            })
            .unwrap_or_else(|| PathBuf::from(".docdblite"));
            
        Self::new(default_dir)
    }
    
    /// Set the maximum nesting levels
    pub fn with_max_nesting_levels(mut self, levels: usize) -> Self {
        self.max_nesting_levels = levels;
        self
    }
    
    /// Set the connection pool size
    pub fn with_connection_pool_size(mut self, size: usize) -> Self {
        self.connection_pool_size = size;
        self
    }
    
    /// Set the number of cached statements
    pub fn with_cached_statements(mut self, count: usize) -> Self {
        self.cached_statements = count;
        self
    }
    
    /// Set the timeout in milliseconds
    pub fn with_timeout_ms(mut self, timeout: u32) -> Self {
        self.timeout_ms = timeout;
        self
    }
}

impl Default for DbConfig {
    fn default() -> Self {
        Self::with_default_dir()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_config() {
        let config = DbConfig::new("/tmp/test");
        assert_eq!(config.dir, PathBuf::from("/tmp/test"));
        assert_eq!(config.max_nesting_levels, 100);
        assert_eq!(config.connection_pool_size, 10);
        assert_eq!(config.cached_statements, 128);
        assert_eq!(config.timeout_ms, 5000);
    }

    #[test]
    fn test_default_config() {
        let config = DbConfig::default();
        assert!(config.dir.to_string_lossy().contains(".docdblite"));
    }

    #[test]
    fn test_builder_pattern() {
        let config = DbConfig::new("/tmp/test")
            .with_max_nesting_levels(50)
            .with_connection_pool_size(5)
            .with_cached_statements(64)
            .with_timeout_ms(3000);
            
        assert_eq!(config.max_nesting_levels, 50);
        assert_eq!(config.connection_pool_size, 5);
        assert_eq!(config.cached_statements, 64);
        assert_eq!(config.timeout_ms, 3000);
    }
}