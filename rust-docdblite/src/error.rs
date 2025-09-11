//! Error types for DocDbLite

use thiserror::Error;

/// Result type for DocDbLite operations
pub type Result<T> = std::result::Result<T, DocDbError>;

/// Error types that can occur in DocDbLite operations
#[derive(Error, Debug)]
pub enum DocDbError {
    /// Database error from SQLite
    #[error("Database error: {0}")]
    Database(#[from] rusqlite::Error),
    
    /// JSON serialization/deserialization error
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
    
    /// Invalid UUID error
    #[error("Invalid UUID: {0}")]
    InvalidUuid(#[from] uuid::Error),
    
    /// I/O error
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    
    /// Document not found
    #[error("Document not found with ID: {0}")]
    DocumentNotFound(String),
    
    /// Collection not found
    #[error("Collection not found: {0}")]
    CollectionNotFound(String),
    
    /// Invalid filter format
    #[error("Invalid filter: {0}")]
    InvalidFilter(String),
    
    /// Type conversion error
    #[error("Type conversion error: {0}")]
    TypeConversion(String),
    
    /// General error
    #[error("Error: {0}")]
    General(String),
}