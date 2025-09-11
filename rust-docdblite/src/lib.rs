//! DocDbLite - Embedded document database built on top of SQLite
//! 
//! This library provides a MongoDB-like document database interface
//! backed by SQLite for performance and simplicity.

pub mod collection;
pub mod db_config;
pub mod db_ctx;
pub mod db_value_type;
pub mod doc_db_lite;
pub mod error;
pub mod object_id;

pub use collection::Collection;
pub use db_config::DbConfig;
pub use db_ctx::DbCtx;
pub use doc_db_lite::DocDbLite;
pub use error::{Result, DocDbError};
pub use object_id::ObjectId;
