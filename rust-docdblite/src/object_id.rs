//! ObjectId implementation using UUIDv7

use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;
use uuid::Uuid;

use crate::error::{DocDbError, Result};

/// A UUIDv7-based object identifier
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ObjectId {
    uuid: Uuid,
}

impl ObjectId {
    /// Create a new ObjectId with a generated UUIDv7
    pub fn new() -> Self {
        Self {
            uuid: Uuid::now_v7(),
        }
    }

    /// Create an ObjectId from an existing UUID string
    pub fn from_str(uuid_str: &str) -> Result<Self> {
        let uuid = Uuid::from_str(uuid_str)?;
        
        // Verify it's a v7 UUID
        if uuid.get_version() != Some(uuid::Version::SortRand) {
            return Err(DocDbError::General("Invalid UUID version: expected v7".to_string()));
        }
        
        Ok(Self { uuid })
    }

    /// Create an ObjectId from a UUID
    pub fn from_uuid(uuid: Uuid) -> Result<Self> {
        // Verify it's a v7 UUID
        if uuid.get_version() != Some(uuid::Version::SortRand) {
            return Err(DocDbError::General("Invalid UUID version: expected v7".to_string()));
        }
        
        Ok(Self { uuid })
    }

    /// Get the inner UUID
    pub fn as_uuid(&self) -> &Uuid {
        &self.uuid
    }

    /// Convert to string representation
    pub fn to_string(&self) -> String {
        self.uuid.to_string()
    }

    /// Check if a string is a valid UUIDv7
    pub fn is_valid_uuid7(uuid_str: &str) -> bool {
        match Uuid::from_str(uuid_str) {
            Ok(uuid) => uuid.get_version() == Some(uuid::Version::SortRand),
            Err(_) => false,
        }
    }
}

impl Default for ObjectId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for ObjectId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.uuid)
    }
}

impl FromStr for ObjectId {
    type Err = DocDbError;

    fn from_str(s: &str) -> Result<Self> {
        Self::from_str(s)
    }
}

impl From<ObjectId> for String {
    fn from(object_id: ObjectId) -> Self {
        object_id.to_string()
    }
}

impl From<&ObjectId> for String {
    fn from(object_id: &ObjectId) -> Self {
        object_id.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_object_id() {
        let id1 = ObjectId::new();
        let id2 = ObjectId::new();
        
        // They should be different
        assert_ne!(id1, id2);
        
        // They should be valid UUIDv7s
        assert_eq!(id1.uuid.get_version(), Some(uuid::Version::SortRand));
        assert_eq!(id2.uuid.get_version(), Some(uuid::Version::SortRand));
    }

    #[test]
    fn test_from_string() {
        let id = ObjectId::new();
        let id_str = id.to_string();
        let parsed_id = ObjectId::from_str(&id_str).unwrap();
        
        assert_eq!(id, parsed_id);
    }

    #[test]
    fn test_invalid_uuid() {
        // Invalid UUID string
        assert!(ObjectId::from_str("invalid-uuid").is_err());
        
        // Valid UUID but not v7
        let v4_uuid = Uuid::new_v4();
        assert!(ObjectId::from_uuid(v4_uuid).is_err());
    }

    #[test]
    fn test_is_valid_uuid7() {
        let valid_id = ObjectId::new();
        let valid_str = valid_id.to_string();
        
        assert!(ObjectId::is_valid_uuid7(&valid_str));
        assert!(!ObjectId::is_valid_uuid7("invalid-uuid"));
        
        let v4_uuid = Uuid::new_v4().to_string();
        assert!(!ObjectId::is_valid_uuid7(&v4_uuid));
    }

    #[test]
    fn test_display() {
        let id = ObjectId::new();
        let displayed = format!("{}", id);
        let to_string = id.to_string();
        
        assert_eq!(displayed, to_string);
    }
}