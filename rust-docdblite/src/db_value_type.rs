//! Database value type enumeration

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Enum representing the types of values that can be stored in the database
/// Values match the Python implementation for compatibility
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(i32)]
pub enum DbValueType {
    /// Null value
    Null = 5,
    /// JSON object (dict in Python)
    Object = 10,
    /// JSON array (list in Python)  
    Array = 15,
    /// String value
    String = 20,
    /// Integer value
    Integer = 25,
    /// Float value
    Float = 30,
    /// Boolean value
    Boolean = 35,
    /// DateTime value (not yet implemented)
    DateTime = 40,
}

impl DbValueType {
    /// Get the type of a JSON value
    pub fn from_json_value(value: &Value) -> Self {
        match value {
            Value::Null => DbValueType::Null,
            Value::Bool(_) => DbValueType::Boolean,
            Value::Number(n) => {
                if n.is_i64() || n.is_u64() {
                    DbValueType::Integer
                } else {
                    DbValueType::Float
                }
            }
            Value::String(_) => DbValueType::String,
            Value::Array(_) => DbValueType::Array,
            Value::Object(_) => DbValueType::Object,
        }
    }
    
    /// Convert to integer representation for database storage
    pub fn as_i32(self) -> i32 {
        self as i32
    }
    
    /// Create from integer representation from database
    pub fn from_i32(value: i32) -> Option<Self> {
        match value {
            5 => Some(DbValueType::Null),
            10 => Some(DbValueType::Object),
            15 => Some(DbValueType::Array),
            20 => Some(DbValueType::String),
            25 => Some(DbValueType::Integer),
            30 => Some(DbValueType::Float),
            35 => Some(DbValueType::Boolean),
            40 => Some(DbValueType::DateTime),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_from_json_value() {
        assert_eq!(DbValueType::from_json_value(&json!(null)), DbValueType::Null);
        assert_eq!(DbValueType::from_json_value(&json!(true)), DbValueType::Boolean);
        assert_eq!(DbValueType::from_json_value(&json!(42)), DbValueType::Integer);
        assert_eq!(DbValueType::from_json_value(&json!(3.14)), DbValueType::Float);
        assert_eq!(DbValueType::from_json_value(&json!("hello")), DbValueType::String);
        assert_eq!(DbValueType::from_json_value(&json!([])), DbValueType::Array);
        assert_eq!(DbValueType::from_json_value(&json!({})), DbValueType::Object);
    }

    #[test]
    fn test_conversion_roundtrip() {
        let types = [
            DbValueType::Null,
            DbValueType::Object,
            DbValueType::Array,
            DbValueType::String,
            DbValueType::Integer,
            DbValueType::Float,
            DbValueType::Boolean,
            DbValueType::DateTime,
        ];

        for db_type in types {
            let i32_val = db_type.as_i32();
            let converted_back = DbValueType::from_i32(i32_val).unwrap();
            assert_eq!(db_type, converted_back);
        }
    }
}