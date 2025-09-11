//! C FFI (Foreign Function Interface) layer for language bindings
//! 
//! This module provides a C-compatible interface that can be used from
//! other programming languages like Python, Node.js, etc.

use std::collections::HashMap;
use std::ffi::{CStr, CString};
use std::os::raw::c_char;
use std::ptr;

use crate::{DocDbLite, DbConfig, Collection, ObjectId, Result};
use serde_json::Value;

/// Error codes for FFI operations
#[repr(C)]
pub enum DocDbErrorCode {
    Success = 0,
    NullPointer = 1,
    InvalidUtf8 = 2,
    JsonError = 3,
    DatabaseError = 4,
    DocumentNotFound = 5,
    CollectionNotFound = 6,
    InvalidFilter = 7,
    GeneralError = 8,
}

/// Opaque handle for DocDbLite instance
pub struct DocDbHandle {
    db: DocDbLite,
}

/// Opaque handle for Collection instance
pub struct CollectionHandle {
    collection: Collection,
}

/// Helper function to convert Rust Result to C error code and optional result
fn handle_result<T, F>(result: Result<T>, success_handler: F) -> DocDbErrorCode
where
    F: FnOnce(T),
{
    match result {
        Ok(value) => {
            success_handler(value);
            DocDbErrorCode::Success
        }
        Err(e) => match e {
            crate::error::DocDbError::Database(_) => DocDbErrorCode::DatabaseError,
            crate::error::DocDbError::Json(_) => DocDbErrorCode::JsonError,
            crate::error::DocDbError::DocumentNotFound(_) => DocDbErrorCode::DocumentNotFound,
            crate::error::DocDbError::CollectionNotFound(_) => DocDbErrorCode::CollectionNotFound,
            crate::error::DocDbError::InvalidFilter(_) => DocDbErrorCode::InvalidFilter,
            _ => DocDbErrorCode::GeneralError,
        }
    }
}

/// Helper function to safely get string from C char pointer
unsafe fn get_str_from_c_char(ptr: *const c_char) -> std::result::Result<String, DocDbErrorCode> {
    if ptr.is_null() {
        return Err(DocDbErrorCode::NullPointer);
    }
    
    CStr::from_ptr(ptr)
        .to_str()
        .map(|s| s.to_string())
        .map_err(|_| DocDbErrorCode::InvalidUtf8)
}

/// Helper function to create C string from Rust string
fn create_c_string(s: &str) -> *mut c_char {
    match CString::new(s) {
        Ok(cstring) => cstring.into_raw(),
        Err(_) => ptr::null_mut(),
    }
}

/// Create a new DocDbLite instance with default configuration
/// 
/// # Safety
/// The returned handle must be freed with docdblite_free()
#[no_mangle]
pub unsafe extern "C" fn docdblite_new() -> *mut DocDbHandle {
    docdblite_new_with_dir(ptr::null())
}

/// Create a new DocDbLite instance with specified directory
/// 
/// # Safety
/// - dir_path can be null for default directory
/// - The returned handle must be freed with docdblite_free()
#[no_mangle]
pub unsafe extern "C" fn docdblite_new_with_dir(dir_path: *const c_char) -> *mut DocDbHandle {
    let config = if dir_path.is_null() {
        DbConfig::default()
    } else {
        match get_str_from_c_char(dir_path) {
            Ok(path_str) => DbConfig::new(path_str),
            Err(_) => return ptr::null_mut(),
        }
    };

    match DocDbLite::new(Some(config)) {
        Ok(db) => Box::into_raw(Box::new(DocDbHandle { db })),
        Err(_) => ptr::null_mut(),
    }
}

/// Free a DocDbLite instance
/// 
/// # Safety
/// handle must be a valid pointer returned from docdblite_new*()
#[no_mangle]
pub unsafe extern "C" fn docdblite_free(handle: *mut DocDbHandle) {
    if !handle.is_null() {
        drop(Box::from_raw(handle));
    }
}

/// Add a collection to the database
/// 
/// # Safety
/// - handle must be a valid DocDbHandle
/// - name must be a valid null-terminated string
/// - The returned collection handle must be freed with collection_free()
#[no_mangle]
pub unsafe extern "C" fn docdblite_add_collection(
    handle: *mut DocDbHandle,
    name: *const c_char,
) -> *mut CollectionHandle {
    if handle.is_null() {
        return ptr::null_mut();
    }

    let name_str = match get_str_from_c_char(name) {
        Ok(s) => s,
        Err(_) => return ptr::null_mut(),
    };

    let db_handle = &mut *handle;
    match db_handle.db.add_collection(&name_str) {
        Ok(collection) => {
            // Clone the collection for the FFI handle
            Box::into_raw(Box::new(CollectionHandle { collection: collection.clone() }))
        }
        Err(_) => ptr::null_mut(),
    }
}

/// Free a Collection instance
/// 
/// # Safety
/// handle must be a valid pointer returned from docdblite_add_collection()
#[no_mangle]
pub unsafe extern "C" fn collection_free(handle: *mut CollectionHandle) {
    if !handle.is_null() {
        drop(Box::from_raw(handle));
    }
}

/// Insert a document into a collection
/// 
/// # Safety
/// - collection_handle must be a valid CollectionHandle
/// - json_doc must be a valid null-terminated JSON string
/// - doc_id_out must point to valid memory for storing the result
#[no_mangle]
pub unsafe extern "C" fn collection_insert_one(
    collection_handle: *mut CollectionHandle,
    json_doc: *const c_char,
    doc_id_out: *mut *mut c_char,
) -> DocDbErrorCode {
    if collection_handle.is_null() || doc_id_out.is_null() {
        return DocDbErrorCode::NullPointer;
    }

    let json_str = match get_str_from_c_char(json_doc) {
        Ok(s) => s,
        Err(e) => return e,
    };

    let collection = &(*collection_handle).collection;
    
    let document: Value = match serde_json::from_str(&json_str) {
        Ok(doc) => doc,
        Err(_) => return DocDbErrorCode::JsonError,
    };

    handle_result(collection.insert_one(&document, None), |doc_id| {
        *doc_id_out = create_c_string(&doc_id.to_string());
    })
}

/// Find a document by ID
/// 
/// # Safety
/// - collection_handle must be a valid CollectionHandle
/// - doc_id must be a valid null-terminated string
/// - json_out must point to valid memory for storing the result
#[no_mangle]
pub unsafe extern "C" fn collection_find_one(
    collection_handle: *mut CollectionHandle,
    doc_id: *const c_char,
    json_out: *mut *mut c_char,
) -> DocDbErrorCode {
    if collection_handle.is_null() || json_out.is_null() {
        return DocDbErrorCode::NullPointer;
    }

    let doc_id_str = match get_str_from_c_char(doc_id) {
        Ok(s) => s,
        Err(e) => return e,
    };

    let object_id = match ObjectId::from_str(&doc_id_str) {
        Ok(id) => id,
        Err(_) => return DocDbErrorCode::InvalidFilter,
    };

    let collection = &(*collection_handle).collection;
    
    handle_result(collection.find_one(&object_id), |maybe_doc| {
        match maybe_doc {
            Some(doc) => {
                match serde_json::to_string(&doc) {
                    Ok(json_str) => *json_out = create_c_string(&json_str),
                    Err(_) => *json_out = ptr::null_mut(),
                }
            }
            None => *json_out = ptr::null_mut(),
        }
    })
}

/// Count documents matching a filter
/// 
/// # Safety
/// - collection_handle must be a valid CollectionHandle
/// - filter_json must be a valid null-terminated JSON string representing the filter
/// - count_out must point to valid memory for storing the result
#[no_mangle]
pub unsafe extern "C" fn collection_count_documents(
    collection_handle: *mut CollectionHandle,
    filter_json: *const c_char,
    count_out: *mut i64,
) -> DocDbErrorCode {
    if collection_handle.is_null() || count_out.is_null() {
        return DocDbErrorCode::NullPointer;
    }

    let filter_str = match get_str_from_c_char(filter_json) {
        Ok(s) => s,
        Err(e) => return e,
    };

    let filter_value: Value = match serde_json::from_str(&filter_str) {
        Ok(val) => val,
        Err(_) => return DocDbErrorCode::JsonError,
    };

    let filter: HashMap<String, Value> = match filter_value {
        Value::Object(obj) => obj.into_iter().collect(),
        _ => return DocDbErrorCode::InvalidFilter,
    };

    let collection = &(*collection_handle).collection;
    
    handle_result(collection.count_documents(&filter), |count| {
        *count_out = count;
    })
}

/// Delete a document matching the filter
/// 
/// # Safety
/// - collection_handle must be a valid CollectionHandle
/// - filter_json must be a valid null-terminated JSON string representing the filter
#[no_mangle]
pub unsafe extern "C" fn collection_delete_one(
    collection_handle: *mut CollectionHandle,
    filter_json: *const c_char,
) -> DocDbErrorCode {
    if collection_handle.is_null() {
        return DocDbErrorCode::NullPointer;
    }

    let filter_str = match get_str_from_c_char(filter_json) {
        Ok(s) => s,
        Err(e) => return e,
    };

    let filter_value: Value = match serde_json::from_str(&filter_str) {
        Ok(val) => val,
        Err(_) => return DocDbErrorCode::JsonError,
    };

    let filter: HashMap<String, Value> = match filter_value {
        Value::Object(obj) => obj.into_iter().collect(),
        _ => return DocDbErrorCode::InvalidFilter,
    };

    let collection = &(*collection_handle).collection;
    
    handle_result(collection.delete_one(&filter), |_| {})
}

/// Free a C string allocated by this library
/// 
/// # Safety
/// ptr must be a valid pointer returned from this library
#[no_mangle]
pub unsafe extern "C" fn docdblite_free_string(ptr: *mut c_char) {
    if !ptr.is_null() {
        drop(CString::from_raw(ptr));
    }
}

#[cfg(test)]
mod ffi_tests {
    use super::*;
    use std::ffi::CString;

    #[test]
    fn test_ffi_basic_operations() {
        unsafe {
            // Create database
            let db_handle = docdblite_new();
            assert!(!db_handle.is_null());

            // Add collection
            let collection_name = CString::new("test_collection").unwrap();
            let collection_handle = docdblite_add_collection(db_handle, collection_name.as_ptr());
            assert!(!collection_handle.is_null());

            // Insert document
            let json_doc = CString::new(r#"{"key": "value", "number": 42}"#).unwrap();
            let mut doc_id_ptr: *mut c_char = ptr::null_mut();
            let result = collection_insert_one(collection_handle, json_doc.as_ptr(), &mut doc_id_ptr);
            assert_eq!(result as i32, DocDbErrorCode::Success as i32);
            assert!(!doc_id_ptr.is_null());

            // Find document
            let mut json_out_ptr: *mut c_char = ptr::null_mut();
            let result = collection_find_one(collection_handle, doc_id_ptr, &mut json_out_ptr);
            assert_eq!(result as i32, DocDbErrorCode::Success as i32);
            assert!(!json_out_ptr.is_null());

            // Verify the JSON
            let retrieved_json = CStr::from_ptr(json_out_ptr).to_str().unwrap();
            let parsed: Value = serde_json::from_str(retrieved_json).unwrap();
            assert_eq!(parsed["key"], "value");
            assert_eq!(parsed["number"], 42);

            // Clean up
            docdblite_free_string(doc_id_ptr);
            docdblite_free_string(json_out_ptr);
            collection_free(collection_handle);
            docdblite_free(db_handle);
        }
    }
}