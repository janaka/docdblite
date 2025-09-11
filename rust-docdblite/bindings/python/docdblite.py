"""
Python bindings for DocDbLite using ctypes
"""

import ctypes
import json
import os
from typing import Optional, Dict, Any, Union
from pathlib import Path

# Load the shared library
def _load_library():
    # Try to find the library in common locations
    possible_paths = [
        # Development build path
        Path(__file__).parent.parent.parent / "target" / "release" / "libdocdblite.so",
        # Installed system path
        "libdocdblite.so",
        # Windows
        "libdocdblite.dll",
        "docdblite.dll",
    ]
    
    for path in possible_paths:
        try:
            if isinstance(path, Path):
                path = str(path)
            return ctypes.CDLL(path)
        except OSError:
            continue
    
    raise RuntimeError("Could not find libdocdblite shared library")

lib = _load_library()

# Error codes
class DocDbError(Exception):
    """Base exception for DocDbLite errors"""
    pass

class DatabaseError(DocDbError):
    """Database operation error"""
    pass

class JsonError(DocDbError):
    """JSON parsing/serialization error"""
    pass

class DocumentNotFoundError(DocDbError):
    """Document not found error"""
    pass

class CollectionNotFoundError(DocDbError):
    """Collection not found error"""
    pass

class InvalidFilterError(DocDbError):
    """Invalid filter error"""
    pass

# Error code mapping
ERROR_MAP = {
    1: DocDbError("Null pointer"),
    2: DocDbError("Invalid UTF-8"),
    3: JsonError("JSON error"),
    4: DatabaseError("Database error"),
    5: DocumentNotFoundError("Document not found"),
    6: CollectionNotFoundError("Collection not found"),
    7: InvalidFilterError("Invalid filter"),
    8: DocDbError("General error"),
}

def _check_error(error_code: int):
    """Check error code and raise appropriate exception"""
    if error_code != 0:
        raise ERROR_MAP.get(error_code, DocDbError(f"Unknown error: {error_code}"))

# Function signatures
lib.docdblite_new.argtypes = []
lib.docdblite_new.restype = ctypes.c_void_p

lib.docdblite_new_with_dir.argtypes = [ctypes.c_char_p]
lib.docdblite_new_with_dir.restype = ctypes.c_void_p

lib.docdblite_free.argtypes = [ctypes.c_void_p]
lib.docdblite_free.restype = None

lib.docdblite_add_collection.argtypes = [ctypes.c_void_p, ctypes.c_char_p]
lib.docdblite_add_collection.restype = ctypes.c_void_p

lib.collection_free.argtypes = [ctypes.c_void_p]
lib.collection_free.restype = None

lib.collection_insert_one.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.POINTER(ctypes.c_char_p)]
lib.collection_insert_one.restype = ctypes.c_int

lib.collection_find_one.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.POINTER(ctypes.c_char_p)]
lib.collection_find_one.restype = ctypes.c_int

lib.collection_count_documents.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.POINTER(ctypes.c_longlong)]
lib.collection_count_documents.restype = ctypes.c_int

lib.collection_delete_one.argtypes = [ctypes.c_void_p, ctypes.c_char_p]
lib.collection_delete_one.restype = ctypes.c_int

lib.docdblite_free_string.argtypes = [ctypes.c_char_p]
lib.docdblite_free_string.restype = None

class ObjectId:
    """Simple ObjectId wrapper"""
    def __init__(self, value: str):
        self.value = value
    
    def __str__(self):
        return self.value
    
    def __repr__(self):
        return f"ObjectId('{self.value}')"

class Collection:
    """Python wrapper for Collection"""
    
    def __init__(self, handle: int, name: str):
        self._handle = handle
        self.name = name
    
    def __del__(self):
        if hasattr(self, '_handle') and self._handle:
            lib.collection_free(self._handle)
    
    def insert_one(self, document: Union[Dict[str, Any], str], uuid: Optional[ObjectId] = None) -> ObjectId:
        """Insert a document into the collection"""
        if isinstance(document, dict):
            json_str = json.dumps(document)
        else:
            json_str = document
        
        json_bytes = json_str.encode('utf-8')
        doc_id_ptr = ctypes.c_char_p()
        
        error_code = lib.collection_insert_one(
            self._handle,
            json_bytes,
            ctypes.byref(doc_id_ptr)
        )
        
        _check_error(error_code)
        
        if doc_id_ptr:
            doc_id_str = doc_id_ptr.value.decode('utf-8')
            lib.docdblite_free_string(doc_id_ptr)
            return ObjectId(doc_id_str)
        else:
            raise DocDbError("Failed to get document ID")
    
    def find_one(self, uuid: ObjectId) -> Optional[Dict[str, Any]]:
        """Find a document by ID"""
        uuid_bytes = str(uuid).encode('utf-8')
        json_ptr = ctypes.c_char_p()
        
        error_code = lib.collection_find_one(
            self._handle,
            uuid_bytes,
            ctypes.byref(json_ptr)
        )
        
        _check_error(error_code)
        
        if json_ptr:
            json_str = json_ptr.value.decode('utf-8')
            lib.docdblite_free_string(json_ptr)
            return json.loads(json_str)
        else:
            return None
    
    def count_documents(self, filter_dict: Dict[str, Any]) -> int:
        """Count documents matching the filter"""
        filter_json = json.dumps(filter_dict)
        filter_bytes = filter_json.encode('utf-8')
        count = ctypes.c_longlong()
        
        error_code = lib.collection_count_documents(
            self._handle,
            filter_bytes,
            ctypes.byref(count)
        )
        
        _check_error(error_code)
        return count.value
    
    def delete_one(self, filter_dict: Dict[str, Any]) -> None:
        """Delete a document matching the filter"""
        filter_json = json.dumps(filter_dict)
        filter_bytes = filter_json.encode('utf-8')
        
        error_code = lib.collection_delete_one(self._handle, filter_bytes)
        _check_error(error_code)

class DocDbLite:
    """Python wrapper for DocDbLite"""
    
    def __init__(self, config: Optional[Union[str, 'DbConfig']] = None):
        if config is None:
            self._handle = lib.docdblite_new()
        elif isinstance(config, str):
            config_bytes = config.encode('utf-8')
            self._handle = lib.docdblite_new_with_dir(config_bytes)
        elif hasattr(config, 'dir'):
            config_bytes = str(config.dir).encode('utf-8')
            self._handle = lib.docdblite_new_with_dir(config_bytes)
        else:
            raise ValueError("Invalid config type")
        
        if not self._handle:
            raise DocDbError("Failed to create DocDbLite instance")
    
    def __del__(self):
        if hasattr(self, '_handle') and self._handle:
            lib.docdblite_free(self._handle)
    
    def add_collection(self, name: str) -> Collection:
        """Add a collection to the database"""
        name_bytes = name.encode('utf-8')
        collection_handle = lib.docdblite_add_collection(self._handle, name_bytes)
        
        if not collection_handle:
            raise DocDbError(f"Failed to create collection: {name}")
        
        return Collection(collection_handle, name)

class DbConfig:
    """Configuration for DocDbLite"""
    def __init__(self, dir_path: str = None):
        if dir_path is None:
            self.dir = os.path.expanduser("~/.docdblite")
        else:
            self.dir = dir_path

# For compatibility with the original Python API
def get_default_config():
    return DbConfig()