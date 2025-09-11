#!/usr/bin/env python3
"""
Test script for Python bindings to Rust DocDbLite
"""

import sys
import tempfile
from pathlib import Path

# Add the bindings directory to the path
sys.path.insert(0, str(Path(__file__).parent))

from docdblite import DocDbLite, DbConfig, ObjectId

def test_basic_operations():
    """Test basic CRUD operations"""
    print("=== Testing Basic Operations ===")
    
    with tempfile.TemporaryDirectory() as temp_dir:
        # Create database
        config = DbConfig(temp_dir)
        db = DocDbLite(config)
        print("✓ DocDbLite created")
        
        # Add collection
        collection = db.add_collection("test")
        print(f"✓ Collection created: {collection.name}")
        
        # Insert document
        doc = {"key": "value", "number": 42, "bool": True}
        doc_id = collection.insert_one(doc)
        print(f"✓ Document inserted with ID: {doc_id}")
        
        # Find document
        retrieved = collection.find_one(doc_id)
        print(f"✓ Document retrieved: {retrieved}")
        
        # Verify the document matches
        assert retrieved == doc, f"Document mismatch: {retrieved} != {doc}"
        print("✓ Document content verified")

def test_complex_document():
    """Test complex nested document operations"""
    print("\n=== Testing Complex Document ===")
    
    with tempfile.TemporaryDirectory() as temp_dir:
        config = DbConfig(temp_dir)
        db = DocDbLite(config)
        collection = db.add_collection("complex_test")
        
        # Complex document similar to the original tests
        complex_doc = {
            "id": "HOME001",
            "name": "Home Appliances",
            "subcategories": [
                {
                    "id": "KITC001", 
                    "name": "Kitchen Appliances",
                    "products": [
                        {
                            "id": "KA98765",
                            "name": "SmartChef Oven",
                            "brand": "HomeTech",
                            "price": 599.99,
                            "currency": "USD",
                            "inStock": True,
                            "specifications": {
                                "capacity": "30L",
                                "functions": ["Bake", "Roast", "Grill", "Air Fry"],
                                "connectivity": "Wi-Fi",
                                "powerConsumption": "1800W"
                            },
                            "reviews": [
                                {
                                    "userId": "U901234",
                                    "rating": 4.6,
                                    "comment": "Love the smart features and versatility!",
                                    "date": "2024-09-08T19:17:03Z",
                                    "something": [
                                        ["one", "two", "three"],
                                        ["four", "five", "six"],
                                        ["seven", "eight", "nine"]
                                    ]
                                }
                            ]
                        }
                    ]
                }
            ]
        }
        
        # Insert complex document
        doc_id = collection.insert_one(complex_doc)
        print(f"✓ Complex document inserted with ID: {doc_id}")
        
        # Retrieve and verify
        retrieved = collection.find_one(doc_id)
        assert retrieved == complex_doc, "Complex document mismatch"
        print("✓ Complex document verified")

def test_count_and_delete():
    """Test count and delete operations"""
    print("\n=== Testing Count and Delete ===")
    
    with tempfile.TemporaryDirectory() as temp_dir:
        config = DbConfig(temp_dir)
        db = DocDbLite(config)
        collection = db.add_collection("count_test")
        
        # Insert multiple documents
        doc1 = {"testKey1": "testValue1", "testKey2": {"testKey3": 100}}
        doc2 = {"testKey1": "testValue1", "testKey2": {"testKey4": 100}}
        doc3 = {"testKey1": "testValue1", "testKey2": {"testKey5": 100}}
        
        collection.insert_one(doc1)
        collection.insert_one(doc2)
        collection.insert_one(doc3)
        print("✓ Three test documents inserted")
        
        # Count documents
        filter_dict = {"testKey1": "testValue1"}
        count = collection.count_documents(filter_dict)
        assert count == 3, f"Expected 3 documents, got {count}"
        print(f"✓ Count verified: {count} documents")
        
        # Delete one and count again
        collection.delete_one(filter_dict)
        count_after_delete = collection.count_documents(filter_dict)
        assert count_after_delete == 2, f"Expected 2 documents after delete, got {count_after_delete}"
        print(f"✓ After delete count verified: {count_after_delete} documents")
        
        # Delete another and count
        collection.delete_one(filter_dict)
        count_after_second_delete = collection.count_documents(filter_dict)
        assert count_after_second_delete == 1, f"Expected 1 document after second delete, got {count_after_second_delete}"
        print(f"✓ After second delete count verified: {count_after_second_delete} documents")

if __name__ == "__main__":
    try:
        test_basic_operations()
        test_complex_document()
        test_count_and_delete()
        print("\n🎉 All tests passed! Python bindings are working correctly!")
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)