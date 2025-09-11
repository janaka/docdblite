//! Tests that replicate the Python test suite for compatibility verification

use docdblite::{DocDbLite, DbConfig};
use serde_json::json;
use std::collections::HashMap;
use tempfile::TempDir;

#[test]
fn test_insert_json_str_and_get_consistency_of_doc() -> Result<(), Box<dyn std::error::Error>> {
    // This replicates the Python test exactly
    let temp_dir = TempDir::new()?;
    let config = DbConfig::new(temp_dir.path());
    let mut db = DocDbLite::new(Some(config))?;

    let test_collection = db.add_collection("testCollection")?;

    let test_json_str = r#"{
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
              "inStock": true,
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
    }"#;

    // Parse the JSON string
    let expected_doc: serde_json::Value = serde_json::from_str(test_json_str)?;

    let doc_id = test_collection.insert_one(&expected_doc, None)?;
    let doc = test_collection.find_one(&doc_id)?.unwrap();

    // Verify the documents match
    assert_eq!(expected_doc.as_object().unwrap().len(), doc.as_object().unwrap().len());
    assert_eq!(expected_doc, doc);

    println!("✅ JSON string insert and retrieval test passed");
    Ok(())
}

#[test]
fn test_insert_json_typed_and_get_consistency_of_doc() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = TempDir::new()?;
    let config = DbConfig::new(temp_dir.path());
    let mut db = DocDbLite::new(Some(config))?;

    let test_collection = db.add_collection("testCollection")?;

    let doc1 = json!({"testKey100": "testValue1234", "testKey233": {"testKey2343": 1001}});

    let doc_id = test_collection.insert_one(&doc1, None)?;
    let doc_result = test_collection.find_one(&doc_id)?.unwrap();

    assert_eq!(doc1.as_object().unwrap().len(), doc_result.as_object().unwrap().len());
    assert_eq!(doc1, doc_result);

    println!("✅ JSON typed insert and retrieval test passed");
    Ok(())
}

#[test]
fn test_count_documents() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = TempDir::new()?;
    let config = DbConfig::new(temp_dir.path());
    let mut db = DocDbLite::new(Some(config))?;

    let test_collection = db.add_collection("testCollection")?;

    let doc1 = json!({"testKey1": "testValue1", "testKey2": {"testKey3": 100}});
    let doc2 = json!({"testKey1": "testValue1", "testKey2": {"testKey4": 100}});
    let doc3 = json!({"testKey1": "testValue1", "testKey2": {"testKey5": 100}});

    test_collection.insert_one(&doc1, None)?;
    test_collection.insert_one(&doc2, None)?;
    test_collection.insert_one(&doc3, None)?;

    let mut filter = HashMap::new();
    filter.insert("testKey1".to_string(), json!("testValue1"));

    assert_eq!(test_collection.count_documents(&filter)?, 3);

    test_collection.delete_one(&filter)?;
    assert_eq!(test_collection.count_documents(&filter)?, 2);

    test_collection.delete_one(&filter)?;
    assert_eq!(test_collection.count_documents(&filter)?, 1);

    test_collection.delete_one(&filter)?;
    assert_eq!(test_collection.count_documents(&filter)?, 0);

    println!("✅ Count documents test passed");
    Ok(())
}