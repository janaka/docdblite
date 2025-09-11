use docdblite::{DocDbLite, DbConfig};
use serde_json::json;
use std::collections::HashMap;
use tempfile::TempDir;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = TempDir::new()?;
    let config = DbConfig::new(temp_dir.path());
    let mut db = DocDbLite::new(Some(config))?;

    println!("=== Complex Document Test ===");

    let collection = db.add_collection("test")?;
    println!("Collection created: {}", collection.name);

    // Test complex document similar to the Python tests
    let complex_doc = json!({
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
    });

    println!("Inserting complex document...");
    let doc_id = collection.insert_one(&complex_doc, None)?;
    println!("Document inserted with ID: {}", doc_id);

    println!("Retrieving complex document...");
    if let Some(retrieved) = collection.find_one(&doc_id)? {
        println!("Retrieved document successfully!");
        
        // Verify structure
        if retrieved == complex_doc {
            println!("✅ Document matches original perfectly!");
        } else {
            println!("❌ Document does not match original");
            println!("Original: {}", serde_json::to_string_pretty(&complex_doc)?);
            println!("Retrieved: {}", serde_json::to_string_pretty(&retrieved)?);
        }
    } else {
        println!("Document not found!");
    }

    // Test count and delete operations
    println!("\n=== Testing count and filter operations ===");
    
    // Insert some test documents for filtering
    let doc1 = json!({"testKey1": "testValue1", "testKey2": {"testKey3": 100}});
    let doc2 = json!({"testKey1": "testValue1", "testKey2": {"testKey4": 100}});
    let doc3 = json!({"testKey1": "testValue1", "testKey2": {"testKey5": 100}});

    collection.insert_one(&doc1, None)?;
    collection.insert_one(&doc2, None)?;
    collection.insert_one(&doc3, None)?;

    let mut filter = HashMap::new();
    filter.insert("testKey1".to_string(), json!("testValue1"));
    
    let count = collection.count_documents(&filter)?;
    println!("Found {} documents with testKey1='testValue1'", count);

    // Delete one and count again
    collection.delete_one(&filter)?;
    let count_after_delete = collection.count_documents(&filter)?;
    println!("After deleting one: {} documents remain", count_after_delete);

    println!("✅ All operations completed successfully!");
    
    Ok(())
}