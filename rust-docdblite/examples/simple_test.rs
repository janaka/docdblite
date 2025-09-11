use docdblite::{DocDbLite, DbConfig};
use serde_json::json;
use tempfile::TempDir;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = TempDir::new()?;
    let config = DbConfig::new(temp_dir.path());
    let mut db = DocDbLite::new(Some(config))?;

    println!("DocDbLite created successfully!");

    let collection = db.add_collection("test")?;
    println!("Collection created: {}", collection.name);

    // Insert a simple document
    let doc = json!({"key": "value", "number": 42});
    let doc_id = collection.insert_one(&doc, None)?;
    println!("Document inserted with ID: {}", doc_id);

    // Find the document
    if let Some(retrieved) = collection.find_one(&doc_id)? {
        println!("Retrieved document: {}", retrieved);
    } else {
        println!("Document not found!");
    }

    Ok(())
}