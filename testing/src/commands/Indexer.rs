use serde::{Serialize, Deserialize};
use std::collections::HashMap;
use std::fs;

#[derive(Serialize, Deserialize)]
pub struct Indexer {
    // Map a key (for example, a column value) to a list of row IDs
    pub index: HashMap<String, Vec<String>>,
}

impl Indexer {
    pub fn new() -> Self {
        Indexer {
            index: HashMap::new(),
        }
    }

    pub fn add(&mut self, key: &str, row_id: &str) {
        self.index.entry(key.to_string()).or_insert(Vec::new()).push(row_id.to_string());
    }

    pub fn get(&self, key: &str) -> Option<&Vec<String>> {
        self.index.get(key)
    }

    pub fn save_to_file(&self, file_path: &str) -> std::io::Result<()> {
        let serialized = serde_json::to_string(self).unwrap();
        fs::write(file_path, serialized)
    }

    pub fn load_from_file(file_path: &str) -> std::io::Result<Self> {
        let data = fs::read_to_string(file_path)?;
        let indexer: Indexer = serde_json::from_str(&data).unwrap();
        Ok(indexer)
    }
}