use serde::{Serialize, Deserialize};
use std::fs;

#[derive(Serialize, Deserialize)]
pub struct BloomFilter {
    bit_array: Vec<bool>,
    size: usize,
}

impl BloomFilter {
    pub fn new(size: usize) -> Self {
        BloomFilter {
            bit_array: vec![false; size],
            size,
        }
    }

    pub fn add(&mut self, item: &str) {
        let hash1 = Self::hash1(item) % self.size;
        let hash2 = Self::hash2(item) % self.size;
        self.bit_array[hash1] = true;
        self.bit_array[hash2] = true;
    }

    pub fn contains(&self, item: &str) -> bool {
        let hash1 = Self::hash1(item) % self.size;
        let hash2 = Self::hash2(item) % self.size;
        self.bit_array[hash1] && self.bit_array[hash2]
    }

    fn hash1(item: &str) -> usize {
        let mut hash = 5381;
        for byte in item.bytes() {
            hash = ((hash << 5) + hash) + (byte as usize);
        }
        hash
    }

    fn hash2(item: &str) -> usize {
        let mut hash: usize = 0;
        for byte in item.bytes() {
            hash = hash.wrapping_mul(31).wrapping_add(byte as usize);
        }
        hash
    }

    pub fn save_to_file(&self, file_path: &str) -> std::io::Result<()> {
        let serialized = serde_json::to_string(self).unwrap();
        fs::write(file_path, serialized)
    }

    pub fn load_from_file(file_path: &str) -> std::io::Result<Self> {
        let data = fs::read_to_string(file_path)?;
        let bf: BloomFilter = serde_json::from_str(&data).unwrap();
        Ok(bf)
    }
}