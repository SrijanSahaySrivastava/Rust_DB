use crate::commands::BloomFilter;
use crate::commands::Indexer;
use crate::db::Database;
use log::{error, info};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

pub struct IndexEngine {
    db: Arc<Mutex<Database>>,
    interval: Duration,
}

impl IndexEngine {
    pub fn new(db: Arc<Mutex<Database>>, interval: Duration) -> Self {
        IndexEngine { db, interval }
    }

    pub fn start(self) {
        let db_clone = Arc::clone(&self.db);
        let interval = self.interval;
        thread::spawn(move || {
            loop {
                {
                    let mut db = db_clone.lock().unwrap();
                    db.build_indexes();
                    db.build_bloom_filter();

                    // Save indexes and bloom filter to file so they can be loaded later.
                    if let Some(ref indexer) = db.indexer {
                        if let Err(e) = indexer.save_to_file("indexer.json") {
                            error!("Failed to save indexer: {}", e);
                        }
                    }
                    if let Some(ref bf) = db.bloom_filter {
                        if let Err(e) = bf.save_to_file("bloom_filter.json") {
                            error!("Failed to save bloom filter: {}", e);
                        }
                    }
                    info!("Indexes and bloom filter rebuilt and saved.");
                }
                thread::sleep(interval);
            }
        });
    }
}
