//// filepath: c:\Users\srija\Documents\GitHub\Rust_DB\testing\src\commands\walengine.rs
use super::db::Database;
use log::{error, info};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

pub struct WalEngine {
    db: Arc<Mutex<Database>>,
    interval: Duration,
}

impl WalEngine {
    pub fn new(db: Arc<Mutex<Database>>, interval: Duration) -> Self {
        WalEngine { db, interval }
    }

    pub fn start(&self) {
        let db_clone = Arc::clone(&self.db);
        let interval = self.interval;
        thread::spawn(move || {
            loop {
                {
                    // Recover from a poisoned mutex by taking the inner value.
                    let mut db = db_clone
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner());
                    // Persist the working WAL.
                    if let Err(e) = db.persist_wal() {
                        error!("Failed to persist WAL: {}", e);
                    } else {
                        info!("WAL persisted successfully.");
                    }
                    // Replay the WAL to update in-memory state.
                    if let Err(e) = db.replay_wal() {
                        error!("Failed to replay WAL: {}", e);
                    } else {
                        info!("WAL replayed successfully.");
                    }
                    // Commit the WAL.
                    if let Err(e) = db.commit_wal() {
                        error!("Failed to commit WAL: {}", e);
                    } else {
                        info!("WAL commit completed.");
                    }
                }
                thread::sleep(interval);
            }
        });
    }
}
