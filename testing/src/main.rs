use core::num;
use env_logger;
#[warn(unused_imports)]
use std::fs;
pub mod table;

mod commands;
const FOLDER_PATH: &str = "./src/commands";
use commands::indexer_engine::IndexEngine;
use commands::{db, walengine, walwriter};

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

// fn get_command_names() -> Vec<String> {
//     let folder_path = FOLDER_PATH;
//     let mut files = vec![];
//     if let Ok(entries) = fs::read_dir(folder_path) {
//         for entry in entries.flatten() {
//             let path = entry.path();
//             if path.extension() == Some(std::ffi::OsStr::new("rs")) &&
//                path.file_name() != Some(std::ffi::OsStr::new("mod.rs")) {
//                 if let Some(file_name) = path.file_name().and_then(|f| f.to_str()) {
//                     println!("{:?}", file_name);
//                     files.push(file_name.split('.').next().unwrap().to_string());
//                 }
//             }
//         }
//     } else {
//         eprintln!("Error reading directory");
//     }
//     println!("{:?}", files);
//     files
// }

use rand::Rng;
use std::collections::HashMap;
use std::time::Instant;

fn test_entire_db(db: &mut db::Database, num_rows: usize) {
    // Time table creation and adding columns.
    let start_table = Instant::now();
    db.create_table("test_table").unwrap();
    db.add_column("test_table", "name").unwrap();
    db.add_column("test_table", "age").unwrap();
    db.add_column("test_table", "email").unwrap();
    let duration_table = start_table.elapsed();
    println!(
        "Table creation and column addition took: {:?}",
        duration_table
    );

    let mut rng = rand::thread_rng();

    // Time bulk insertion.
    let start_insert = Instant::now();
    for i in 0..num_rows {
        let id = i.to_string();
        let name = format!("User_{:05}", rng.gen_range(1..100000));
        let age = rng.gen_range(18..=80).to_string();
        let email = format!("user{}@example.com", rng.gen_range(1..100000));

        let mut row_data = std::collections::HashMap::new();
        row_data.insert("name".to_string(), name);
        row_data.insert("age".to_string(), age);
        row_data.insert("email".to_string(), email);

        db.insert_row("test_table", &id, row_data).unwrap();
    }
    let duration_insert = start_insert.elapsed();
    println!("Insertion of {} rows took: {:?}", num_rows, duration_insert);

    // Time random searches.
    let start_search = Instant::now();
    for _ in 0..5 {
        let random_age = rng.gen_range(18..=80).to_string();
        match db.find_rows_by_value_in_table("test_table", "age", &random_age, true) {
            Ok(rows) => println!("Search for age {}: found {} rows", random_age, rows.len()),
            Err(e) => println!("Search error: {}", e),
        }
    }
    let duration_search = start_search.elapsed();
    println!("Performing 5 random searches took: {:?}", duration_search);
}

fn main() {
    env_logger::init();

    // Initialize the database wrapped in Arc<Mutex<>>
    let db = Arc::new(Mutex::new(db::Database::new()));
    let running = Arc::new(AtomicBool::new(true));

    // Load the WAL at startup
    {
        let mut db_lock = db.lock().unwrap();
        if let Err(e) = db_lock.load_wal() {
            eprintln!("Failed to load WAL: {}", e);
        }
        if let Err(e) = db_lock.flush_wal() {
            eprintln!("Failed to flush WAL: {}", e);
        }
    }

    // Setup the asynchronous WAL writer:
    // Create the WAL writer with a batch interval of 1 second.
    let (wal_writer_instance, wal_writer_handle) =
        walwriter::WalWriter::new(Duration::from_secs(1));
    {
        // Inject the wal_writer into the database.
        let mut db_lock = db.lock().unwrap();
        db_lock.wal_writer = Some(wal_writer_instance);
    }
    // Start the asynchronous WAL writer thread.
    wal_writer_handle.start("wal.log".to_string());

    // Start the WAL engine to persist/replay WAL periodically
    let wal_engine = walengine::WalEngine::new(Arc::clone(&db), Duration::from_secs(10));
    thread::spawn(move || wal_engine.start());

    // Start the Index and Bloom Engine to rebuild indexes and bloom filter periodically.
    let index_engine = IndexEngine::new(Arc::clone(&db), Duration::from_secs(15));
    index_engine.start();

    // Simulate database operations
    {
        let mut db_lock = db.lock().unwrap();
        test_entire_db(&mut *db_lock, 10_000);
        // test_entire_db(&mut db_lock);
        // db_lock.commit_wal().unwrap();
        // db_lock.create_table("users").unwrap();
        // db_lock.flush_wal().unwrap();

        // // db_lock.add_column("users", "name").unwrap();
        // // db_lock.add_column("users", "age").unwrap();
        // // db_lock.add_column("users", "email").unwrap();

        // let column_names = vec!["name", "age", "email"];
        // let column_types = vec!["string", "int", "string"];
        // db_lock.add_columns("users", column_names, column_types).unwrap();
        // db_lock.flush_wal().unwrap();

        // let mut row_data = std::collections::HashMap::new();
        // row_data.insert("name".to_string(), "yes".to_string());
        // row_data.insert("age".to_string(), "100".to_string());
        // row_data.insert("email".to_string(), "xyz@.com".to_string());
        // db_lock.insert_row_with_datatype("users", "1", row_data).unwrap();
        // let mut row_data = std::collections::HashMap::new();
        // row_data.insert("name".to_string(), "no".to_string());
        // row_data.insert("age".to_string(), "1".to_string());
        // row_data.insert("email".to_string(), "x@.com".to_string());
        // db_lock.insert_row_with_datatype("users", "2", row_data).unwrap();

        // db_lock.save_table("users", "users.csv").unwrap();

        // // db_lock.update_row("users", "4", "age", "10").unwrap();
        // // db_lock.update_row("users", "2", "email", "y@.com").unwrap();

        // // match db_lock.get_row("users", "1") {
        // //     Ok(row) => println!("Row: {:?}", row),
        // //     Err(e) => eprintln!("Error: {}", e),
        // // }

        // match db_lock.get_table("users") {
        //     Ok(table) => println!("Table: {}", table),
        //     Err(e) => eprintln!("Error: {}", e),
        // }

        // // match db_lock.search_rows_by_condition_in_table("users", "age < 10") {
        // //     Ok(rows) => println!("Rows: {:?}", rows),
        // //     Err(e) => eprintln!("Error: {}", e),
        // // }

        // // match db_lock.find_rows_by_value_in_table("users", "age", "5", false) {
        // //     Ok(rows) => println!("Rows: {:?}", rows),
        // //     Err(e) => eprintln!("Error: {}", e),
        // // }
        // // // Optionally, perform a manual commit here if needed:
        // // // db_lock.flush_wal().unwrap();
        // // db_lock.commit_wal().unwrap();
    }

    // Run for a finite duration then exit.
    thread::sleep(Duration::from_secs(60));
    running.store(false, Ordering::SeqCst);
    println!("Shutting down.");
}
