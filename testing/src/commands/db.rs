//// filepath: c:\Users\srija\Documents\GitHub\Rust_DB\testing\src\commands\db.rs
use crate::commands::BloomFilter;
use crate::commands::Indexer;
use crate::table::table::Table;
use crate::walwriter;
use log::{error, info};
use serde_json;
use std::collections::HashMap;
use std::fs;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::Path;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum DatabaseError {
    #[error("Table '{0}' already exists.")]
    TableAlreadyExists(String),
    #[error("Table '{0}' does not exist.")]
    TableDoesNotExist(String),
    #[error("Row '{0}' does not exist in table '{1}'.")]
    RowDoesNotExist(String, String),
    #[error("Row '{0}' not found in table '{1}'.")]
    RowNotFound(String, String),
    #[error("Error creating file '{0}': {1}")]
    FileCreationError(String, String),
    #[error("datatype error")]
    DataTypeError,
    #[error("Invalid datatype provided.")]
    InvalidDataType,
}

pub type Result<T> = std::result::Result<T, DatabaseError>;

pub struct Database {
    pub tables: HashMap<String, Table>,
    pub operations_since_save: usize,
    pub save_threshold: usize,
    pub wal: Vec<String>,
    pub wal_file: String,
    pub datatypes: Vec<String>,
    pub saved_row_count: usize,
    pub wal_writer: Option<walwriter::WalWriter>,

    pub indexer: Option<Indexer::Indexer>,
    pub bloom_filter: Option<BloomFilter::BloomFilter>,
}

impl Database {
    pub fn new() -> Self {
        Database {
            tables: HashMap::new(),
            operations_since_save: 0,
            save_threshold: 5,
            wal: Vec::new(),
            wal_file: "wal.log".to_string(),
            datatypes: vec![
                "int".to_string(),
                "float".to_string(),
                "string".to_string(),
                "bool".to_string(),
            ],
            wal_writer: None,
            saved_row_count: 0,

            indexer: None,
            bloom_filter: None,
        }
    }

    /// Build indexes (for example, index the "name" column of every row).
    pub fn build_indexes(&mut self) {
        // For simplicity, we build one global index on the "name" column.
        let mut idx = Indexer::Indexer::new();
        for (table_name, table) in self.tables.iter() {
            for (row_id, row_data) in table.rows.iter() {
                if let Some(value) = row_data.get("name") {
                    // You could also include table_name in your key if needed.
                    idx.add(value, row_id);
                }
            }
        }
        self.indexer = Some(idx);
        info!("Indexes built.");
    }

    /// Build bloom filter (for instance, for fast lookups on the "email" column).
    pub fn build_bloom_filter(&mut self) {
        // Create a bloom filter of fixed size.
        let mut bf = crate::commands::BloomFilter::BloomFilter::new(1000);
        for (_table_name, table) in self.tables.iter() {
            for (_row_id, row_data) in table.rows.iter() {
                if let Some(email) = row_data.get("email") {
                    bf.add(email);
                }
            }
        }
        self.bloom_filter = Some(bf);
        info!("Bloom filter built.");
    }

    pub fn check_table(&self, table_name: &str) -> bool {
        self.tables.contains_key(table_name)
    }

    // Create table: update in-memory state and log to WAL.
    pub fn create_table(&mut self, table_name: &str) -> Result<String> {
        if self.check_table(table_name) {
            error!("Table '{}' already exists.", table_name);
            Err(DatabaseError::TableAlreadyExists(table_name.to_string()))
        } else {
            // Update in-memory table immediately.
            self.tables.insert(table_name.to_string(), Table::new());
            // Log the operation
            let op = format!("create_table:{}", table_name);
            self.wal.push(op.clone());
            println!("Table '{}' created and logged to WAL", table_name);
            Ok(table_name.to_string())
        }
    }

    // New helper function to load table from CSV file into memory.
    pub fn load_table_from_file(&mut self, table_name: &str, file_name: &str) -> Result<()> {
        let file = File::open(file_name)
            .map_err(|e| DatabaseError::FileCreationError(file_name.to_string(), e.to_string()))?;
        let reader = BufReader::new(file);
        let mut lines = reader.lines();
        // Read header line.
        if let Some(Ok(header_line)) = lines.next() {
            let headers: Vec<String> = header_line.split(',').map(|s| s.to_string()).collect();
            let mut table = Table::new();
            // Add columns if header has more than one value.
            if headers.len() > 1 {
                for col in headers.iter().skip(1) {
                    table.add_column(col);
                }
            }
            // Process rows.
            for line in lines {
                if let Ok(row_line) = line {
                    let values: Vec<&str> = row_line.split(',').collect();
                    if let Some((row_id, row_values)) = values.split_first() {
                        let mut data = HashMap::new();
                        for (col, val) in headers.iter().skip(1).zip(row_values.iter()) {
                            data.insert(col.to_string(), (*val).to_string());
                        }
                        table.insert_row(row_id, data);
                    }
                }
            }
            self.tables.insert(table_name.to_string(), table);
            println!("Loaded table '{}' from file '{}'", table_name, file_name);
            Ok(())
        } else {
            println!("File '{}' is empty.", file_name);
            Err(DatabaseError::TableDoesNotExist(table_name.to_string()))
        }
    }

    // Add a column: log and update in-memory.
    pub fn add_column(&mut self, table_name: &str, column_name: &str) -> Result<Vec<String>> {
        // Check if the table is in-memory.
        if !self.check_table(table_name) {
            // Table not found: try to load it from file.
            let file_name = format!("{}.csv", table_name);
            if fs::metadata(&file_name).is_ok() {
                match self.load_table_from_file(table_name, &file_name) {
                    Ok(_) => println!("Table '{}' loaded from file '{}'.", table_name, file_name),
                    Err(e) => {
                        error!("Failed to load table from file: {}", e);
                        return Err(e);
                    }
                }
            } else {
                error!(
                    "Table '{}' does not exist in memory or on disk.",
                    table_name
                );
                return Err(DatabaseError::TableDoesNotExist(table_name.to_string()));
            }
        }
        // At this point the table should be in memory.
        if let Some(table) = self.tables.get_mut(table_name) {
            table.add_column(column_name);
            let op = format!("add_column:{}:{}", table_name, column_name);
            // self.wal.push(op);
            if let Some(ref writer) = self.wal_writer {
                writer.log(op);
            } else {
                self.wal.push(op);
            }
            println!(
                "Column '{}' added to table '{}' and logged to WAL",
                column_name, table_name
            );
            Ok(vec![column_name.to_string(), table_name.to_string()])
        } else {
            error!(
                "Table '{}' is still not found after attempting to load.",
                table_name
            );
            Err(DatabaseError::TableDoesNotExist(table_name.to_string()))
        }
    }

    #[allow(dead_code)]
    fn valid_datatype(dt: &str) -> bool {
        match dt {
            "int" | "float" | "string" => true,
            _ => false,
        }
    }
    #[allow(dead_code)]
    fn check_value_matches(value: &str, dtype: &str) -> bool {
        match dtype {
            "int" => value.parse::<i64>().is_ok(),
            "float" => value.parse::<f64>().is_ok(),
            "bool" => {
                let lower = value.to_lowercase();
                lower == "true" || lower == "false"
            }
            "string" => true,
            _ => false,
        }
    }
    #[allow(dead_code)]
    fn is_subset_vec_str(&self, a: &Vec<&str>) -> bool {
        a.iter().all(|&dt| self.datatypes.contains(&dt.to_string()))
    }
    pub fn add_columns(
        &mut self,
        table_name: &str,
        column_names: Vec<&str>,
        datatypes: Vec<&str>,
    ) -> Result<Vec<Vec<String>>> {
        if column_names.len() != datatypes.len() {
            error!("Column names and datatypes must have the same length.");
            return Err(DatabaseError::DataTypeError);
        }

        if !self.check_table(table_name) {
            // Table not found: try to load it from file.
            let file_name = format!("{}.csv", table_name);
            if fs::metadata(&file_name).is_ok() {
                match self.load_table_from_file(table_name, &file_name) {
                    Ok(_) => println!("Table '{}' loaded from file '{}'.", table_name, file_name),
                    Err(e) => {
                        error!("Failed to load table from file: {}", e);
                        return Err(e);
                    }
                }
            } else {
                error!(
                    "Table '{}' does not exist in memory or on disk.",
                    table_name
                );
                return Err(DatabaseError::TableDoesNotExist(table_name.to_string()));
            }
        }
        if Database::is_subset_vec_str(self, &datatypes) == false {
            error!("Invalid datatypes provided.");
            return Err(DatabaseError::InvalidDataType);
        }

        let mut results = Vec::new();

        // Add the new columns.
        for col in column_names.iter() {
            match self.add_column(table_name, col) {
                Ok(res) => results.push(res),
                Err(e) => return Err(e),
            }
        }

        // Insert a single new row that contains the datatypes for each new column.
        let mut data = HashMap::new();
        let table = self
            .tables
            .get_mut(table_name)
            .ok_or(DatabaseError::TableDoesNotExist(table_name.to_string()))?;
        for (col, dt) in column_names.iter().zip(datatypes.iter()) {
            data.insert(col.to_string(), dt.to_string());
            table.add_datatype(col, dt);
        }
        match self.insert_row(table_name, "datatypes", data) {
            Ok(res) => results.push(res),
            Err(e) => return Err(e),
        }

        Ok(results)
    }

    // Get row from table.
    pub fn get_row(&mut self, table_name: &str, row_id: &str) -> Result<Vec<String>> {
        // If the table isn't in memory, try to load it from file.
        if !self.check_table(table_name) {
            let file_name = format!("{}.csv", table_name);
            if fs::metadata(&file_name).is_ok() {
                match self.load_table_from_file(table_name, &file_name) {
                    Ok(_) => println!("Table '{}' loaded from file '{}'.", table_name, file_name),
                    Err(e) => {
                        error!("Failed to load table from file: {}", e);
                        return Err(e);
                    }
                }
            } else {
                error!(
                    "Table '{}' does not exist in memory or on disk.",
                    table_name
                );
                return Err(DatabaseError::TableDoesNotExist(table_name.to_string()));
            }
        }
        // Now the table must be in memory.
        if let Some(table) = self.tables.get(table_name) {
            if let Some(row) = table.get_row(row_id) {
                println!("Row '{}': {:?}", row_id, row);
                let row_string = format!("{:?}", row);
                Ok(vec![row_id.to_string(), row_string])
            } else {
                error!("Row '{}' does not exist in '{}'.", row_id, table_name);
                Err(DatabaseError::RowDoesNotExist(
                    row_id.to_string(),
                    table_name.to_string(),
                ))
            }
        } else {
            error!(
                "Table '{}' is still not found after attempting to load.",
                table_name
            );
            Err(DatabaseError::TableDoesNotExist(table_name.to_string()))
        }
    }

    // Insert row: update in-memory table and log the operation.
    pub fn insert_row(
        &mut self,
        table_name: &str,
        row_id: &str,
        data: HashMap<String, String>,
    ) -> Result<Vec<String>> {
        // If the table isn't in memory, try to load it from file.
        if !self.check_table(table_name) {
            let file_name = format!("{}.csv", table_name);
            if fs::metadata(&file_name).is_ok() {
                match self.load_table_from_file(table_name, &file_name) {
                    Ok(_) => println!("Table '{}' loaded from file '{}'.", table_name, file_name),
                    Err(e) => {
                        error!("Failed to load table from file: {}", e);
                        return Err(e);
                    }
                }
            } else {
                error!(
                    "Table '{}' does not exist in memory or on disk.",
                    table_name
                );
                return Err(DatabaseError::TableDoesNotExist(table_name.to_string()));
            }
        }

        // //check for datatype
        // for (col, val) in &data {
        //     if let Some(table) = self.tables.get(table_name) {
        //         if let Some(dt) = table.row_datatypes.get(col) {
        //             if !Database::check_value_matches(val, dt) {
        //                 error!("Value '{}' does not match datatype '{}' for column '{}'.", val, dt, col);
        //                 return Err(DatabaseError::DataTypeError);
        //             }
        //         } else {
        //             error!("Column '{}' not found in table '{}'.", col, table_name);
        //             return Err(DatabaseError::RowDoesNotExist(row_id.to_string(), table_name.to_string()));
        //         }
        //     }
        // }

        // Now perform the row insertion.
        if let Some(table) = self.tables.get_mut(table_name) {
            table.insert_row(row_id, data.clone());
            let op = format!(
                "insert_row:{}:{}:{}",
                table_name,
                row_id,
                serde_json::to_string(&data).unwrap()
            );
            // self.wal.push(op);
            if let Some(ref writer) = self.wal_writer {
                writer.log(op);
            } else {
                self.wal.push(op);
            }
            println!(
                "Inserted row '{}' in table '{}' and logged to WAL",
                row_id, table_name
            );

            self.operations_since_save += 1;
            if self.operations_since_save >= self.save_threshold {
                let file_name = format!("{}.csv", table_name);
                if let Err(e) = self.save_table_for_insert(table_name, &file_name) {
                    error!("Failed to save table '{}': {}", table_name, e);
                }
                self.operations_since_save = 0;
            }
            Ok(vec![row_id.to_string(), table_name.to_string()])
        } else {
            error!(
                "Table '{}' is still not found after attempting to load.",
                table_name
            );
            Err(DatabaseError::TableDoesNotExist(table_name.to_string()))
        }
    }

    pub fn insert_row_with_datatype(
        &mut self,
        table_name: &str,
        row_id: &str,
        data: HashMap<String, String>,
    ) -> Result<Vec<Vec<String>>> {
        if !self.check_table(table_name) {
            // Table not found: try to load it from file.
            let file_name = format!("{}.csv", table_name);
            if fs::metadata(&file_name).is_ok() {
                match self.load_table_from_file(table_name, &file_name) {
                    Ok(_) => println!("Table '{}' loaded from file '{}'.", table_name, file_name),
                    Err(e) => {
                        error!("Failed to load table from file: {}", e);
                        return Err(e);
                    }
                }
            } else {
                error!(
                    "Table '{}' does not exist in memory or on disk.",
                    table_name
                );
                return Err(DatabaseError::TableDoesNotExist(table_name.to_string()));
            }
        }
        let table = self
            .tables
            .get_mut(table_name)
            .ok_or(DatabaseError::TableDoesNotExist(table_name.to_string()))?;
        //check if the row_id already exists
        if let Some(existing_row) = table.get_row(row_id) {
            error!("Row '{}' already exists in table '{}'.", row_id, table_name);
            return Err(DatabaseError::RowDoesNotExist(
                row_id.to_string(),
                table_name.to_string(),
            ));
        }

        //check for datatype
        for (col, val) in &data {
            if let Some(dt) = table.row_datatypes.get(col) {
                if !Database::check_value_matches(val, dt) {
                    error!(
                        "Value '{}' does not match datatype '{}' for column '{}'.",
                        val, dt, col
                    );
                    return Err(DatabaseError::DataTypeError);
                }
            } else {
                error!("Column '{}' not found in table '{}'.", col, table_name);
                return Err(DatabaseError::RowDoesNotExist(
                    row_id.to_string(),
                    table_name.to_string(),
                ));
            }
        }
        // Now perform the row insertion.
        let result = self.insert_row(table_name, row_id, data)?;
        Ok(vec![result])
    }

    // Update a value in a row for a specific column.
    pub fn update_row(
        &mut self,
        table_name: &str,
        row_id: &str,
        column_name: &str,
        new_value: &str,
    ) -> Result<Vec<String>> {
        if !self.check_table(table_name) {
            let file_name = format!("{}.csv", table_name);
            if fs::metadata(&file_name).is_ok() {
                match self.load_table_from_file(table_name, &file_name) {
                    Ok(_) => println!("Table '{}' loaded from file '{}'.", table_name, file_name),
                    Err(e) => {
                        error!("Failed to load table '{}' from file: {}", table_name, e);
                        return Err(e);
                    }
                }
            } else {
                error!(
                    "Table '{}' does not exist in memory or on disk.",
                    table_name
                );
                return Err(DatabaseError::TableDoesNotExist(table_name.to_string()));
            }
        }
        // Now the table should be in memory.
        if let Some(table) = self.tables.get_mut(table_name) {
            // Ensure the column exists; add it if not.
            if !table.columns.contains(&column_name.to_string()) {
                table.add_column(column_name);
                println!(
                    "Column '{}' was added to table '{}'",
                    column_name, table_name
                );
            }
            if let Some(row) = table.rows.get_mut(row_id) {
                // Update the row in place.
                row.insert(column_name.to_string(), new_value.to_string());

                // Log the update operation in the WAL.
                let op = format!(
                    "update_row:{}:{}:{}:{}",
                    table_name,
                    row_id,
                    column_name,
                    serde_json::to_string(new_value).unwrap()
                );
                // self.wal.push(op);
                if let Some(ref writer) = self.wal_writer {
                    writer.log(op);
                } else {
                    self.wal.push(op);
                }
                println!(
                    "Updated row '{}' in table '{}', column '{}' set to '{}'.",
                    row_id, table_name, column_name, new_value
                );
                self.save_table(table_name, &format!("{}.csv", table_name))?;
                self.operations_since_save += 1;
                if self.operations_since_save >= self.save_threshold {
                    let file_name = format!("{}.csv", table_name);
                    if let Err(e) = self.save_table(table_name, &file_name) {
                        error!("Failed to save table '{}': {}", table_name, e);
                    }
                    self.operations_since_save = 0;
                }
                Ok(vec![
                    row_id.to_string(),
                    column_name.to_string(),
                    new_value.to_string(),
                ])
            } else {
                error!("Row '{}' does not exist in table '{}'.", row_id, table_name);
                Err(DatabaseError::RowDoesNotExist(
                    row_id.to_string(),
                    table_name.to_string(),
                ))
            }
        } else {
            error!(
                "Table '{}' is still not found after attempting to load.",
                table_name
            );
            Err(DatabaseError::TableDoesNotExist(table_name.to_string()))
        }
    }

    pub fn save_table_for_insert(
        &mut self,
        table_name: &str,
        file_name: &str,
    ) -> Result<Vec<String>> {
        if let Some(table) = self.tables.get(table_name) {
            // Get columns sorted.
            let mut columns_in_order: Vec<_> = table.columns.iter().cloned().collect();
            columns_in_order.sort();

            let path = Path::new(file_name);
            let mut writer: BufWriter<Box<dyn Write>>;

            if path.exists() {
                // Open in append mode.
                let file = OpenOptions::new()
                    .append(true)
                    .open(file_name)
                    .map_err(|e| {
                        DatabaseError::FileCreationError(file_name.to_string(), e.to_string())
                    })?;
                writer = BufWriter::new(Box::new(file));
            } else {
                // Create new file and write header.
                let file = File::create(file_name).map_err(|e| {
                    DatabaseError::FileCreationError(file_name.to_string(), e.to_string())
                })?;
                writer = BufWriter::new(Box::new(file));
                let header = {
                    let mut hdr = vec!["row_id".to_string()];
                    hdr.extend(columns_in_order.iter().cloned());
                    hdr.join(",")
                };
                writeln!(writer, "{}", header).unwrap();
            }

            // Get only the unsaved rows.
            let unsaved_rows: Vec<(&String, &HashMap<String, String>)> = table
                .rows
                .iter()
                .skip(self.saved_row_count)
                .filter(|(row_id, _)| *row_id != "datatypes")
                .collect();

            for (row_id, row_data) in unsaved_rows.iter() {
                let mut row_vec = vec![(*row_id).clone()];
                for col in &columns_in_order {
                    row_vec.push(row_data.get(col).cloned().unwrap_or_default());
                }
                writeln!(writer, "{}", row_vec.join(",")).unwrap();
            }
            writer.flush().unwrap();

            // Update the saved row count.
            self.saved_row_count = table.rows.len();
            println!(
                "Table '{}' appended to '{}' with {} new rows.",
                table_name,
                file_name,
                unsaved_rows.len()
            );
            Ok(vec![table_name.to_string(), file_name.to_string()])
        } else {
            error!("Table '{}' does not exist.", table_name);
            Err(DatabaseError::TableDoesNotExist(table_name.to_string()))
        }
    }

    // Save the table to a CSV file.
    pub fn save_table(&self, table_name: &str, file_name: &str) -> Result<Vec<String>> {
        match self.tables.get(table_name) {
            Some(table) => {
                // Get columns sorted.
                let mut columns_in_order: Vec<_> = table.columns.iter().cloned().collect();
                columns_in_order.sort();

                let file_result = File::create(file_name);
                match file_result {
                    Ok(file) => {
                        let mut writer = BufWriter::new(file);
                        // Write header.
                        let header = {
                            let mut hdr = vec!["row_id".to_string()];
                            hdr.extend(columns_in_order.iter().cloned());
                            hdr.join(",")
                        };
                        writeln!(writer, "{}", header).unwrap();

                        // If a "datatypes" row exists, write it as the second row.
                        if let Some(datatype_row) = table.rows.get("datatypes") {
                            let mut row_vec = vec!["datatypes".to_string()];
                            for col in &columns_in_order {
                                row_vec.push(datatype_row.get(col).cloned().unwrap_or_default());
                            }
                            writeln!(writer, "{}", row_vec.join(",")).unwrap();
                        }

                        // Write all other rows (exclude "datatypes").
                        let mut rows: Vec<_> = table
                            .rows
                            .iter()
                            .filter(|(row_id, _)| *row_id != "datatypes")
                            .collect();
                        rows.sort_by_key(|(row_id, _)| row_id.to_string());
                        for (row_id, row_data) in rows {
                            let mut row_vec = vec![row_id.clone()];
                            for col in &columns_in_order {
                                row_vec.push(row_data.get(col).cloned().unwrap_or_default());
                            }
                            writeln!(writer, "{}", row_vec.join(",")).unwrap();
                        }
                        println!("Table '{}' saved to '{}'.", table_name, file_name);
                        Ok(vec![table_name.to_string(), file_name.to_string()])
                    }
                    Err(e) => {
                        error!("Error creating file '{}': {}", file_name, e);
                        Err(DatabaseError::FileCreationError(
                            file_name.to_string(),
                            e.to_string(),
                        ))
                    }
                }
            }
            None => {
                error!("Table '{}' does not exist.", table_name);
                Err(DatabaseError::TableDoesNotExist(table_name.to_string()))
            }
        }
    }

    pub fn get_table(&self, table_name: &str) -> Result<&Table> {
        self.tables
            .get(table_name)
            .ok_or(DatabaseError::TableDoesNotExist(table_name.to_string()))
    }

    /// Finds rows by the given column having a specific value.
    /// Returns a vector of tuples: (table_name, row_id, row_data).
    /// If `return_many` is false, stops at the first match.
    pub fn find_rows_by_value_in_table(
        &self,
        table_name: &str,
        column: &str,
        value: &str,
        return_many: bool,
    ) -> Result<Vec<(String, HashMap<String, String>)>> {
        // If we're searching on a column that we index (e.g., "name"),
        // use the indexer instead of scanning every row.
        if let Some(ref indexer) = self.indexer {
            // Assume that our indexer indexes the column we're interested in.
            if let Some(row_ids) = indexer.get(value) {
                if let Some(table) = self.tables.get(table_name) {
                    let mut results = Vec::new();
                    for row_id in row_ids {
                        if let Some(row) = table.rows.get(row_id) {
                            results.push((row_id.clone(), row.clone()));
                            if !return_many {
                                break;
                            }
                        }
                    }
                    return Ok(results);
                } else {
                    return Err(DatabaseError::TableDoesNotExist(table_name.to_string()));
                }
            }
        }
        // For columns not indexed or when index miss occurs, use the full scan.
        if let Some(table) = self.tables.get(table_name) {
            let mut results = Vec::new();
            for (row_id, row_data) in &table.rows {
                if let Some(v) = row_data.get(column) {
                    // If a BloomFilter is available for this column,
                    // check it to quickly rule out non-existent values.
                    if column == "email" {
                        if let Some(ref bf) = self.bloom_filter {
                            if !bf.contains(v) {
                                continue;
                            }
                        }
                    }
                    if v == value {
                        results.push((row_id.clone(), row_data.clone()));
                        if !return_many {
                            break;
                        }
                    }
                }
            }
            Ok(results)
        } else {
            Err(DatabaseError::TableDoesNotExist(table_name.to_string()))
        }
    }

    /// Searches rows by a simple condition.
    /// The condition should be in the format "column operator value", e.g., "age > 10" or "name == Alice".
    /// Supported operators: "==", ">", "<", ">=", "<=".
    /// Returns a vector of tuples: (table_name, row_id, row_data) for rows matching the condition.
    pub fn search_rows_by_condition_in_table(
        &self,
        table_name: &str,
        condition: &str,
    ) -> Result<Vec<(String, HashMap<String, String>)>> {
        if let Some(table) = self.tables.get(table_name) {
            let parts: Vec<&str> = condition.split_whitespace().collect();
            if parts.len() != 3 {
                println!("Condition format invalid. Expected format: \"column operator value\"");
                return Ok(Vec::new());
            }
            let col = parts[0];
            let operator = parts[1];
            let cond_value = parts[2];
            let mut results = Vec::new();
            for (row_id, row_data) in &table.rows {
                if let Some(val) = row_data.get(col) {
                    let condition_met = match operator {
                        "==" => val == cond_value,
                        ">" => {
                            if let (Ok(num_val), Ok(num_cond)) =
                                (val.parse::<f64>(), cond_value.parse::<f64>())
                            {
                                num_val > num_cond
                            } else {
                                val.as_str() > cond_value
                            }
                        }
                        "<" => {
                            if let (Ok(num_val), Ok(num_cond)) =
                                (val.parse::<f64>(), cond_value.parse::<f64>())
                            {
                                num_val < num_cond
                            } else {
                                val.as_str() < cond_value
                            }
                        }
                        ">=" => {
                            if let (Ok(num_val), Ok(num_cond)) =
                                (val.parse::<f64>(), cond_value.parse::<f64>())
                            {
                                num_val >= num_cond
                            } else {
                                val.as_str() >= cond_value
                            }
                        }
                        "<=" => {
                            if let (Ok(num_val), Ok(num_cond)) =
                                (val.parse::<f64>(), cond_value.parse::<f64>())
                            {
                                num_val <= num_cond
                            } else {
                                val.as_str() <= cond_value
                            }
                        }
                        _ => {
                            println!("Unsupported operator: {}", operator);
                            false
                        }
                    };
                    if condition_met {
                        results.push((row_id.clone(), row_data.clone()));
                    }
                }
            }
            Ok(results)
        } else {
            Err(DatabaseError::TableDoesNotExist(table_name.to_string()))
        }
    }

    // --- WAL functions ---
    // flush_wal() replays all in‑memory operations.
    pub fn flush_wal(&mut self) -> Result<()> {
        for entry in &self.wal {
            let parts: Vec<&str> = entry.split(':').collect();
            match parts[0] {
                "create_table" => {
                    // Already applied during create_table.
                    println!("Replay: Table '{}' exists.", parts[1]);
                }
                "add_column" => {
                    if let Some(table) = self.tables.get_mut(parts[1]) {
                        table.add_column(parts[2]);
                        println!(
                            "Replay: Column '{}' added to table '{}'.",
                            parts[2], parts[1]
                        );
                    }
                }
                "insert_row" => {
                    let table_name = parts[1];
                    let row_id = parts[2];
                    match serde_json::from_str::<HashMap<String, String>>(parts[3]) {
                        Ok(data) => {
                            if let Some(table) = self.tables.get_mut(table_name) {
                                table.insert_row(row_id, data);
                                println!(
                                    "Replay: Row '{}' inserted into table '{}'.",
                                    row_id, table_name
                                );
                            }
                        }
                        Err(e) => {
                            error!(
                                "Failed to deserialize row data for table '{}': {}",
                                table_name, e
                            );
                        }
                    }
                }
                "update_row" => {
                    // Expected format: update_row:{table_name}:{row_id}:{column_name}:{new_value_json}
                    if parts.len() < 5 {
                        error!("Malformed WAL entry: {}", entry);
                        continue;
                    }
                    let table_name = parts[1];
                    let row_id = parts[2];
                    let column_name = parts[3];
                    // Deserialize the new_value
                    let new_value: String =
                        serde_json::from_str(parts[4]).unwrap_or_else(|_| parts[4].to_string());
                    if let Some(table) = self.tables.get_mut(table_name) {
                        if let Some(row) = table.rows.get_mut(row_id) {
                            row.insert(column_name.to_string(), new_value.clone());
                            println!(
                                "Replay: Row '{}' in table '{}' updated column '{}' to '{}'.",
                                row_id, table_name, column_name, new_value
                            );
                        } else {
                            error!(
                                "Replay: Row '{}' not found in table '{}'.",
                                row_id, table_name
                            );
                        }
                    } else {
                        error!("Replay: Table '{}' not found.", table_name);
                    }
                }
                _ => {
                    println!("Unknown WAL entry: {}", entry);
                }
            }
        }
        Ok(())
    }

    // Call this after a set of operations has been committed.
    pub fn commit_wal(&mut self) -> Result<()> {
        // Append the current in‑memory WAL entries to the archive file.
        let archive_file = "wal_archive.log".to_string();
        let archive = OpenOptions::new()
            .append(true)
            .create(true)
            .open(&archive_file)
            .map_err(|err| {
                DatabaseError::FileCreationError(archive_file.clone(), err.to_string())
            })?;
        let mut archive_writer = BufWriter::new(archive);
        for entry in &self.wal {
            writeln!(archive_writer, "{}", entry).map_err(|err| {
                DatabaseError::FileCreationError(archive_file.clone(), err.to_string())
            })?;
        }
        archive_writer.flush().unwrap();
        println!("WAL entries committed to archive '{}'.", archive_file);

        // Now clear the persistent WAL:
        self.wal.clear();
        // Truncate the working persistent WAL file by creating a new file.
        File::create(&self.wal_file).map_err(|err| {
            DatabaseError::FileCreationError(self.wal_file.clone(), err.to_string())
        })?;
        println!("Persistent WAL '{}' cleared.", self.wal_file);
        Ok(())
    }

    // persist_wal() writes the in‑memory WAL to disk in append mode.
    pub fn persist_wal(&self) -> Result<()> {
        let file = OpenOptions::new()
            .append(true)
            .create(true)
            .open(&self.wal_file)
            .map_err(|err| {
                DatabaseError::FileCreationError(self.wal_file.to_string(), err.to_string())
            })?;
        let mut writer = BufWriter::new(file);
        for entry in &self.wal {
            writeln!(writer, "{}", entry).map_err(|err| {
                DatabaseError::FileCreationError(self.wal_file.to_string(), err.to_string())
            })?;
        }
        writer.flush().unwrap();
        println!("WAL persisted to {}", self.wal_file);
        Ok(())
    }

    // load_wal() reads existing WAL operations from disk.
    pub fn load_wal(&mut self) -> Result<()> {
        let file = File::open(&self.wal_file)
            .map_err(|e| DatabaseError::FileCreationError(self.wal_file.clone(), e.to_string()))?;
        let reader = BufReader::new(file);
        for line in reader.lines() {
            let ln = line.map_err(|e| {
                DatabaseError::FileCreationError(self.wal_file.clone(), e.to_string())
            })?;
            if !ln.trim().is_empty() {
                match serde_json::from_str::<HashMap<String, String>>(&ln) {
                    Ok(row_data) => {
                        // Process the row_data.
                    }
                    Err(e) => {
                        error!(
                            "Failed to deserialize row data for table 'test_table': {}",
                            e
                        );
                    }
                }
            }
        }
        Ok(())
    }

    // clear_wal() clears both the in‑memory WAL and truncates the WAL file.
    pub fn clear_wal(&mut self) -> Result<()> {
        self.wal.clear();
        File::create(&self.wal_file).map_err(|err| {
            DatabaseError::FileCreationError(self.wal_file.to_string(), err.to_string())
        })?;
        println!("WAL cleared.");
        Ok(())
    }

    // replay_wal() simply flushes the WAL to replay its operations.
    pub fn replay_wal(&mut self) -> Result<()> {
        self.flush_wal()?;
        Ok(())
    }
}
