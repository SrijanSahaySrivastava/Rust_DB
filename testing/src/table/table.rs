use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt;

#[derive(Debug)]
pub struct Table {
    pub columns: HashSet<String>,  // List of allowed column names
    pub rows: BTreeMap<String, HashMap<String, String>>, // row_id -> { column_name -> value }
    pub row_datatypes: HashMap<String, String>, // column_name -> datatype
}

impl Table {
    pub fn new() -> Self {
        Table {
            columns: HashSet::new(),
            rows: BTreeMap::new(),
            row_datatypes: HashMap::new(),
        }
    }

    /// Add a new column to the table. Existing rows do not automatically get a value for this column.
    pub fn add_column(&mut self, column_name: &str) {
        self.columns.insert(column_name.to_string());
    }


    pub fn add_datatype(&mut self, column_name: &str, datatype: &str) {
        if self.row_datatypes.contains_key(column_name) {
            println!(" - already exists");
            return;
        }
        println!("Adding datatype {} to column {}", datatype, column_name);
        self.row_datatypes.insert(column_name.to_string(), datatype.to_string());
    }

    /// Insert or update a row with (column -> value) pairs; restrict columns to those known in `columns`.
    pub fn insert_row(&mut self, row_id: &str, data: HashMap<String, String>) {
        // Only allow data for columns that exist in this table.
        let valid_data: HashMap<String, String> = data
            .into_iter()
            .filter(|(col, _)| self.columns.contains(col))
            .collect();

        // Upsert (insert if none, update if it exists).
        self.rows
            .entry(row_id.to_string())
            .and_modify(|existing| {
                for (col, val) in &valid_data {
                    existing.insert(col.clone(), val.clone());
                }
            })
            .or_insert(valid_data);
    }

    /// Retrieve data for a specific row.
    pub fn get_row(&self, row_id: &str) -> Option<&HashMap<String, String>> {
        self.rows.get(row_id)
    }
    /// Delete a specific row by row_id.
    pub fn delete_row(&mut self, row_id: &str) -> bool {
        self.rows.remove(row_id).is_some()
    }

    /// Print the table contents (for demo).
    pub fn print_table(&self) {
        println!("Columns: {:?}", self.columns);
        for (row_id, row_data) in &self.rows {
            println!("Row '{}': {:?}", row_id, row_data);
        }
    }

    pub fn get_table(&self) -> &BTreeMap<String, HashMap<String, String>> {
        &self.rows
    }
}

impl fmt::Display for Table {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Sort columns for predictable order
        let mut cols: Vec<&String> = self.columns.iter().collect();
        cols.sort();
        
        // Write header row
        write!(f, "{:<10}", "Row ID")?;
        for col in &cols {
            write!(f, " | {:<15}", col)?;
        }
        writeln!(f)?;
        writeln!(f, "{}", "-".repeat(10 + cols.len() * 18))?;
        
        // Write each row sorted by row_id
        let mut row_ids: Vec<&String> = self.rows.keys().collect();
        row_ids.sort();
        for row_id in row_ids {
            write!(f, "{:<10}", row_id)?;
            for col in &cols {
                let value = self
                    .rows
                    .get(row_id)
                    .and_then(|r| r.get(col.as_str()))
                    .cloned()
                    .unwrap_or_default();
                write!(f, " | {:<15}", value)?;
            }
            writeln!(f)?;
        }
        Ok(())
    }
}