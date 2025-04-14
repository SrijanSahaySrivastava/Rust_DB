use std::collections::HashMap;
use std::fs::File;
use std::io::{self, Read, Write, BufReader, BufWriter};

/// Supported data types for row values.
#[derive(Debug, PartialEq)]
pub enum DataValue {
    Int(i64),
    Float(f64),
    Bool(bool),
    Text(String),
}

/// A row with its own data types and an encryption flag.
#[derive(Debug)]
pub struct Row {
    pub data: HashMap<String, DataValue>,
    pub encrypted: bool,
}

impl Default for Row {
    fn default() -> Self {
        Self { 
            data: HashMap::new(),
            encrypted: false,
        }
    }
}

/// Table now uses the new Row type.
#[derive(Debug, Default)]
pub struct Table {
    pub columns: Vec<String>,
    pub rows: HashMap<String, Row>,
}

/// Database remains mostly the same.
#[derive(Debug, Default)]
pub struct Database {
    pub tables: HashMap<String, Table>,
}

/// Helper function to write a string in binary form with a length prefix.
fn write_string<W: Write>(writer: &mut W, s: &str) -> io::Result<()> {
    let bytes = s.as_bytes();
    let len = bytes.len() as u32;
    writer.write_all(&len.to_le_bytes())?;
    writer.write_all(bytes)?;
    Ok(())
}

/// Helper function to read a length-prefixed string.
fn read_string<R: Read>(reader: &mut R) -> io::Result<String> {
    let mut len_buf = [0u8; 4];
    reader.read_exact(&mut len_buf)?;
    let len = u32::from_le_bytes(len_buf) as usize;
    let mut buffer = vec![0u8; len];
    reader.read_exact(&mut buffer)?;
    Ok(String::from_utf8_lossy(&buffer).into_owned())
}

/// Write a DataValue to the writer in binary form.
/// Format: variant id (u8) followed by the value.
fn write_data_value<W: Write>(writer: &mut W, value: &DataValue) -> io::Result<()> {
    match value {
        DataValue::Int(i) => {
            writer.write_all(&[0])?;
            writer.write_all(&i.to_le_bytes())?;
        },
        DataValue::Float(f) => {
            writer.write_all(&[1])?;
            writer.write_all(&f.to_le_bytes())?;
        },
        DataValue::Bool(b) => {
            writer.write_all(&[2])?;
            writer.write_all(&[*b as u8])?;
        },
        DataValue::Text(s) => {
            writer.write_all(&[3])?;
            write_string(writer, s)?;
        },
    }
    Ok(())
}

/// Read a DataValue from the reader.
fn read_data_value<R: Read>(reader: &mut R) -> io::Result<DataValue> {
    let mut variant = [0u8; 1];
    reader.read_exact(&mut variant)?;
    match variant[0] {
        0 => {
            let mut buf = [0u8; 8];
            reader.read_exact(&mut buf)?;
            Ok(DataValue::Int(i64::from_le_bytes(buf)))
        },
        1 => {
            let mut buf = [0u8; 8];
            reader.read_exact(&mut buf)?;
            Ok(DataValue::Float(f64::from_le_bytes(buf)))
        },
        2 => {
            let mut buf = [0u8; 1];
            reader.read_exact(&mut buf)?;
            Ok(DataValue::Bool(buf[0] != 0))
        },
        3 => {
            let s = read_string(reader)?;
            Ok(DataValue::Text(s))
        },
        _ => Err(io::Error::new(io::ErrorKind::InvalidData, "Unknown DataValue variant")),
    }
}

/// Writes the Database state to a binary file.
pub fn write_database_to_binary(db: &Database, file_path: &str) -> io::Result<()> {
    let file = File::create(file_path)?;
    let mut writer = BufWriter::new(file);

    // Write a simple header.
    writer.write_all(b"RDBB")?;

    // Write the number of tables.
    let num_tables = db.tables.len() as u32;
    writer.write_all(&num_tables.to_le_bytes())?;

    for (table_name, table) in &db.tables {
        // Write table name.
        write_string(&mut writer, table_name)?;

        // Write columns.
        let num_columns = table.columns.len() as u32;
        writer.write_all(&num_columns.to_le_bytes())?;
        for col in &table.columns {
            write_string(&mut writer, col)?;
        }

        // Write rows.
        let num_rows = table.rows.len() as u32;
        writer.write_all(&num_rows.to_le_bytes())?;
        for (row_id, row) in &table.rows {
            write_string(&mut writer, row_id)?;
            
            // Write encrypted flag (1 byte: 0 or 1).
            writer.write_all(&[row.encrypted as u8])?;

            // Write number of entries in the row.
            let num_entries = row.data.len() as u32;
            writer.write_all(&num_entries.to_le_bytes())?;
            for (col, value) in &row.data {
                write_string(&mut writer, col)?;
                write_data_value(&mut writer, value)?;
            }
        }
    }
    writer.flush()?;
    println!("Database written to binary file: {}", file_path);
    Ok(())
}

/// Reads the Database state from a binary file.
pub fn read_database_from_binary(file_path: &str) -> io::Result<Database> {
    let file = File::open(file_path)?;
    let mut reader = BufReader::new(file);

    let mut header = [0u8; 4];
    reader.read_exact(&mut header)?;
    if &header != b"RDBB" {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "Invalid file header"));
    }

    let mut num_tables_buf = [0u8; 4];
    reader.read_exact(&mut num_tables_buf)?;
    let num_tables = u32::from_le_bytes(num_tables_buf);

    let mut db = Database::default();
    for _ in 0..num_tables {
        // Read table name.
        let table_name = read_string(&mut reader)?;

        // Read columns.
        let mut num_cols_buf = [0u8; 4];
        reader.read_exact(&mut num_cols_buf)?;
        let num_columns = u32::from_le_bytes(num_cols_buf);
        let mut columns = Vec::with_capacity(num_columns as usize);
        for _ in 0..num_columns {
            columns.push(read_string(&mut reader)?);
        }

        // Read rows.
        let mut num_rows_buf = [0u8; 4];
        reader.read_exact(&mut num_rows_buf)?;
        let num_rows = u32::from_le_bytes(num_rows_buf);
        let mut rows = HashMap::new();
        for _ in 0..num_rows {
            let row_id = read_string(&mut reader)?;
            
            // Read encrypted flag.
            let mut flag_buf = [0u8; 1];
            reader.read_exact(&mut flag_buf)?;
            let encrypted = flag_buf[0] != 0;

            // Read number of entries.
            let mut num_entries_buf = [0u8; 4];
            reader.read_exact(&mut num_entries_buf)?;
            let num_entries = u32::from_le_bytes(num_entries_buf);
            let mut row_data = HashMap::new();
            for _ in 0..num_entries {
                let col = read_string(&mut reader)?;
                let val = read_data_value(&mut reader)?;
                row_data.insert(col, val);
            }
            rows.insert(row_id, Row { data: row_data, encrypted });
        }

        db.tables.insert(table_name, Table { columns, rows });
    }
    println!("Database read from binary file: {}", file_path);
    Ok(db)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn test_write_and_read_database() {
        let mut db = Database::default();
        let mut table = Table::default();
        table.columns = vec!["name".to_string(), "age".to_string()];
        let mut row_data = HashMap::new();
        row_data.insert("name".to_string(), DataValue::Text("Alice".to_string()));
        row_data.insert("age".to_string(), DataValue::Int(30));
        // Create an unencrypted row.
        table.rows.insert("1".to_string(), Row { data: row_data, encrypted: false });
        db.tables.insert("users".to_string(), table);

        let file_path = "test_db.bin";
        write_database_to_binary(&db, file_path).expect("Failed to write database");
        let read_db = read_database_from_binary(file_path).expect("Failed to read database");

        // Clean up test file.
        fs::remove_file(file_path).unwrap();

        // Verify read content.
        assert!(read_db.tables.contains_key("users"));
        let users_table = read_db.tables.get("users").unwrap();
        assert_eq!(users_table.columns, vec!["name", "age"]);
        let row = users_table.rows.get("1").unwrap();
        assert_eq!(row.encrypted, false);
        assert_eq!(row.data.get("name").unwrap(), &DataValue::Text("Alice".to_string()));
        assert_eq!(row.data.get("age").unwrap(), &DataValue::Int(30));
    }

    #[test]
    fn test_encrypted_row() {
        let mut db = Database::default();
        let mut table = Table::default();
        table.columns = vec!["message".to_string()];
        let mut row_data = HashMap::new();
        row_data.insert("message".to_string(), DataValue::Text("Secret".to_string()));
        // Create an encrypted row.
        table.rows.insert("encrypted1".to_string(), Row { data: row_data, encrypted: true });
        db.tables.insert("secrets".to_string(), table);

        let file_path = "encrypted_test_db.bin";
        write_database_to_binary(&db, file_path).expect("Failed to write encrypted database");
        let read_db = read_database_from_binary(file_path).expect("Failed to read encrypted database");

        // Clean up test file.
        fs::remove_file(file_path).unwrap();

        let secrets_table = read_db.tables.get("secrets").unwrap();
        let row = secrets_table.rows.get("encrypted1").unwrap();
        assert!(row.encrypted);
        assert_eq!(row.data.get("message").unwrap(), &DataValue::Text("Secret".to_string()));
    }
}

fn main() -> io::Result<()> {
    // For manual testing, create a dummy Database with both encrypted and unencrypted rows.
    let mut db = Database::default();
    
    let mut table1 = Table::default();
    table1.columns = vec!["username".to_string(), "email".to_string()];
    let mut row1_data = HashMap::new();
    row1_data.insert("username".to_string(), DataValue::Text("bob".to_string()));
    row1_data.insert("email".to_string(), DataValue::Text("bob@example.com".to_string()));
    table1.rows.insert("user1".to_string(), Row { data: row1_data, encrypted: false });
    db.tables.insert("accounts".to_string(), table1);

    let mut table2 = Table::default();
    table2.columns = vec!["message".to_string()];
    let mut row2_data = HashMap::new();
    row2_data.insert("message".to_string(), DataValue::Text("This is secret".to_string()));
    // Mark this row as encrypted.
    table2.rows.insert("msg1".to_string(), Row { data: row2_data, encrypted: true });
    db.tables.insert("messages".to_string(), table2);

    let file_path = "db_test.bin";
    write_database_to_binary(&db, file_path)?;

    let loaded_db = read_database_from_binary(file_path)?;
    println!("Loaded database: {:#?}", loaded_db);

    Ok(())
}