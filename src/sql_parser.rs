//! SQL parser utilities for extracting metadata from SQL statements.
//!
//! This module provides lightweight SQL parsing to extract information
//! needed for optimizations, such as the table name from INSERT statements.

use sqlparser::ast::{ObjectName, ObjectNamePart, Statement, TableObject};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

/// Extracts the table name from an INSERT statement.
///
/// Returns `Some(table_name)` if the SQL is a valid INSERT statement,
/// or `None` if it's not an INSERT or cannot be parsed.
///
/// # Examples
///
/// ```ignore
/// let sql = "INSERT INTO users (id, name) VALUES (1, 'Alice')";
/// let table = extract_insert_table_name(sql);
/// assert_eq!(table, Some("users".to_string()));
/// ```
pub fn extract_insert_table_name(sql: &str) -> Option<String> {
    let dialect = GenericDialect {};
    let statements = Parser::parse_sql(&dialect, sql).ok()?;

    if statements.len() != 1 {
        return None;
    }

    match &statements[0] {
        Statement::Insert(insert) => {
            // Extract ObjectName from TableObject
            let obj_name = match &insert.table {
                TableObject::TableName(name) => name,
                TableObject::TableFunction(_) => return None,
            };
            
            let table_name = object_name_to_string(obj_name);
            Some(table_name)
        }
        _ => None,
    }
}

/// Converts an ObjectName (potentially schema-qualified) to a simple string.
///
/// For "schema.table", returns "table".
/// For "table", returns "table".
fn object_name_to_string(obj_name: &ObjectName) -> String {
    // Take the last component (the actual table name)
    obj_name
        .0
        .last()
        .and_then(|part| match part {
            ObjectNamePart::Identifier(ident) => Some(ident.value.clone()),
            _ => None,
        })
        .unwrap_or_default()
}

/// Checks if a SQL statement is an INSERT statement.
pub fn is_insert_statement(sql: &str) -> bool {
    extract_insert_table_name(sql).is_some()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_insert_table_name_simple() {
        let sql = "INSERT INTO users VALUES (1, 'Alice')";
        assert_eq!(
            extract_insert_table_name(sql),
            Some("users".to_string())
        );
    }

    #[test]
    fn test_extract_insert_table_name_with_columns() {
        let sql = "INSERT INTO users (id, name) VALUES (1, 'Alice')";
        assert_eq!(
            extract_insert_table_name(sql),
            Some("users".to_string())
        );
    }

    #[test]
    fn test_extract_insert_table_name_schema_qualified() {
        let sql = "INSERT INTO public.users (id, name) VALUES (1, 'Alice')";
        assert_eq!(
            extract_insert_table_name(sql),
            Some("users".to_string())
        );
    }

    #[test]
    fn test_extract_insert_table_name_duckling_queue() {
        let sql = "INSERT INTO duckling_queue.events VALUES (1, 'test')";
        assert_eq!(
            extract_insert_table_name(sql),
            Some("events".to_string())
        );
    }

    #[test]
    fn test_extract_insert_table_name_not_insert() {
        let sql = "SELECT * FROM users";
        assert_eq!(extract_insert_table_name(sql), None);
    }

    #[test]
    fn test_extract_insert_table_name_invalid_sql() {
        let sql = "INVALID SQL";
        assert_eq!(extract_insert_table_name(sql), None);
    }

    #[test]
    fn test_is_insert_statement() {
        assert!(is_insert_statement(
            "INSERT INTO users VALUES (1, 'Alice')"
        ));
        assert!(!is_insert_statement("SELECT * FROM users"));
        assert!(!is_insert_statement("UPDATE users SET name = 'Bob'"));
    }
}
