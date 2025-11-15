//! SQL parser utilities for extracting metadata from SQL statements.
//!
//! This module provides lightweight SQL parsing to extract information
//! needed for optimizations, such as the table name from INSERT statements.

use sqlparser::ast::{ObjectName, ObjectNamePart, Statement, TableObject};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

/// Represents a parsed SQL statement with extracted metadata.
///
/// This provides an object-oriented interface for working with parsed SQL,
/// making it easier to extend for DQ statements and other use cases.
pub struct ParsedStatement {
    statement: Statement,
}

impl ParsedStatement {
    /// Parse a SQL statement.
    ///
    /// Returns `None` if the SQL cannot be parsed or contains multiple statements.
    pub fn parse(sql: &str) -> Option<Self> {
        let dialect = GenericDialect {};
        let statements = Parser::parse_sql(&dialect, sql).ok()?;

        if statements.len() != 1 {
            return None;
        }

        Some(Self {
            statement: statements.into_iter().next()?,
        })
    }

    /// Check if this is an INSERT statement.
    pub fn is_insert(&self) -> bool {
        matches!(self.statement, Statement::Insert(_))
    }

    /// Check if this is a query statement (returns results).
    ///
    /// Returns true for SELECT, SHOW, EXPLAIN, and other query statements.
    /// Returns false for INSERT, UPDATE, DELETE, CREATE, ALTER, DROP, etc.
    pub fn is_query(&self) -> bool {
        matches!(
            self.statement,
            Statement::Query(_)
                | Statement::ShowTables { .. }
                | Statement::ShowColumns { .. }
                | Statement::ShowCreate { .. }
                | Statement::ShowVariable { .. }
                | Statement::ShowVariables { .. }
                | Statement::Explain { .. }
                | Statement::ExplainTable { .. }
        )
    }

    /// Get the table name from an INSERT statement.
    ///
    /// Returns the fully qualified table name (with schema if present).
    /// For example:
    /// - "INSERT INTO users ..." returns "users"
    /// - "INSERT INTO schema.users ..." returns "schema.users"
    pub fn get_insert_table_name(&self) -> Option<String> {
        match &self.statement {
            Statement::Insert(insert) => {
                let obj_name = match &insert.table {
                    TableObject::TableName(name) => name,
                    TableObject::TableFunction(_) => return None,
                };
                
                Some(format_object_name(obj_name))
            }
            _ => None,
        }
    }

    /// Check if this is a DQ-related statement.
    ///
    /// This can be extended to detect PRAGMA duckling_queue statements,
    /// duckling_queue schema references, etc.
    pub fn is_dq_statement(&self) -> bool {
        // Placeholder for future DQ statement detection
        // Can be extended to parse:
        // - PRAGMA duckling_queue.flush
        // - CALL duckling_queue_flush()
        // - INSERT INTO duckling_queue.table_name
        false
    }
}

/// Converts an ObjectName to a fully qualified string.
///
/// Preserves the schema prefix if present:
/// - "table" becomes "table"
/// - "schema.table" becomes "schema.table"
/// - "catalog.schema.table" becomes "catalog.schema.table"
fn format_object_name(obj_name: &ObjectName) -> String {
    obj_name
        .0
        .iter()
        .filter_map(|part| match part {
            ObjectNamePart::Identifier(ident) => Some(ident.value.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join(".")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parsed_statement_simple_insert() {
        let sql = "INSERT INTO users VALUES (1, 'Alice')";
        let parsed = ParsedStatement::parse(sql).expect("should parse");
        assert!(parsed.is_insert());
        assert_eq!(parsed.get_insert_table_name(), Some("users".to_string()));
    }

    #[test]
    fn test_parsed_statement_insert_with_columns() {
        let sql = "INSERT INTO users (id, name) VALUES (1, 'Alice')";
        let parsed = ParsedStatement::parse(sql).expect("should parse");
        assert!(parsed.is_insert());
        assert_eq!(parsed.get_insert_table_name(), Some("users".to_string()));
    }

    #[test]
    fn test_parsed_statement_schema_qualified() {
        let sql = "INSERT INTO public.users (id, name) VALUES (1, 'Alice')";
        let parsed = ParsedStatement::parse(sql).expect("should parse");
        assert!(parsed.is_insert());
        // Schema should be preserved
        assert_eq!(
            parsed.get_insert_table_name(),
            Some("public.users".to_string())
        );
    }

    #[test]
    fn test_parsed_statement_duckling_queue() {
        let sql = "INSERT INTO duckling_queue.events VALUES (1, 'test')";
        let parsed = ParsedStatement::parse(sql).expect("should parse");
        assert!(parsed.is_insert());
        // Schema should be preserved
        assert_eq!(
            parsed.get_insert_table_name(),
            Some("duckling_queue.events".to_string())
        );
    }

    #[test]
    fn test_parsed_statement_not_insert() {
        let sql = "SELECT * FROM users";
        let parsed = ParsedStatement::parse(sql).expect("should parse");
        assert!(!parsed.is_insert());
        assert_eq!(parsed.get_insert_table_name(), None);
    }

    #[test]
    fn test_parsed_statement_invalid_sql() {
        let sql = "INVALID SQL";
        assert!(ParsedStatement::parse(sql).is_none());
    }

    #[test]
    fn test_parsed_statement_multiple_statements() {
        let sql = "INSERT INTO users VALUES (1, 'Alice'); INSERT INTO users VALUES (2, 'Bob');";
        // Should reject multiple statements
        assert!(ParsedStatement::parse(sql).is_none());
    }
}
