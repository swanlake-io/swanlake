//! SQL parser utilities for extracting metadata from SQL statements.
//!
//! This module provides lightweight SQL parsing to extract information
//! needed for optimizations, such as the table name from INSERT statements.

use sqlparser::ast::{ObjectName, Statement, TableObject};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

/// Represents a parsed SQL statement with extracted metadata.
///
/// This provides an object-oriented interface for working with parsed SQL,
/// making it easier to extend for DQ statements and other use cases.
pub struct ParsedStatement {
    statement: Statement,
}

pub struct TableReference {
    sql_name: String,
    logical_name: String,
}

impl TableReference {
    pub fn sql_name(&self) -> &str {
        &self.sql_name
    }

    pub fn logical_name(&self) -> &str {
        &self.logical_name
    }
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
    /// Returns true for SELECT, SHOW, EXPLAIN, PRAGMA, and other query statements.
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
                | Statement::Pragma { .. }
        )
    }

    /// Get the table name from an INSERT statement, including quoting info.
    pub fn get_insert_table(&self) -> Option<TableReference> {
        match &self.statement {
            Statement::Insert(insert) => match &insert.table {
                TableObject::TableName(name) => Some(TableReference::from_object_name(name)),
                TableObject::TableFunction(_) => None,
            },
            _ => None,
        }
    }

    /// Convenience wrapper returning the unquoted, dot-separated table name.
    #[allow(dead_code)]
    pub fn get_insert_table_name(&self) -> Option<String> {
        self.get_insert_table()
            .map(|table| table.logical_name().to_string())
    }

    /// Get the column names from an INSERT statement.
    ///
    /// Returns the list of column names if specified in the INSERT statement.
    /// For example:
    /// - "INSERT INTO users (id, name) VALUES (1, 'Alice')" returns ["id", "name"]
    /// - "INSERT INTO users VALUES (1, 'Alice')" returns None (no explicit columns)
    pub fn get_insert_columns(&self) -> Option<Vec<String>> {
        match &self.statement {
            Statement::Insert(insert) => {
                if insert.columns.is_empty() {
                    None
                } else {
                    Some(
                        insert
                            .columns
                            .iter()
                            .map(|ident| ident.value.clone())
                            .collect(),
                    )
                }
            }
            _ => None,
        }
    }

    /// Check if this is a DQ-related statement.
    ///
    /// This can be extended to detect PRAGMA duckling_queue statements,
    /// duckling_queue schema references, etc.
    #[allow(dead_code)]
    pub fn is_dq_statement(&self) -> bool {
        // Placeholder for future DQ statement detection
        // Can be extended to parse:
        // - PRAGMA duckling_queue.flush
        // - CALL duckling_queue_flush()
        // - INSERT INTO duckling_queue.table_name
        false
    }
}

impl TableReference {
    fn from_object_name(name: &ObjectName) -> Self {
        let sql_name = name.to_string();
        let logical_name = name
            .0
            .iter()
            .map(|part| {
                part.as_ident()
                    .map(|ident| ident.value.clone())
                    .unwrap_or_else(|| part.to_string())
            })
            .collect::<Vec<_>>()
            .join(".");
        Self {
            sql_name,
            logical_name,
        }
    }
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
    fn test_parsed_statement_preserves_quotes() {
        let sql = r#"INSERT INTO "CamelCase" VALUES (1)"#;
        let parsed = ParsedStatement::parse(sql).expect("should parse");
        assert_eq!(
            parsed.get_insert_table_name(),
            Some("CamelCase".to_string())
        );
        let table_ref = parsed.get_insert_table().expect("should have table");
        assert_eq!(table_ref.sql_name(), r#""CamelCase""#);
        assert_eq!(table_ref.logical_name(), "CamelCase");
    }

    #[test]
    fn test_parsed_statement_preserves_schema_and_quotes() {
        let sql = r#"INSERT INTO analytics."Monthly Sales" VALUES (1)"#;
        let parsed = ParsedStatement::parse(sql).expect("should parse");
        assert_eq!(
            parsed.get_insert_table_name(),
            Some("analytics.Monthly Sales".to_string())
        );
        let table_ref = parsed.get_insert_table().expect("should have table");
        assert_eq!(table_ref.sql_name(), r#"analytics."Monthly Sales""#);
        assert_eq!(table_ref.logical_name(), "analytics.Monthly Sales");
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
