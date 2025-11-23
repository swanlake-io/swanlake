//! SQL parser utilities for extracting metadata from SQL statements.
//!
//! This module provides lightweight SQL parsing to extract information
//! needed for optimizations, such as the table name from INSERT statements.

use std::ops::ControlFlow;

use sqlparser::ast::{visit_relations, ObjectName, ObjectNamePart, Statement, TableObject};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

/// Represents a parsed SQL statement with extracted metadata.
///
/// This provides an object-oriented interface for working with parsed SQL,
/// making it easier to extend for DQ statements and other use cases.
pub struct ParsedStatement {
    statement: Statement,
}

/// Reference to a table in SQL with parsed components.
///
/// - `parts`: Unquoted components split by dots for catalog/schema/table extraction
///   (e.g., `["analytics", "Monthly Sales"]`)
pub struct TableReference {
    parts: Vec<String>,
}

impl TableReference {
    pub fn parts(&self) -> &[String] {
        &self.parts
    }
}

impl ParsedStatement {
    /// Parse a SQL statement.
    ///
    /// For multi-statement SQL, returns the last statement (which determines the result type).
    /// Returns `None` if the SQL cannot be parsed.
    pub fn parse(sql: &str) -> Option<Self> {
        let dialect = GenericDialect {};
        let statements = Parser::parse_sql(&dialect, sql).ok()?;

        Some(Self {
            statement: statements.into_iter().last()?,
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

    /// Return the SQL for the INSERT source (VALUES/SELECT).
    pub fn insert_source_sql(&self) -> Option<String> {
        match &self.statement {
            Statement::Insert(insert) => insert.source.as_ref().map(|query| query.to_string()),
            _ => None,
        }
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

    /// Returns true if any relation in the statement references the duckling_queue schema.
    pub fn references_duckling_queue_relation(&self) -> bool {
        statement_references_duckling_queue(&self.statement)
    }

    /// Returns true if INSERT uses plain VALUES and every value is a placeholder (e.g., ?, $1).
    ///
    /// Used to decide whether DoPut batches contain all data or whether server-side expressions/defaults
    /// must be evaluated by executing the SQL.
    pub fn insert_values_all_placeholders(&self) -> bool {
        match &self.statement {
            Statement::Insert(insert) => match &insert.source {
                Some(query) => match &*query.body {
                    sqlparser::ast::SetExpr::Values(values) => values.rows.iter().all(|row| {
                        row.iter().all(|e| {
                            matches!(
                                e,
                                sqlparser::ast::Expr::Value(sqlparser::ast::ValueWithSpan {
                                    value: sqlparser::ast::Value::Placeholder(_),
                                    ..
                                })
                            )
                        })
                    }),
                    _ => false,
                },
                None => false,
            },
            _ => false,
        }
    }
}

impl TableReference {
    fn from_object_name(name: &ObjectName) -> Self {
        let parts = name
            .0
            .iter()
            .map(|part| {
                part.as_ident()
                    .map(|ident| ident.value.clone())
                    .unwrap_or_else(|| part.to_string())
            })
            .collect::<Vec<_>>();
        Self { parts }
    }
}

fn statement_references_duckling_queue(statement: &Statement) -> bool {
    let mut found = false;
    let _ = visit_relations(statement, |relation| {
        if relation
            .0
            .first()
            .and_then(ObjectNamePart::as_ident)
            .is_some_and(|ident| ident.value.eq_ignore_ascii_case("duckling_queue"))
        {
            found = true;
            ControlFlow::Break(())
        } else {
            ControlFlow::Continue(())
        }
    });
    found
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parsed_statement_simple_insert() {
        let sql = "INSERT INTO users VALUES (1, 'Alice')";
        let parsed = ParsedStatement::parse(sql).expect("should parse");
        assert!(parsed.is_insert());
        let table_ref = parsed.get_insert_table().expect("should have table");
        assert_eq!(table_ref.parts(), &["users"]);
    }

    #[test]
    fn test_parsed_statement_insert_with_columns() {
        let sql = "INSERT INTO users (id, name) VALUES (1, 'Alice')";
        let parsed = ParsedStatement::parse(sql).expect("should parse");
        assert!(parsed.is_insert());
        let table_ref = parsed.get_insert_table().expect("should have table");
        assert_eq!(table_ref.parts(), &["users"]);
    }

    #[test]
    fn test_parsed_statement_schema_qualified() {
        let sql = "INSERT INTO public.users (id, name) VALUES (1, 'Alice')";
        let parsed = ParsedStatement::parse(sql).expect("should parse");
        assert!(parsed.is_insert());
        let table_ref = parsed.get_insert_table().expect("should have table");
        assert_eq!(table_ref.parts(), &["public", "users"]);
    }

    #[test]
    fn test_parsed_statement_duckling_queue() {
        let sql = "INSERT INTO duckling_queue.events VALUES (1, 'test')";
        let parsed = ParsedStatement::parse(sql).expect("should parse");
        assert!(parsed.is_insert());
        let table_ref = parsed.get_insert_table().expect("should have table");
        assert_eq!(table_ref.parts(), &["duckling_queue", "events"]);
    }

    #[test]
    fn test_parsed_statement_preserves_quotes() {
        let sql = r#"INSERT INTO "CamelCase" VALUES (1)"#;
        let parsed = ParsedStatement::parse(sql).expect("should parse");
        let table_ref = parsed.get_insert_table().expect("should have table");
        assert_eq!(table_ref.parts(), &["CamelCase"]);
    }

    #[test]
    fn test_parsed_statement_preserves_schema_and_quotes() {
        let sql = r#"INSERT INTO analytics."Monthly Sales" VALUES (1)"#;
        let parsed = ParsedStatement::parse(sql).expect("should parse");
        let table_ref = parsed.get_insert_table().expect("should have table");
        assert_eq!(table_ref.parts(), &["analytics", "Monthly Sales"]);
    }

    #[test]
    fn test_parsed_statement_not_insert() {
        let sql = "SELECT * FROM users";
        let parsed = ParsedStatement::parse(sql).expect("should parse");
        assert!(!parsed.is_insert());
        assert!(parsed.get_insert_table().is_none());
    }

    #[test]
    fn test_parsed_statement_invalid_sql() {
        let sql = "INVALID SQL";
        assert!(ParsedStatement::parse(sql).is_none());
    }

    #[test]
    fn test_parsed_statement_multiple_statements() {
        let sql = "INSERT INTO users VALUES (1, 'Alice'); INSERT INTO users VALUES (2, 'Bob');";
        // Should parse and use the last statement
        let parsed = ParsedStatement::parse(sql).expect("should parse");
        assert!(parsed.is_insert());
    }

    #[test]
    fn test_parsed_statement_multi_statement_query() {
        let sql = "USE swanlake; SHOW TABLES;";
        let parsed = ParsedStatement::parse(sql).expect("should parse");
        assert!(parsed.is_query());
    }

    #[test]
    fn test_parsed_statement_multi_statement_uses_last() {
        let sql = "SELECT 1; INSERT INTO users VALUES (1, 'Alice');";
        let parsed = ParsedStatement::parse(sql).expect("should parse");
        // Last statement is INSERT, so is_query should be false
        assert!(parsed.is_insert());
        assert!(!parsed.is_query());
    }

    #[test]
    fn test_detects_duckling_queue_in_select() {
        let sql = "SELECT * FROM duckling_queue.events";
        let parsed = ParsedStatement::parse(sql).expect("should parse");
        assert!(parsed.references_duckling_queue_relation());
    }

    #[test]
    fn test_detects_duckling_queue_in_insert_source() {
        let sql = "INSERT INTO analytics.events SELECT * FROM duckling_queue.buffer";
        let parsed = ParsedStatement::parse(sql).expect("should parse");
        assert!(parsed.references_duckling_queue_relation());
    }

    #[test]
    fn test_duckling_queue_not_detected_in_literals() {
        let sql = "SELECT 'duckling_queue' AS label";
        let parsed = ParsedStatement::parse(sql).expect("should parse");
        assert!(!parsed.references_duckling_queue_relation());
    }
}
