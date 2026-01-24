//! SQL parser utilities for extracting metadata from SQL statements.
//!
//! This module provides lightweight SQL parsing to extract information
//! needed for optimizations, such as the table name from INSERT statements.

use sqlparser::ast::{
    AssignmentTarget, Expr, ObjectName, Query, Select, SetExpr, Statement, TableFactor,
    TableObject, TableWithJoins, Value, ValueWithSpan,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

/// Represents a parsed SQL statement with extracted metadata.
///
/// This provides an object-oriented interface for working with parsed SQL
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

    /// Get the primary table reference for this statement when it is a simple
    /// INSERT/UPDATE/DELETE/SELECT against a single table.
    pub fn get_statement_table(&self) -> Option<TableReference> {
        match &self.statement {
            Statement::Insert(insert) => match &insert.table {
                TableObject::TableName(name) => Some(TableReference::from_object_name(name)),
                TableObject::TableFunction(_) => None,
            },
            Statement::Update(update) => table_from_joins(&update.table),
            Statement::Delete(delete) => table_from_from_table(&delete.from),
            Statement::Query(query) => table_from_query(query),
            _ => None,
        }
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

    /// Return column names mapped to placeholders in statement order for simple
    /// UPDATE/DELETE/SELECT statements.
    pub fn parameter_columns(&self) -> Option<Vec<String>> {
        let mut columns: Vec<Option<String>> = Vec::new();
        match &self.statement {
            Statement::Update(update) => {
                for assignment in &update.assignments {
                    let col = column_from_assignment_target(&assignment.target);
                    collect_placeholders_with_column(&assignment.value, col, &mut columns);
                }
                if let Some(selection) = &update.selection {
                    collect_params_from_expr(selection, &mut columns);
                }
            }
            Statement::Delete(delete) => {
                if let Some(selection) = &delete.selection {
                    collect_params_from_expr(selection, &mut columns);
                }
            }
            Statement::Query(query) => {
                if let Some(selection) = selection_from_query(query) {
                    collect_params_from_expr(selection, &mut columns);
                }
            }
            _ => return None,
        }

        if columns.iter().all(|col| col.is_some()) {
            Some(columns.into_iter().flatten().collect())
        } else {
            None
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

fn table_from_query(query: &Query) -> Option<TableReference> {
    let select = match &*query.body {
        SetExpr::Select(select) => select.as_ref(),
        _ => return None,
    };
    table_from_select(select)
}

fn table_from_select(select: &Select) -> Option<TableReference> {
    if select.from.len() != 1 {
        return None;
    }
    let table = select.from.first()?;
    table_from_joins(table)
}

fn table_from_joins(table: &TableWithJoins) -> Option<TableReference> {
    match &table.relation {
        TableFactor::Table { name, .. } => Some(TableReference::from_object_name(name)),
        _ => None,
    }
}

fn table_from_from_table(from: &sqlparser::ast::FromTable) -> Option<TableReference> {
    let tables = match from {
        sqlparser::ast::FromTable::WithFromKeyword(tables) => tables,
        sqlparser::ast::FromTable::WithoutKeyword(tables) => tables,
    };
    if tables.len() != 1 {
        return None;
    }
    table_from_joins(&tables[0])
}

fn selection_from_query(query: &Query) -> Option<&Expr> {
    match &*query.body {
        SetExpr::Select(select) => select.selection.as_ref(),
        _ => None,
    }
}

fn column_name(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Identifier(ident) => Some(ident.value.clone()),
        Expr::CompoundIdentifier(idents) => idents.last().map(|ident| ident.value.clone()),
        _ => None,
    }
}

fn column_from_assignment_target(target: &AssignmentTarget) -> Option<String> {
    match target {
        AssignmentTarget::ColumnName(name) => object_name_last_part(name),
        AssignmentTarget::Tuple(columns) => columns.first().and_then(object_name_last_part),
    }
}

fn object_name_last_part(name: &ObjectName) -> Option<String> {
    name.0.last().map(|part| {
        part.as_ident()
            .map(|ident| ident.value.clone())
            .unwrap_or_else(|| part.to_string())
    })
}

fn collect_placeholders_with_column(
    expr: &Expr,
    column: Option<String>,
    out: &mut Vec<Option<String>>,
) {
    match expr {
        Expr::Value(ValueWithSpan {
            value: Value::Placeholder(_),
            ..
        }) => out.push(column),
        Expr::Nested(inner) => collect_placeholders_with_column(inner, column, out),
        Expr::Cast { expr, .. } => collect_placeholders_with_column(expr, column, out),
        Expr::BinaryOp { left, right, .. } => {
            collect_placeholders_with_column(left, column.clone(), out);
            collect_placeholders_with_column(right, column, out);
        }
        Expr::Between { low, high, .. } => {
            collect_placeholders_with_column(low, column.clone(), out);
            collect_placeholders_with_column(high, column, out);
        }
        Expr::InList { list, .. } => {
            for item in list {
                collect_placeholders_with_column(item, column.clone(), out);
            }
        }
        _ => {}
    }
}

fn collect_params_from_expr(expr: &Expr, out: &mut Vec<Option<String>>) {
    match expr {
        Expr::BinaryOp { left, right, .. } => {
            if let Some(col) = column_name(left) {
                let before = out.len();
                collect_placeholders_with_column(right, Some(col.clone()), out);
                if out.len() != before {
                    return;
                }
            }
            if let Some(col) = column_name(right) {
                let before = out.len();
                collect_placeholders_with_column(left, Some(col.clone()), out);
                if out.len() != before {
                    return;
                }
            }
            collect_params_from_expr(left, out);
            collect_params_from_expr(right, out);
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            if let Some(col) = column_name(expr) {
                collect_placeholders_with_column(low, Some(col.clone()), out);
                collect_placeholders_with_column(high, Some(col), out);
            } else {
                collect_params_from_expr(expr, out);
                collect_params_from_expr(low, out);
                collect_params_from_expr(high, out);
            }
        }
        Expr::InList { expr, list, .. } => {
            if let Some(col) = column_name(expr) {
                for item in list {
                    collect_placeholders_with_column(item, Some(col.clone()), out);
                }
            } else {
                collect_params_from_expr(expr, out);
                for item in list {
                    collect_params_from_expr(item, out);
                }
            }
        }
        Expr::Nested(inner) => collect_params_from_expr(inner, out),
        Expr::Cast { expr, .. } => collect_params_from_expr(expr, out),
        Expr::Value(ValueWithSpan {
            value: Value::Placeholder(_),
            ..
        }) => out.push(None),
        _ => {}
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
    fn test_parameter_columns_select_where() {
        let sql = "SELECT * FROM usertable WHERE ycsb_key = ?";
        let parsed = ParsedStatement::parse(sql).expect("should parse");
        let cols = parsed.parameter_columns().expect("should infer columns");
        assert_eq!(cols, vec!["ycsb_key"]);
    }

    #[test]
    fn test_parameter_columns_between() {
        let sql = "SELECT * FROM usertable WHERE ycsb_key > ? AND ycsb_key < ?";
        let parsed = ParsedStatement::parse(sql).expect("should parse");
        let cols = parsed.parameter_columns().expect("should infer columns");
        assert_eq!(cols, vec!["ycsb_key", "ycsb_key"]);
    }

    #[test]
    fn test_parameter_columns_update() {
        let sql = "UPDATE usertable SET field1=?, field2=? WHERE ycsb_key=?";
        let parsed = ParsedStatement::parse(sql).expect("should parse");
        let cols = parsed.parameter_columns().expect("should infer columns");
        assert_eq!(cols, vec!["field1", "field2", "ycsb_key"]);
    }
}
