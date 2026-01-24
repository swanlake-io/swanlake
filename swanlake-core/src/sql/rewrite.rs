use sqlparser::ast::Statement;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use tracing::warn;

#[derive(Debug, Clone)]
pub struct SqlRewriteResult {
    pub sql: String,
    pub stripped_select_locks: bool,
}

pub fn strip_select_locks(sql: &str) -> SqlRewriteResult {
    let dialect = GenericDialect {};
    let mut statements = match Parser::parse_sql(&dialect, sql) {
        Ok(statements) => statements,
        Err(_) => {
            return SqlRewriteResult {
                sql: sql.to_string(),
                stripped_select_locks: false,
            }
        }
    };

    let mut stripped = false;
    if let Some(Statement::Query(query)) = statements.last_mut() {
        if !query.locks.is_empty() {
            query.locks.clear();
            stripped = true;
        }
    }

    if !stripped {
        return SqlRewriteResult {
            sql: sql.to_string(),
            stripped_select_locks: false,
        };
    }

    let mut rendered = String::new();
    for (idx, statement) in statements.iter().enumerate() {
        if idx > 0 {
            rendered.push_str("; ");
        }
        rendered.push_str(&statement.to_string());
    }

    warn!(
        original_sql = %sql,
        rewritten_sql = %rendered,
        "SELECT locking clause not supported; stripped FOR UPDATE/SHARE"
    );

    SqlRewriteResult {
        sql: rendered,
        stripped_select_locks: true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strip_select_locks_removes_for_update() {
        let input = "SELECT * FROM usertable WHERE ycsb_key = ? FOR UPDATE";
        let result = strip_select_locks(input);
        assert!(result.stripped_select_locks);
        assert!(!result.sql.to_uppercase().contains("FOR UPDATE"));
    }

    #[test]
    fn strip_select_locks_preserves_other_sql() {
        let input = "SELECT * FROM usertable WHERE ycsb_key = ?";
        let result = strip_select_locks(input);
        assert!(!result.stripped_select_locks);
        assert_eq!(result.sql, input);
    }

    #[test]
    fn strip_select_locks_handles_multi_statement() {
        let input = "USE swanlake; SELECT * FROM usertable FOR UPDATE";
        let result = strip_select_locks(input);
        assert!(result.stripped_select_locks);
        assert_eq!(result.sql, "USE swanlake; SELECT * FROM usertable");
    }
}
