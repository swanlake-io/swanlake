use adbc_core::options::{OptionConnection, OptionValue};
use adbc_core::{Connection, Optionable, Statement};
use adbc_driver_manager::{ManagedConnection, ManagedStatement};
use anyhow::Result;
use arrow_array::RecordBatch;

use crate::client::{QueryResult, UpdateResult};

pub(crate) fn execute_query(conn: &mut ManagedConnection, sql: &str) -> Result<QueryResult> {
    let mut stmt = conn.new_statement()?;
    stmt.set_sql_query(sql)?;
    execute_statement_query(&mut stmt)
}

pub(crate) fn execute_query_with_params(
    conn: &mut ManagedConnection,
    sql: &str,
    params: RecordBatch,
) -> Result<QueryResult> {
    let mut stmt = conn.new_statement()?;
    stmt.set_sql_query(sql)?;
    stmt.prepare()?;
    stmt.bind(params)?;
    execute_statement_query(&mut stmt)
}

pub(crate) fn execute_update(conn: &mut ManagedConnection, sql: &str) -> Result<UpdateResult> {
    let mut stmt = conn.new_statement()?;
    stmt.set_sql_query(sql)?;
    let rows_affected = stmt.execute_update()?;
    Ok(UpdateResult { rows_affected })
}

pub(crate) fn execute_update_with_batch(
    conn: &mut ManagedConnection,
    sql: &str,
    batch: RecordBatch,
) -> Result<UpdateResult> {
    let mut stmt = conn.new_statement()?;
    stmt.set_sql_query(sql)?;
    stmt.prepare()?;
    stmt.bind(batch)?;
    let rows_affected = stmt.execute_update()?;
    Ok(UpdateResult { rows_affected })
}

pub(crate) fn execute_statement_query(stmt: &mut ManagedStatement) -> Result<QueryResult> {
    let reader = stmt.execute()?;
    let mut batches = Vec::new();
    for batch in reader {
        batches.push(batch?);
    }
    Ok(QueryResult::new(batches))
}

pub(crate) fn run_healthcheck(conn: &mut ManagedConnection, sql: &str) -> Result<()> {
    let sql = sql.trim();
    if sql.is_empty() {
        return Ok(());
    }
    let mut stmt = conn.new_statement()?;
    stmt.set_sql_query(sql)?;
    let reader = stmt.execute()?;
    for batch in reader {
        batch?;
    }
    Ok(())
}

pub(crate) fn begin_transaction(conn: &mut ManagedConnection) -> Result<()> {
    conn.set_option(OptionConnection::AutoCommit, OptionValue::from("false"))?;
    Ok(())
}

pub(crate) fn commit_transaction(conn: &mut ManagedConnection) -> Result<()> {
    conn.commit()?;
    conn.set_option(OptionConnection::AutoCommit, OptionValue::from("true"))?;
    Ok(())
}

pub(crate) fn rollback_transaction(conn: &mut ManagedConnection) -> Result<()> {
    conn.rollback()?;
    conn.set_option(OptionConnection::AutoCommit, OptionValue::from("true"))?;
    Ok(())
}
