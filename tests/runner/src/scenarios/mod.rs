use crate::CliArgs;
use anyhow::Result;

pub mod appender_insert;
pub mod execute_query_commands;
pub mod parameter_types;
pub mod prepared_statements;
pub mod transaction_recovery;

pub fn run_all(args: &CliArgs) -> Result<()> {
    parameter_types::run_parameter_types(args)?;
    execute_query_commands::run_execute_query_commands(args)?;
    prepared_statements::run_prepared_statements(args)?;
    appender_insert::run(args)?;
    transaction_recovery::run_transaction_recovery(args)?;
    Ok(())
}
