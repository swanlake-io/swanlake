use crate::CliArgs;
use anyhow::Result;

pub mod appender_insert;
pub mod execute_query_commands;
pub mod parameter_types;
pub mod prepared_statements;
pub mod transaction_recovery;

pub async fn run_all(args: &CliArgs) -> Result<()> {
    parameter_types::run_parameter_types(args).await?;
    execute_query_commands::run_execute_query_commands(args).await?;
    prepared_statements::run_prepared_statements(args).await?;
    appender_insert::run(args).await?;
    transaction_recovery::run_transaction_recovery(args).await?;
    Ok(())
}
