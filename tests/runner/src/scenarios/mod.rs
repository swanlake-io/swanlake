use crate::CliArgs;
use anyhow::Result;

pub mod appender_insert;
pub mod duckling_queue_auto_load;
pub mod duckling_queue_rotation;
pub mod parameter_types;
pub mod prepared_statements;

pub async fn run_all(args: &CliArgs) -> Result<()> {
    duckling_queue_rotation::run_duckling_queue_rotation(args).await?;
    duckling_queue_auto_load::run_duckling_queue_auto_load(args).await?;
    parameter_types::run_parameter_types(args).await?;
    prepared_statements::run_prepared_statements(args).await?;
    appender_insert::run(args).await?;
    Ok(())
}
