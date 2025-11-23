use arrow_flight::sql::{
    ActionBeginTransactionRequest, ActionBeginTransactionResult, ActionEndTransactionRequest,
};
use tonic::{Request, Status};
use tracing::{error, field::display, info};

use crate::service::SwanFlightSqlService;
use crate::session::id::TransactionId;

/// Starts a transaction for the current session via FlightSQL action.
pub(crate) async fn do_action_begin_transaction(
    service: &SwanFlightSqlService,
    _query: ActionBeginTransactionRequest,
    request: Request<arrow_flight::Action>,
) -> Result<ActionBeginTransactionResult, Status> {
    let session = service.prepare_request(&request).await?;

    let session_clone = session.clone();
    let transaction_id = tokio::task::spawn_blocking(move || session_clone.begin_transaction())
        .await
        .map_err(SwanFlightSqlService::status_from_join)?
        .map_err(SwanFlightSqlService::status_from_error)?;

    info!(transaction_id = %transaction_id, "transaction started in session");

    Ok(ActionBeginTransactionResult {
        transaction_id: transaction_id.into(),
    })
}

/// Commits or rolls back a session transaction based on the action flag,
/// tolerating autocommit scenarios where the operation becomes a no-op.
pub(crate) async fn do_action_end_transaction(
    service: &SwanFlightSqlService,
    query: ActionEndTransactionRequest,
    request: Request<arrow_flight::Action>,
) -> Result<(), Status> {
    let session = service.prepare_request(&request).await?;

    let transaction_id = match TransactionId::from_bytes(&query.transaction_id) {
        Some(id) => id,
        None => {
            error!(
                handle_len = query.transaction_id.len(),
                "invalid transaction ID received from client"
            );
            return Err(Status::invalid_argument("invalid transaction ID"));
        }
    };
    tracing::Span::current().record("transaction_id", display(transaction_id));

    let action = query.action;

    let session_clone = session.clone();
    let txn_id_clone = transaction_id;

    let commit_result = tokio::task::spawn_blocking(move || {
        if action == 1 {
            session_clone.commit_transaction(txn_id_clone)
        } else {
            session_clone.rollback_transaction(txn_id_clone)
        }
    })
    .await
    .map_err(SwanFlightSqlService::status_from_join)?
    .map_err(SwanFlightSqlService::status_from_error);

    match commit_result {
        Ok(()) => {}
        Err(status) => {
            if action == 1
                && status
                    .message()
                    .contains("Cannot commit when autocommit is enabled")
            {
                info!(
                    transaction_id = %transaction_id,
                    "commit requested while autocommit enabled; treated as no-op"
                );
            } else if action != 1
                && status
                    .message()
                    .contains("cannot rollback when autocommit is enabled")
            {
                info!(
                    transaction_id = %transaction_id,
                    "rollback requested while autocommit enabled; treated as no-op"
                );
            } else {
                return Err(status);
            }
        }
    }

    let op = if action == 1 {
        "committed"
    } else {
        "rolled back"
    };
    info!(transaction_id = %transaction_id, op, "transaction completed in session");

    Ok(())
}
