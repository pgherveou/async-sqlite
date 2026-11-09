use super::{done, receive, sql, Operation, Response, Transaction};
use crate::Error;

impl Transaction {
    sql_methods!();

    async fn dispatch(&self, operation: Operation) -> Result<Response, Error> {
        receive(self.client.backend.submit(sql::Request::Sql {
            operation,
            in_transaction: true,
        })?)
        .await
    }

    /// Commits all changes and releases the reserved connection.
    ///
    /// Failure queues rollback. Canceling after commit is admitted may still
    /// commit the changes; a lost reply is never a reason to retry the commit.
    pub async fn commit(self) -> Result<(), Error> {
        self.finish(sql::Request::Commit).await
    }

    /// Rolls back all changes and releases the reserved connection.
    ///
    /// Canceling this future still queues rollback. An automatic SQLite rollback
    /// makes this operation a no-op.
    pub async fn rollback(self) -> Result<(), Error> {
        self.finish(sql::Request::Rollback).await
    }

    async fn finish(mut self, request: sql::Request) -> Result<(), Error> {
        done(receive(self.client.backend.submit(request)?).await?)?;
        self.reservation = None;
        Ok(())
    }
}

impl Drop for Transaction {
    fn drop(&mut self) {
        if self.reservation.is_some() {
            self.client.backend.submit_cleanup(sql::Request::Rollback);
        }
    }
}
