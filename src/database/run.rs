/*pub async fn run<F, Fut, T>(&self, closure: F) -> Result<T, DbError>
where
    F: Fn(RetryableTransaction, MaybeCommitted) -> Fut,
    Fut: Future<Output = Result<T, DbError>>,
{
    let mut maybe_committed_transaction = false;
    let mut transaction = RetryableTransaction::new(self.fdb.create_trx()?);
    loop {
        let result_closure = closure(
            transaction.clone(),
            MaybeCommitted(maybe_committed_transaction),
        )
        .await;
        if let Err(e) = result_closure {
            match e {
                DbError::FoundationDb(fdb_error) => {
                    maybe_committed_transaction = fdb_error.is_maybe_committed();
                    match transaction.on_error(e).await {
                        // we can retry the error
                        Ok(Ok(t)) => {
                            transaction = t;
                            continue;
                        }
                        Ok(Err(non_retryable_error)) => {
                            return Err(DbError::NonRetryable(non_retryable_error));
                        }
                        // The only FdbBindingError that can be thrown here is `ReferenceToTransactionKept`
                        Err(non_retryable_error) => {
                            return Err(DbError::ReferenceToTxnKept(non_retryable_error));
                        }
                    }
                    todo!()
                }
                _ => (),
            }
        }
    }
    todo!()
}*/
#[derive(Clone)]
pub struct RetryableTransaction {
    inner: std::sync::Arc<Transaction>,
}
impl RetryableTransaction {
    pub(crate) fn new(t: Transaction) -> RetryableTransaction {
        RetryableTransaction {
            inner: std::sync::Arc::new(t),
        }
    }
    pub(crate) fn take(self) -> Result<Transaction, FdbBindingError> {
        // checking weak references
        if std::sync::Arc::weak_count(&self.inner) != 0 {
            return Err(FdbBindingError::ReferenceToTransactionKept);
        }
        Arc::try_unwrap(self.inner).map_err(|_| FdbBindingError::ReferenceToTransactionKept)
    }

    pub(crate) async fn on_error(
        self,
        err: FdbError,
    ) -> Result<Result<RetryableTransaction, FdbError>, FdbBindingError> {
        Ok(self
            .take()?
            .on_error(err)
            .await
            .map(RetryableTransaction::new))
    }

    pub(crate) async fn commit(
        self,
    ) -> Result<Result<TransactionCommitted, TransactionCommitError>, FdbBindingError> {
        Ok(self.take()?.commit().await)
    }
}
pub struct MaybeCommitted(bool);

/*
pub async fn run<F, Fut, T>(&self, closure: F) -> Result<T, FdbBindingError>
where
    F: Fn(RetryableTransaction, MaybeCommitted) -> Fut,
    Fut: Future<Output = Result<T, FdbBindingError>>,
{
    let mut maybe_committed_transaction = false;
    // we just need to create the transaction once,
    // in case there is a error, it will be reset automatically
    let mut transaction = self.create_retryable_trx()?;

    loop {
        // executing the closure
        let result_closure = closure(
            transaction.clone(),
            MaybeCommitted(maybe_committed_transaction),
        )
        .await;

        if let Err(e) = result_closure {
            // checks if it is an FdbError
            if let Some(e) = e.get_fdb_error() {
                maybe_committed_transaction = e.is_maybe_committed();
                // The closure returned an Error,
                match transaction.on_error(e).await {
                    // we can retry the error
                    Ok(Ok(t)) => {
                        transaction = t;
                        continue;
                    }
                    Ok(Err(non_retryable_error)) => {
                        return Err(FdbBindingError::from(non_retryable_error))
                    }
                    // The only FdbBindingError that can be thrown here is `ReferenceToTransactionKept`
                    Err(non_retryable_error) => return Err(non_retryable_error),
                }
            }
            // Otherwise, it cannot be retried
            return Err(e);
        }

        let commit_result = transaction.commit().await;

        match commit_result {
            // The only FdbBindingError that can be thrown here is `ReferenceToTransactionKept`
            Err(err) => return Err(err),
            Ok(Ok(_)) => return result_closure,
            Ok(Err(transaction_commit_error)) => {
                maybe_committed_transaction = transaction_commit_error.is_maybe_committed();
                // we have an error during commit, checking if it is a retryable error
                match transaction_commit_error.on_error().await {
                    Ok(t) => {
                        transaction = RetryableTransaction::new(t);
                        continue;
                    }
                    Err(non_retryable_error) => {
                        return Err(FdbBindingError::from(non_retryable_error))
                    }
                }
            }
        }
    }
}
*/
