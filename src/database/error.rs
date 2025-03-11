use thiserror::Error;

#[allow(dead_code)]
#[derive(Debug, Error)]
pub enum DbError {
    #[error("Rejected Embedding due to missing payload")]
    FoundationDb(foundationdb::FdbError),
    #[error("Nonretryable FdbError")]
    Retryable {
        message: &'static str,
        maybe_commited: bool,
        retryable_not_commited: bool,
    },
    #[error("Rejected Embedding due to missing payload")]
    NonRetryable {
        message: &'static str,
        maybe_commited: bool,
    },
    #[error("Rejected Embedding due to missing payload")]
    ReferenceToTxnKept(foundationdb::FdbBindingError),
    #[error("Rejected Embedding due to missing payload")]
    PayloadMissing,
    #[error("Rejected Embedding due to missing vector")]
    VectorMissing,
}

impl From<foundationdb::FdbError> for DbError {
    fn from(error: foundationdb::FdbError) -> Self {
        if error.is_retryable() {
            Self::Retryable {
                message: error.message(),
                maybe_commited: error.is_maybe_committed(),
                retryable_not_commited: error.is_retryable_not_committed(),
            }
        } else {
            Self::NonRetryable {
                message: error.message(),
                maybe_commited: error.is_maybe_committed(),
            }
        }
    }
}
