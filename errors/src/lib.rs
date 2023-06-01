use thiserror::Error;

#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum LedgerError {
    #[error("Error processing command")]
    CommandError,
    #[error("Error while network_protocol interaction")]
    NetworkError,
    #[error("Error while processing block")]
    BlockError,
    #[error("Synchronization error")]
    SyncError,
    #[error("Persistence error")]
    PersistenceError,
}

