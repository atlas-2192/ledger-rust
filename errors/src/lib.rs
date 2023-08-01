use std::io;
use thiserror::Error;

#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum LedgerError {
    #[error("Wrong command received")]
    WrongCommandError,
    #[error("Error while network_protocol interaction")]
    NetworkError,
    #[error("Serializing error")]
    SerializeError,
    #[error("Deserializing error")]
    DeserializationError,
    #[error("Error while processing block")]
    BlockError,
    #[error("Genesis block already exists")]
    GenesisBlockError,
    #[error("Synchronization error")]
    SyncError,
    #[error("Persistence error")]
    PersistenceError,
    #[error("API error")]
    ApiError,
    #[error("Insufficient funds")]
    InsufficientFunds,
    #[error("No such asset")] // TODO ugly name
    NoSuchAsset,
    
}

