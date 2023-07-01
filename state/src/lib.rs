use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use crypto::Hash;
use serde::{Deserialize, Serialize};
use derive_more::Display;
use utils::print_bytes;

pub const MAX_TRANSACTIONS_IN_BLOCK: usize = 100; // TODO constraint size of block

#[derive(Debug, Clone)]
pub struct Account {
    public_key: String,
}

pub type Accounts = HashMap<u32, Account>;

#[derive(Debug, Clone)]
pub struct Asset {
    value: i32,
}

pub type Assets = HashMap<(u32, String), Asset>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Transaction {
    pub commands: Vec<Command>,
}

impl Display for Transaction {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.commands.iter()
            .map(|c| c.to_string())
            .reduce(|acc, c| acc + " " + c.as_str())
            .unwrap())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Display)]
pub enum Command {
    CreateAccount {
        public_key: String,
    },
    #[display(fmt = "account_id: {}, value: {}, asset_id: {}",
                     account_id,     value,     asset_id)]
    AddFunds {
        account_id: u32,
        value: i32,
        asset_id: String,
    },
    #[display(fmt = "account_from_id: {}, account_to_id: {} value: {}, asset_id: {}",
                     account_from_id,     account_to_id,    value,     asset_id)]
    TransferFunds {
        account_from_id: u32,
        account_to_id: u32,
        value: i32,
        asset_id: String,
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Block {
    pub id: u64,
    pub timestamp: i64,
    pub nonce: u32,
    pub signature: Vec<u8>,
    pub hash: Hash,
    pub previous_block_hash: Option<Hash>,
    pub transactions: Vec<Transaction>
}

impl Display for Block {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "block: \n
                   id: {}, \n
                   timestamp: {}, \n
                   nonce: {}, \n
                   signature: {},  \n
                   hash: {}, \n
                   previous_block_hash: {}, \n
                   transactions: {} \n",
               &self.id,
               &self.timestamp,
               &self.nonce,
               print_bytes(&self.signature),
               print_bytes(&self.hash),
               print_bytes(&self.previous_block_hash.clone().unwrap()),
               self.transactions.iter()
            .map(|c| c.to_string())
            .reduce(|acc, c| acc + " " + c.as_str())
            .unwrap())
    }
}

impl Command {
    pub fn execute(&self, accounts: &mut Accounts, assets: &mut Assets) {
        match self {
            Self::CreateAccount { public_key } => {
                accounts.insert(
                    (accounts.len() + 1) as u32,
                    Account {
                        public_key: public_key.clone(),
                    },
                );
            }
            Self::AddFunds {
                account_id,
                value,
                asset_id,
            } => {
                assets.insert((*account_id, asset_id.clone()), Asset { value: *value });
            },

            Self::TransferFunds {
                account_from_id,
                account_to_id,
                value,
                asset_id
            } => {
                todo!()
            }
        }
    }
}