use std::cmp::Ordering;
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use crypto::Hash;
use serde::{Deserialize, Serialize};
use derive_more::Display;
use errors::LedgerError;
use utils::print_bytes;

pub const MAX_TRANSACTIONS_IN_BLOCK: usize = 100; // TODO constraint size of block
pub const NATIVE_COIN: &str = "NATIVE";

#[derive(Debug, Clone)]
pub struct Account {
    public_key: String,
}

pub type Accounts = HashMap<u32, Account>;

#[derive(Debug, Clone)]
pub struct Asset {
    value: u32,
}

impl Asset {
    pub fn new_with_value(value: u32) -> Self {
        Self { value }
    }
}

pub type Assets = HashMap<(u32, String), Asset>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Transaction {
    pub fee: u32,
    pub commands: Vec<Command>,
}

unsafe impl Send for Transaction {}
unsafe impl Sync for Transaction {}

impl Eq for Transaction {}

impl PartialEq<Self> for Transaction {
    fn eq(&self, other: &Self) -> bool {
        self.fee == other.fee
    }
}

impl PartialOrd<Self> for Transaction {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Transaction {
    fn cmp(&self, other: &Self) -> Ordering {
        let difference = i64::from(self.fee.clone()) - i64::from(other.fee.clone());
        match difference {
            d if d > 0 => Ordering::Greater,
            d if d < 0 => Ordering::Less,
            0 => Ordering::Equal,
            _ => unreachable!()
        }
    }
}

impl Display for Transaction {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "transaction: fee : {}, commands: {}", self.fee, self.commands.iter()
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
        value: u32,
        asset_id: String,
    },
    #[display(fmt = "account_from_id: {}, account_to_id: {} value: {}, asset_id: {}",
                     account_from_id,     account_to_id,    value,     asset_id)]
    TransferFunds {
        account_from_id: u32,
        account_to_id: u32,
        value: u32,
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
               print_bytes(&self.previous_block_hash.clone().unwrap_or("None".as_bytes().to_vec())),
               self.transactions.iter()
                    .map(|c| c.to_string())
                    .reduce(|acc, c| acc + " " + c.as_str())
                    .unwrap())
    }
}

impl Command {
    pub fn execute(&self,
                   accounts: &mut Accounts,
                   assets: &mut Assets)
        -> Result<(), LedgerError>
    {
        match self {
            Self::CreateAccount { public_key } => {
                accounts.insert(
                    (accounts.len() + 1) as u32,
                    Account {
                        public_key: public_key.clone(),
                    },
                );
                Ok(())
            }
            Self::AddFunds {
                account_id,
                value,
                asset_id,
            } => {
                assets.insert((account_id.clone(), asset_id.clone()), Asset { value: value.clone() });
                Ok(())
            },

            Self::TransferFunds {
                account_from_id,
                account_to_id,
                value,
                asset_id
            } => {
                if let Some(account_asset) = assets.get(&(account_from_id.clone(), asset_id.clone())) {
                    if account_asset.value < *value {
                        return Err(LedgerError::InsufficientFunds)
                    }
                    assets.remove(&(account_from_id.clone(), asset_id.clone()));
                    assets.insert((account_to_id.clone(), asset_id.clone()), Asset { value: value.clone() });
                    Ok(())
                } else {
                    Err(LedgerError::NoSuchAsset)
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {

}