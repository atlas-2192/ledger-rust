use std::collections::HashMap;
use std::marker::PhantomData;
use crate::crypto::Hash;
// use serde::ser::Serialize;
// use serde::de::Deserialize;
use serde::{Deserialize, Serialize};

mod crypto;

#[derive(Debug, Clone)]
pub struct Account {
    pub public_key: String,
}

pub type Accounts = HashMap<u32, Account>;

#[derive(Debug, Clone)]
pub struct Asset {
    pub value: i32,
}

pub type Assets = HashMap<(u32, String), Asset>;

// pub struct Data<DataType = Transaction> {
//     pub data: DataType,
//     //state: PhantomData<DataType>
// }
//
// impl<DataType> Data<DataType> {
//
//     pub fn new(data: DataType) -> Self {
//         Self {data}
//     }
//
//     pub fn get_data(self) -> Self {
//         self
//     }
// }

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Transaction {
    pub command: Vec<Command>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Command {
    CreateAccount {
        public_key: String,
    },
    AddFunds {
        account_id: u32,
        value: i32,
        asset_id: String,
    },
}

// impl<DATA> Data<DATA> for Command {
//     fn get_data(self) -> DATA {
//         todo!()
//     }
// }

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Block {
    pub data: Vec<Transaction>,
    pub signature: Vec<u8>,
    pub previous_block_hash: Option<Hash>,
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
            }
        }
    }
}