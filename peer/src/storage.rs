use ursa::keys::PublicKey;
use state::{Accounts, Assets, Block, MAX_TRANSACTIONS_IN_BLOCK};
use network::{Data, serialize_data};

use crypto;
use errors::LedgerError;
use utils::{print_bytes, convert_timestamp_to_day_time};

#[derive(Debug)]
pub(crate) struct Storage {
    blockchain: Vec<Block>,  // TODO persistence
    accounts: Accounts,
    /// Key is a tuple of format (account_id, asset_id)
    assets: Assets,
    pub(crate) connector_rx: Option<tokio::sync::mpsc::Receiver<Data>>
}

impl Storage {

    pub fn new() -> Self {
        Self {
            blockchain: Default::default(),
            accounts: Default::default(),
            assets: Default::default(),
            connector_rx: None,
        }
    }

    pub fn try_add_block(&mut self, mut block: Block) -> Result<(), LedgerError> {
        for commands in block.transactions.iter().map(|transaction| &transaction.commands) {
            for command in commands {
                command.execute(&mut self.accounts, &mut self.assets);
            }
        };
        self.blockchain.push(block);  // TODO consensus
        Ok(())
    }

    pub fn get_blockchain_by_ref(&self) -> &Vec<Block> {
        &self.blockchain
    }

    fn validate_block(&self, block: &Block, previous_block: &Block) -> bool {
        if block.id - previous_block.id != 1 {
            println!("invalid block id: {}", &block.id);
            return false
        }
        if previous_block.hash != block.hash {
            println!("invalid previous block hash: {}", print_bytes(&previous_block.hash));
            return false
        }
        if block.transactions.len() > MAX_TRANSACTIONS_IN_BLOCK {
            println!("transactions count exceeded: {}", &block.transactions.len());
            return false
        }
        if convert_timestamp_to_day_time(block.timestamp)
            <=
           convert_timestamp_to_day_time(previous_block.timestamp) {
            println!("invalid block timestamp: {}", &block.timestamp);
            return false
        }
        if !Self::validate_hash(&block) {
            println!("invalid block hash: {}", print_bytes(&block.hash));
            return false
        }
        // TODO check signature ?
        // if !Self::validate_signature(&block.signature) {
        //     println!("invalid block signature: {}", print_bytes(&block.signature));
        //     return false
        // }
        true
    }

    fn validate_hash(block: &Block) -> bool {
        let hash = crypto::hash(serialize_data(block).as_slice());
        hash == block.hash
    }

    // fn validate_signature(signature: &Vec<u8>, public_key: &PublicKey) -> bool {
    //     true
    // }

    fn validate_chain(&self, remote_block_chain: Vec<Block>) -> bool {
        for (i, _) in remote_block_chain.iter().enumerate() {
            if i == 0 {
                continue
            }
            let previous_block = remote_block_chain.get(i-1).unwrap();
            let current_block = remote_block_chain.get(i).unwrap();
            if !Self::validate_block(self, current_block, previous_block) {
                return false
            }
        }
        true
    }
}

