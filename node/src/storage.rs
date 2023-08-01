use tracing::{debug, error, info};
use state::{Accounts, Asset, Assets, Block, MAX_TRANSACTIONS_IN_BLOCK, NATIVE_COIN};

use crypto;
use crypto::hash;
use errors::LedgerError;
use utils::{print_bytes, convert_timestamp_to_day_time};

#[derive(Debug)]
pub(crate) struct Storage {
    id: u64,
    blockchain: Vec<Block>,  // TODO persistence
    accounts: Accounts,
    /// Key is a tuple of format (account_id, asset_id)
    assets: Assets
}

impl Storage {

    pub fn new(id: u64,) -> Self {
        Self {
            id,
            blockchain: Default::default(),
            accounts: Default::default(),
            assets: Default::default(),
        }
    }

    pub fn try_add_block(&mut self, block: Block) -> Result<(), LedgerError> {
        debug!("storage id: {}", &self.id);
        let previous_block = self.blockchain.last();
        if previous_block.is_some() && &block.id != &0u64 {
            let previous_block = previous_block.unwrap();
            return if Self::validate_block(self, &block, previous_block) {
                // TODO      persistence < --- > state in memory???
                for commands in block.transactions.iter().map(|transaction| &transaction.commands) {
                    for command in commands {
                        command.execute(&mut self.accounts, &mut self.assets)?
                    }
                };
                let block_id = block.id;
                self.blockchain.push(block);
                info!("Block with id {} added to node {} blockchain", block_id, self.id);
                Self::reward_for_mined_block(self);
                Ok(())
            } else {
                Err(LedgerError::BlockError)
            }
        } else if previous_block.is_none() {
            Self::try_add_genesis_block(self, block)?;
        }
        Ok(())
    }

    pub fn get_blockchain_by_ref(&self) -> &Vec<Block> {
        &self.blockchain
    }

    fn try_add_genesis_block(&mut self, block: Block) -> Result<(), LedgerError>  {
        if block.previous_block_hash.is_some() {
            return Err(LedgerError::BlockError)
        }
        if block.id > 1 {
            error!("invalid block id: {}", &block.id);
            return Err(LedgerError::BlockError)
        }
        if block.previous_block_hash.is_some() {
            error!("this is not genesis block");
            return Err(LedgerError::BlockError)
        }
        if block.transactions.len() > MAX_TRANSACTIONS_IN_BLOCK {
            error!("transactions count exceeded: {}", &block.transactions.len());
            return Err(LedgerError::BlockError)
        }
        if !Self::validate_hash(&block) {
            error!("invalid block hash: {}", print_bytes(&block.hash));
            return Err(LedgerError::BlockError)
        }
        let block_id = block.id;
        self.blockchain.push(block);
        info!("Genesis block with id {} added to node {} blockchain", block_id, self.id);
        Ok(())
    }

    fn validate_block(&self, block: &Block, previous_block: &Block) -> bool {
        // if block.id != previous_block.id + 1 {
        //     error!("invalid block id: {}", &block.id);
        //     return false
        // }
        if &previous_block.hash != block.previous_block_hash.as_ref().unwrap() {
            error!("invalid previous block hash: {}", print_bytes(&previous_block.hash));
            return false
        }
        if block.transactions.len() > MAX_TRANSACTIONS_IN_BLOCK {
            error!("transactions count exceeded: {}", &block.transactions.len());
            return false
        }
        if convert_timestamp_to_day_time(block.timestamp)
            <=
           convert_timestamp_to_day_time(previous_block.timestamp) {
            error!("invalid block timestamp: {}", &block.timestamp);
            return false
        }
        if !Self::validate_hash(&block) {
            error!("invalid block hash: {}", print_bytes(&block.hash));
            return false
        }

        true
    }

    fn validate_hash(block: &Block) -> bool {
        let mut block_to_validate = block.clone();
        block_to_validate.hash = vec![];
        block_to_validate.nonce -= 1;
        let hash_data = bincode::serialize::<Block>(&block_to_validate).unwrap();
        let h = hash(hash_data.as_slice());
        block_to_validate.hash.extend_from_slice(&h);
        block_to_validate.hash == block.hash
    }

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

    fn reward_for_mined_block(&mut self) {
        self.assets.insert((1, String::from(NATIVE_COIN)), Asset::new_with_value(1));
    }
}

