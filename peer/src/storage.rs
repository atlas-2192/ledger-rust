use state::{Accounts, Assets, Block};
use network::serialize_block;

use crypto;

#[derive(Debug, Clone)]
pub struct Storage {
    blockchain: Vec<Block>,  // TODO persistency
    uncommitted_blocks: Vec<Block>,
    accounts: Accounts,
    /// Key is a tuple of format (account_id, asset_id)
    assets: Assets,
}

impl Storage {

    pub fn new() -> Self {
        Self {
            blockchain: Default::default(),
            uncommitted_blocks: Default::default(),
            accounts: Default::default(),
            assets: Default::default(),
        }
    }

    pub fn add_block(&mut self, mut block: Block) {
        for commands in block.transactions.iter().map(|transaction| &transaction.commands) {
            for command in commands {
                command.execute(&mut self.accounts, &mut self.assets);
            }
        }
        if let Some(last_block) = self.blockchain.last() {
            block.previous_block_hash = Some(crypto::hash(last_block.to_string().as_bytes()));
        }
        //self.blockchain.push(block);  // TODO consensus
        self.uncommitted_blocks.push(block);
    }

    pub fn get_uncommitted_blocks(&self) -> Vec<Block> {
        self.uncommitted_blocks.clone()
    }
}

fn validate_block(block: Block) -> bool {
    true
}
