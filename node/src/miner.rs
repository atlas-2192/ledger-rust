use std::collections::BinaryHeap;
use std::sync::{Arc};
use tokio::sync::mpsc::{
    channel,
    Receiver as Rx,
    Sender as Tx
};
use std::time::Duration;
use tokio::sync::{Mutex};
use chrono::{Timelike, Utc};
//use futures::channel::mpsc;
//use futures::channel::mpsc::{Sender, Receiver};
//use futures::channel::mpsc::Sender;
use ursa::keys::PublicKey;
use ursa::keys::PrivateKey;
use ursa::signatures::ed25519::Ed25519Sha512;
use ursa::signatures::SignatureScheme;
use crypto::Hash;
use network::Data;
use state::{Block, Transaction};
use utils::print_bytes;
use async_trait::async_trait;
use blake2::Digest;
use tracing::{debug, error, info, trace, warn};
use crate::connector::{Connect, Connector};
use crate::storage::Storage;

#[derive(Debug)]
pub(crate) struct Miner {
    id: u64,
    public_key: PublicKey,
    private_key: PrivateKey,
    transaction_pool: Arc<Mutex<BinaryHeap<Transaction>>>,
    pub(crate) storage: Arc<Mutex<Storage>>,
    pub(crate) connector_rx: Arc<Mutex<Option<Rx<Data>>>>,
    pub(crate) connector_tx: Arc<Mutex<Option<Tx<Data>>>>,
}

impl Miner {

    pub fn new(id: u64) -> Self {
        let (public_key, private_key) = Ed25519Sha512::new().keypair(None).unwrap();
        Self {
            id,
            public_key,
            private_key,
            transaction_pool: Arc::new(Mutex::new(BinaryHeap::new())),
            storage: Arc::new(Mutex::new(Storage::new())),
            connector_rx: Arc::new(Mutex::new(None)),
            connector_tx: Arc::new(Mutex::new(None)),
        }
    }

    pub async fn run(&self) {
        let connector_tx = self.connector_tx.clone();
        let connector_rx = self.connector_rx.clone();
        let storage1 = self.storage.clone();
        let storage2 = self.storage.clone();
        let transaction_pool_1 = self.transaction_pool.clone();
        let transaction_pool_2 = self.transaction_pool.clone();
        let id = self.id;
        let private_key = self.private_key.clone();
        tokio::spawn(async move {
            Self::run_listening(
                id,
                connector_rx,
                storage1,
                transaction_pool_1)
                .await
        });
        tokio::spawn(async move {
            Self::run_mining(
                id,
                connector_tx,
                storage2,
                &private_key,
                transaction_pool_2)
                .await
        });
    }

    async fn run_listening(
        id: u64,
        connector_rx: Arc<Mutex<Option<Rx<Data>>>>,
        storage: Arc<Mutex<Storage>>,
        transaction_pool: Arc<Mutex<BinaryHeap<Transaction>>>)
    {
        loop {
            let connector_rx = connector_rx.clone();
            let mut connector_rx = connector_rx.lock().await;
            let connector_rx = connector_rx.as_mut().unwrap();
            while let Some(data) = connector_rx.recv().await {
                match data {
                    // receive block from other node
                    Data::Block(block) => {
                        debug!("miner id: {}", id);
                        info!("block has been received from another node, \
                        block id: {}, block hash: {}", &block.id, print_bytes(&block.hash));
                        let storage = storage.clone();
                        let mut storage = storage.lock().await;
                        let added_block = storage.try_add_block(block);
                        if added_block.is_err() {
                            println!("error while adding block: {}", added_block.err().unwrap())
                        }
                    }
                    // receive transaction from client
                    Data::Transaction(transaction) => {
                        let mut transactions;
                        loop {
                            match transaction_pool.try_lock() {
                                Ok(mutex_guard) => {
                                    transactions = mutex_guard;
                                    break;
                                }
                                Err(_) => {
                                    tokio::time::sleep(Duration::from_secs(3)).await;
                                }
                            }
                        }
                        transactions.push(transaction);
                    }
                    _ => { error!("received wrong data type") }
                }
            }
        }
    }

    async fn run_mining(
        id: u64,
        connector_tx: Arc<Mutex<Option<Tx<Data>>>>,
        storage: Arc<Mutex<Storage>>,
        private_key: &PrivateKey,
        transaction_pool: Arc<Mutex<BinaryHeap<Transaction>>>
    ) {
        // mine block from received transactions
        loop {
            let storage = storage.clone();
            let mut storage_lock = storage.lock().await;
            let previous_block = storage_lock.get_blockchain_by_ref().last();
            // if let Some(b) = previous_block.as_ref() {
            //     debug!("previous_block: {}", &b);
            // }
            let previous_block_id;
            let previous_block_hash;
            match previous_block {
                None => {
                    previous_block_id = None;
                    previous_block_hash = None;
                },
                Some(p_b) => {
                    debug!("miner_id: {}, previous_block id: {}", id, &p_b.id);
                    previous_block_id = Some(p_b.id);
                    debug!("miner_id: {}, previous_block hash: {}", id, print_bytes(&p_b.hash));
                    previous_block_hash = Some(p_b.hash.clone());
                }
            };
            drop(storage_lock);
            let private_key = private_key.clone();
            let transaction_pool = transaction_pool.clone();
            let ready_to_mine = async move {
                let mut transactions;
                loop {
                    match transaction_pool.try_lock() {
                        Ok(mutex_guard) => {
                            transactions = mutex_guard;
                            if transactions.len() < 10 {
                                drop(transactions);
                                tokio::time::sleep(Duration::from_secs(5)).await;
                                continue
                            };
                            let mut ready_transactions = Vec::with_capacity(10);
                            for _ in 0..10 {
                                let transaction = transactions.pop().unwrap();
                                ready_transactions.push(transaction);
                            }
                            //info!("10 transactions are ready to mine");
                            return ready_transactions
                        }
                        Err(_) => {
                            tokio::time::sleep(Duration::from_secs(2)).await;
                        }
                    }
                }
            }
                .await;
            //debug!("10 transactions have been received from transaction pool");
            debug!("mining block started, miner_id: {}", id);
            let block = tokio::task::spawn_blocking(move || {
                Self::mine_block(
                    private_key,
                    2,
                    previous_block_hash,
                    previous_block_id,
                    ready_to_mine)
            })
                .await
                .unwrap();
            info!("miner_id: {}, block has been mined, block: \n {}", id, &block);
            let mut storage = storage.lock().await;
            let added_block = storage.try_add_block(block.clone());
            drop(storage);
            if added_block.is_err() {
                let err = added_block.err().unwrap();
                error!("miner_id: {id}, failed to add self-mined block: {}", err)
            } else {
                let connector_tx = connector_tx.clone();
                let mut connector_tx = connector_tx.lock().await;
                let connector_tx = connector_tx.as_mut().unwrap();
                let data = Data::Block(block);
                let sent_block = connector_tx.send(data).await;
                if sent_block.is_err() {
                    error!("error while sending block to connector: {}", sent_block.err().unwrap())
                } else {
                    //trace!("block sent to connector, miner_id: {}", id);
                }
            }
        }
    }

    /// FOR TEST
    // async fn add_transaction_to_pool(transaction_pool: Arc<Mutex<TransactionPool>>,
    //                                  transaction: Transaction) {
    //     transaction_pool.lock().await.add_transaction_to_pool(transaction);
    // }

    /// FOR TEST ONLY
    async fn add_block_to_storage(&mut self, block: Block) {
        let mut storage = self.storage.lock().await;
        if let Err(_) = storage.try_add_block(block) {
           error!("could not add block to storage")
        };
    }

    fn mine_block(
        private_key: PrivateKey,
        target_hash_zero_count: usize,
        previous_block_hash: Option<Hash>,
        previous_block_id: Option<u64>,
        transactions: Vec<Transaction>)
        -> Block
    {
        let mut id = 0;
        if previous_block_id.is_some() {
            id = previous_block_id.unwrap() + 1;
        };
        let timestamp = Utc::now().timestamp();
        let signature = Ed25519Sha512::new()
            .sign(format!("{:?}", &transactions).as_bytes(), &private_key)
            .unwrap();
        let mut nonce = 0;
        let mut hash = vec![];
        let mut block = Default::default();
        let start = Utc::now();
        while !is_hash_valid(&hash, target_hash_zero_count) {  // TODO concurrent calculation
            let mut hasher = crypto::hasher();
            block = Block {
                id,
                timestamp,
                nonce,
                signature: signature.clone(),
                hash: vec![],
                previous_block_hash: previous_block_hash.clone(),
                transactions: transactions.clone()
            };
            let hash_data = bincode::serialize(&block).unwrap();
            hasher.update(hash_data.as_slice());
            let h = hasher.finalize();
            hash.clear();
            hash.extend_from_slice(&h);
            nonce += 1;
        };
        let finish = Utc::now();
        //info!("valid block hash has been found, total time = {} sec", finish.second() - start.second());
        info!("hash: {}, nonce: {}", print_bytes(&hash), &nonce);
        block.nonce = nonce;
        block.hash = hash;
        info!("block: {}", &block);
        block
    }
}

fn is_hash_valid(hash: &Hash, target_hash_zero_count: usize) -> bool {
    hash.iter().take_while(|n| **n == 0u8).count() >= target_hash_zero_count
}

#[async_trait]
impl Connect for Miner {
    async fn connect(&mut self, connector: Arc<Mutex<Connector>>) {
        let mut connector = connector.lock().await;
        let (tx1, rx1): (Tx<Data>, Rx<Data>) = channel(10);
        let (tx2, rx2): (Tx<Data>, Rx<Data>) = channel(10);
        self.connector_rx = Arc::new(Mutex::new(Some(rx1)));
        connector.miner_tx = Arc::new(Mutex::new(Some(tx1)));
        self.connector_tx = Arc::new(Mutex::new(Some(tx2)));
        connector.miner_rx = Arc::new(Mutex::new(Some(rx2)));
    }
}

// fn hash_data(transactions: &Vec<Transaction>) -> Vec<u8> {
//     Vec::from(transactions.iter()
//         .map(|t| t.to_string())
//         .reduce(|acc, t| acc + " " + t.as_str())
//         .unwrap()
//         .as_bytes())
//
// }

#[cfg(test)]
mod tests {
    use std::hash::Hash;
    use std::sync::Arc;
    use blake2::Digest;
    use rand::prelude::*;
    use chrono::Utc;
    use crypto::hash;
    use state::{Block, Command, Transaction};
    use ursa::signatures::ed25519::Ed25519Sha512;
    use ursa::signatures::SignatureScheme;
    use utils::{LOCAL_HOST, print_bytes};
    use crate::miner::{ Miner};
    use crate::storage::Storage;
    use tokio::sync::{Mutex};
    use tracing::info;

    #[test]
    fn validate_block_hash() {
        // for _ in 0..10 {
        //     let block = generate_block(
        //         11134,
        //         vec![
        //             generate_transaction(),
        //             generate_transaction(),
        //             generate_transaction()
        //         ]);
        //     let block_hash = hash(block.to_string().as_bytes());
        //     info!("block hash is {}", print_bytes(&block_hash));
        // }

        let block = generate_block(
                11134,
                vec![
                    generate_transaction(),
                    generate_transaction(),
                    generate_transaction()
                ]);
        let block_hash1 = hash(block.to_string().as_bytes());
        info!("block hash1 is {}", print_bytes(&block_hash1));
        let block_hash2 = hash(block.to_string().as_bytes());
        info!("block hash2 is {}", print_bytes(&block_hash2));

        // let mut block_to_check = block.clone();
        // block_to_check.hash = vec![];
        // let hash = hash(block_to_check.to_string().as_bytes());
        //hash == block.hash
        //assert_eq!(&block_hash, &block.hash)
    }

    #[tokio::test]
    async fn mine_block_succeed() {
        let miner = Miner::new(1);
        miner.run().await;
        let previous_block_transactions = vec![generate_transaction()];
        let previous_block = generate_block(2, previous_block_transactions);
        let current_block_transactions = vec![generate_transaction()];
        let private_key = miner.private_key.clone();
        let block = Miner::mine_block(
            private_key,
            2,
            Some(previous_block.hash),
            Some(previous_block.id),
            current_block_transactions);
        assert!(&block.hash.starts_with(&[0, 0]))
    }

    #[tokio::test]
    async fn receive_transactions_and_start_mine_block_succeed() {
        let miner = Arc::new(Mutex::new(Miner::new(1)));
        let miner1 = miner.clone();
        let miner3 = miner.clone();
        let previous_block_transactions = vec![generate_transaction(), generate_transaction()];
        let previous_block = generate_block(2, previous_block_transactions);
        tokio::spawn(async move {
            let mut miner2 = miner1.lock().await;
            miner2.add_block_to_storage(previous_block).await;
            let miner4 = miner.clone();
            tokio::spawn(async move {
                let miner4 = miner4.lock().await;
                miner4.run().await; }
            );
        });
        tokio::spawn(async move {
            for _ in 0..10 {
                let mut miner3 = miner3.lock().await;
                //miner3.add_transaction_to_pool(generate_transaction());
            }
        })
            .await
            .expect("Could not add transactions to transaction pool");
    }

    fn generate_block(nonce: u32, transactions: Vec<Transaction>) -> Block {
        let (_, private_key) = Ed25519Sha512::new().keypair(None).unwrap();

        let signature = Ed25519Sha512::new()
            .sign(format!("{:?}", &transactions).as_bytes(), &private_key)
            .unwrap();

        let mut block = Block {
            id: 7,
            timestamp: Utc::now().timestamp(),
            nonce,
            signature,
            hash: vec![],
            previous_block_hash: Some(String::from("0004f4544324323323").as_bytes().to_vec()),
            transactions,
        };
        let hash_data = bincode::serialize::<Block>(&block).unwrap();
        let hash = hash(hash_data.as_slice());
        println!("block hash : {}", print_bytes(&hash));
        block.hash = hash;
        block
    }

    fn generate_transaction() -> Transaction {
        let mut rng = thread_rng();
        let n1: u8 = rng.gen_range(0..2); // command variant
        let mut n2: u8 = rng.gen_range(2..4); // number of commands in transaction
        let mut commands = vec![];

        while n2 > 0 {
            let command: Command;
            match n1 {
                0 => {
                    let (public_key, _) = Ed25519Sha512::new().keypair(None).unwrap();
                    command = Command::CreateAccount { public_key: public_key.to_string() }
                }
                1 => {
                    command = Command::AddFunds {
                    account_id: rng.gen_range(0..100),
                    value: rng.gen_range(0..1000),
                    asset_id: "TEST".to_string() }
                }
                2 => {
                    command = Command::TransferFunds {
                    account_from_id: rng.gen_range(0..100),
                    account_to_id: rng.gen_range(0..100),
                    value: rng.gen_range(0..1000),
                    asset_id: "TEST2".to_string() }
                }
                _ => { unreachable!() }
            };
            println!("command: {}", &command);
            commands.push(command);
            n2 -= 1;
        }

        Transaction {
            fee: 111,
            commands,
        }
    }
}

