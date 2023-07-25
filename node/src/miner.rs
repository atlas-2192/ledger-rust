use std::collections::BinaryHeap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, TryLockResult};
use std::sync::Mutex as StdMutex;
use std::sync::mpsc::{Sender as StdSender, Receiver as StdReceiver};
use std::task::{Context, Poll};
use tokio::sync::mpsc::{
    channel,
    Receiver as Rx,
    Sender as Tx
};
use std::thread;
use std::time::Duration;
use tokio::sync::{Mutex, MutexGuard, TryLockError};
use blake2::{Blake2s, Blake2s256, Digest};
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
use tracing::{error, info, trace, warn};
use crate::connector::{Connect, Connector};
use crate::storage::Storage;

#[derive(Debug)]
pub(crate) struct Miner {
    id: u64,
    public_key: PublicKey,
    private_key: PrivateKey,
    transaction_pool: Arc<Mutex<TransactionPool>>,
    storage: Arc<Mutex<Storage>>,
    pub(crate) connector_rx: Arc<Mutex<Option<Rx<Data>>>>,
    pub(crate) connector_tx: Arc<Mutex<Option<Tx<Data>>>>,
}

#[derive(Debug)]
struct TransactionPool {
    miner_id: u64,
    transactions: Arc<Mutex<BinaryHeap<Transaction>>>,
}

impl TransactionPool {

    fn new(miner_id: u64) -> Self {
        Self {
            miner_id,
            transactions: Arc::new(Mutex::new(BinaryHeap::new())),
        }
    }

    pub fn add_transaction_to_pool(&mut self, transaction: Transaction, id: u64) {
        let transactions = self.transactions.try_lock();
        while let Err(_) = transactions {
            warn!("add_transaction_to_pool() transaction pool lock is already acquired")
        }
        let mut transactions = transactions.unwrap();
        transactions.push(transaction);
        info!("miner id: {}, transactions length: {}", id, transactions.len())
    }

    fn transactions_len(&self, id: u64) -> usize {
        let transactions = self.transactions.try_lock();
        while let Err(_) = transactions {
            warn!("transactions_len() transaction pool lock is already acquired")
        }
        transactions.as_ref().unwrap().len()
    }
}

impl Future for TransactionPool {
    type Output = Vec<Transaction>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let id = self.miner_id;
        //trace!("poll transaction_pool feature , miner_id : {}", &id);
        let len = self.transactions_len(id);
        match len {
            len if len > 9 => {
                info!("transactions length > 9");
                let transactions = self.transactions.clone();
                let transactions = transactions.try_lock();
                return if let Ok(mut transactions) = transactions {
                    let mut ready_transactions = Vec::with_capacity(10);
                    for _ in 0..10 {
                        let transaction = transactions.pop().unwrap();
                        ready_transactions.push(transaction);
                    }
                    info!("transactions are ready to mine");
                    Poll::Ready(ready_transactions)
                } else {
                    cx.waker().clone().wake();
                    Poll::Pending
                }
            }
            len if len < 10 => {
                let _ = thread::spawn(move || {
                    thread::sleep(Duration::from_secs(5));
                })
                    .join();
                cx.waker().clone().wake();
                return Poll::Pending
            }
            _ => { unreachable!() }
        }
    }
}

impl Miner {

    pub fn new(id: u64) -> Self {
        let (public_key, private_key) = Ed25519Sha512::new().keypair(None).unwrap();
        Self {
            id,
            public_key,
            private_key,
            transaction_pool: Arc::new(Mutex::new(TransactionPool::new(id))),
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
                .await;
        }); //.await
        //tokio::spawn(async move {
            Self::run_mining(
                id,
                connector_tx,
                storage2,
                &private_key,
                transaction_pool_2)
                .await;
        //}).await;
    }

    async fn run_listening(
        id: u64,
        connector_rx: Arc<Mutex<Option<Rx<Data>>>>,
        storage: Arc<Mutex<Storage>>,
        transaction_pool: Arc<Mutex<TransactionPool>>)
    {
        loop {
            let connector_rx = connector_rx.clone();
            let mut connector_rx = connector_rx.lock().await;
            let connector_rx = connector_rx.as_mut().unwrap();
            while let Some(data) = connector_rx.recv().await {
                match data {
                    // receive block from other node
                    Data::Block(block) => {
                        info!("received block from other node: {}", &block);
                        let storage = storage.clone();
                        let mut storage = storage.lock().await;
                        let added_block = storage.try_add_block(block);
                        if added_block.is_err() {
                            println!("error while adding block: {}", added_block.err().unwrap())
                        }
                    }
                    // receive transaction from client
                    Data::Transaction(transaction) => {
                        info!("miner_id: {}, received transaction: {}", &id, &transaction);
                        let transaction_pool = transaction_pool.clone();
                        let mut transaction_pool = transaction_pool.lock().await;
                        info!("transaction_pool guard acquired");
                        transaction_pool.add_transaction_to_pool(transaction, id);
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
        transaction_pool: Arc<Mutex<TransactionPool>>
    ) {
        // mine block from received transactions
        loop {
            let storage = storage.clone();
            let mut storage = storage.lock().await;
            let previous_block = storage.get_blockchain_by_ref().last();
            let previous_block_id;
            let previous_block_hash;
            match previous_block {
                None => {
                    previous_block_id = None;
                    previous_block_hash = None;
                },
                Some(p_b) => {
                    previous_block_id = Some(p_b.id);
                    previous_block_hash = Some(p_b.hash.clone());
                }
            };
            let private_key = private_key.clone();
            let transaction_pool = transaction_pool.lock().await;
            let transactions = transaction_pool.transactions.clone();
            let ready_to_mine = async {
                TransactionPool { transactions , miner_id: id}.await
            }
                .await;
            info!("10 transactions were received from transaction pool");
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
            info!("block has been successfully mined");
            let added_block = storage.try_add_block(block.clone());
            if added_block.is_err() {
                trace!("failed to add self-mined block");
            } else {
                let connector_tx = connector_tx.clone();
                let mut connector_tx = connector_tx.lock().await;
                let connector_tx = connector_tx.as_mut().unwrap();
                let data = Data::Block(block);
                let sent_block = connector_tx.send(data).await;
                if sent_block.is_err() {
                    error!("error while sending block to connector: {}", sent_block.err().unwrap())
                } else {
                    trace!("block sent to connector");
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
        let mut hash = vec![0,0,0,0,0,0,0,0,0,0,0,0];
        let mut block = Default::default();
        let start = Utc::now();
        while !is_hash_valid(&hash, target_hash_zero_count) {  // TODO concurrent calculation
            let mut hasher = crypto::hasher();
            block = Block {
                id,
                timestamp,
                nonce,
                signature: signature.clone(),
                hash: hash.clone(),
                previous_block_hash: previous_block_hash.clone(),
                transactions: transactions.clone()
            };
            hasher.update(format!("{:?}", block).as_bytes());
            let h = hasher.finalize();
            hash.clear();
            hash.extend_from_slice(&h);
            nonce += 1;
        };
        let finish = Utc::now();
        info!("success!!!, total time = {} sec", finish.second() - start.second());
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

#[cfg(test)]
mod tests {

    use std::sync::Arc;
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
    use crate::connector::{Connect, Connector};

    #[tokio::test]
    async fn mine_block_succeed() {
        let mut miner = Miner::new(1);
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
                let mut miner4 = miner4.lock().await;
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

    #[test]
    fn receive_transactions_mine_block_send_block() {
        let storage = Arc::new(Mutex::new(Storage::new()));
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
        let hash = hash(&block.to_string().as_bytes());
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

