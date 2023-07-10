use std::collections::BinaryHeap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
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
use tokio::sync::{ Mutex};
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
use crate::connector::{Connect, Connector};
use crate::storage::Storage;

#[derive(Debug)]
pub(crate) struct Miner {
    public_key: PublicKey,
    private_key: PrivateKey,
    transaction_pool: TransactionPool, //Arc<Mutex<BinaryHeap<Transaction>>>,
    pub(crate) connector_rx: Option<Rx<Data>>,
    pub(crate) connector_tx: Option<Tx<Data>>,
}

#[derive(Debug)]
struct TransactionPool {
    transactions: Arc<StdMutex<BinaryHeap<Transaction>>>,
}

impl TransactionPool {

    fn new() -> Self {
        Self {
            transactions: Arc::new(StdMutex::new(BinaryHeap::new())),
        }
    }

    async fn add_transaction_to_pool(&mut self, transaction: Transaction) {
        let mut transactions = self.transactions.lock().unwrap();
        transactions.push(transaction);
    }
}

impl Future for TransactionPool {
    type Output = Vec<Transaction>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let transactions = self.transactions.clone();
        let transactions = transactions.lock().unwrap();
        match transactions.len() {
            len if len > 9 => {
                let mut transactions = Vec::with_capacity(10);
                for _ in 0..transactions.len() {
                    let transaction = transactions.pop().unwrap();
                    transactions.push(transaction);
                }
                return Poll::Ready(transactions);
            }
            len if len < 10 => {
                let transactions2 = self.transactions.clone();
                thread::spawn(move || {
                    let mut transactions2 = transactions2.lock().unwrap();
                    while transactions2.len() < 10 {
                        thread::sleep(Duration::from_secs(1));
                    }
                    println!("transactions length >= 10")
                });
                let waker = cx.waker().clone();
                waker.wake();
                return Poll::Pending
            }
            _ => { unreachable!() }
        }
    }
}

impl Miner {

    pub fn new() -> Self {
        let (public_key, private_key) = Ed25519Sha512::new().keypair(None).unwrap();
        Self {
            public_key,
            private_key,
            transaction_pool: TransactionPool::new(),
            connector_rx: None,
            connector_tx: None,
        }
    }

    pub async fn run(&mut self, storage: Arc<StdMutex<Storage>>) {  //  -> StdReceiver<Block>
        let connector_rx = self.connector_rx.as_mut().unwrap();
        loop {
            let storage = storage.clone();
            let storage = storage.lock().unwrap();
            let previous_block = storage.get_blockchain_reference().last().unwrap();
            let previous_block = previous_block.clone();
            let private_key = self.private_key.clone();
            let transactions = self.transaction_pool.transactions.clone();
            let ready_to_mine = tokio::spawn(async move {
                TransactionPool { transactions }
            }
                .await)
                .await
                .unwrap();
            let block = thread::spawn(move || {
                Self::mine_block(
                    private_key,
                    2,
                    previous_block,
                    ready_to_mine)
            })
                .join()
                .unwrap();
            self.connector_tx.unwrap().send(block);
        }
    }

    pub async fn add_transaction_to_pool(&mut self, transaction: Transaction) {
        self.transaction_pool.add_transaction_to_pool(transaction);
    }

    fn mine_block(
        private_key: PrivateKey,
        target_hash_zero_count: usize,
        previous_block: Block,
        transactions: Vec<Transaction>)
        -> Block
    {
        let id = previous_block.id + 1;
        let timestamp = Utc::now().timestamp();
        let signature = Ed25519Sha512::new()
            .sign(format!("{:?}", &transactions).as_bytes(), &private_key)
            .unwrap();
        let mut nonce = 0;
        let mut hash: Hash = previous_block.hash.clone();
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
                previous_block_hash: Some(previous_block.hash.clone()),
                transactions: transactions.clone()
            };
            hasher.update(format!("{:?}", block).as_bytes());
            let h = hasher.finalize();
            hash.clear();
            hash.extend_from_slice(&h);
            nonce += 1;
        };
        let finish = Utc::now();
        println!("success!!!, total time = {} sec", finish.second() - start.second());
        println!("hash: {}, nonce: {}", print_bytes(&hash), &nonce);
        block.nonce = nonce;
        block.hash = hash;
        println!("block: {}", &block);
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
        self.connector_rx = Some(rx1);
        connector.miner_tx = Arc::new(Mutex::new(Some(tx1)));
        self.connector_tx = Some(tx2);
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
    use utils::print_bytes;
    use crate::miner::{ Miner};
    use crate::storage::Storage;
    use tokio::sync::{ Mutex};

    #[test]
    fn test_mine_block() {
        let storage = Arc::new(Mutex::new(Storage::new()));
        let miner = Miner::new();
        println!("previous block transactions:");
        let previous_block_transactions = generate_transactions();
        let previous_block = generate_block(2, previous_block_transactions);
        println!("current block transactions:");
        let current_block_transactions = generate_transactions();
        let private_key = miner.private_key.clone();
        let block = Miner::mine_block(
            private_key, 2, previous_block, current_block_transactions);
        assert!(&block.hash.starts_with(&[0, 0]))
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

    fn generate_transactions() -> Vec<Transaction> {
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
        let mut transactions = vec![];
        let transaction = Transaction {
            fee: 111,
            commands,
        };
        transactions.push(transaction);
        transactions
    }
}

//
// struct IncomingDataFuture<'a> {
//     queue: &'a Arc<Mutex<Queue<Data>>>,
// }
//
// impl Future for IncomingDataFuture<'_> {
//     type Output = Vec<Data>;
//
//     fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
//         let queue = self.queue.try_lock();
//         if queue.is_ok() {
//             let mut queue = queue.unwrap();
//             match queue.size() {
//                 size if size > 0 => {
//                     let messages_size = match size {
//                         s if s < 10 => s,
//                         s if s > 9 => 10,
//                         _ => unreachable!()
//                     };
//                     let mut messages: Vec<Data> = vec![];
//                     for _ in 0..messages_size {
//                         messages.push(queue.remove().unwrap());
//                     }
//                     return Poll::Ready(messages)
//                 },
//                 size if size == 0 => {
//                     thread::sleep(Duration::from_secs(2)); // TODO notify
//                     let waker = cx.waker().clone();
//                     waker.wake();
//                     return Poll::Pending
//                 }
//                 _ => { unreachable!() }
//             }
//         } else {
//             thread::sleep(Duration::from_secs(2)); // TODO notify
//             cx.waker().wake_by_ref();
//             return Poll::Pending
//         }
//     }
// }

