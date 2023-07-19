use std::borrow::BorrowMut;
use std::collections::HashMap;
use crate::miner::Miner;
use crate::receiver::Receiver;
use crate::sender::Sender;
use crate::storage::Storage;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::ops::DerefMut;
use std::str::FromStr;
use std::sync::{Arc, MutexGuard};
//use std::sync::Mutex;
use std::thread;
use queues::{Queue, queue};
use network::Data;
use tokio::sync::Mutex;

const LOCAL_HOST: &str = "127.0.0.1:";

pub struct Node {
    miner: Miner,
    sender: Sender,
    receiver: Receiver,
}

impl Node {

    // pub async fn new(local_port: &str) -> Self {
    //     let address = SocketAddr::from_str((String::from(LOCAL_HOST) + local_port).as_str()).unwrap();
    //     Self {
    //         storage: Arc::new(Mutex::new(Storage::new())),
    //         miner: Miner::new(),
    //         sender: Sender::new(),
    //         receiver: Receiver::new(address).await
    //     }
    // }

    pub async fn start(local_port: &str) { //&mut self
        let address = SocketAddr::from_str((String::from(LOCAL_HOST) + local_port).as_str()).unwrap();
        let storage = Arc::new(Mutex::new(Storage::new()));
        let mut miner = Miner::new();
        let sender = Sender::new();
        let (data_tx, mut data_rx) = tokio::sync::mpsc::channel::<Data>(10);
        //Receiver::run(address, data_tx).await;
        let storage1 = storage.clone();
        //let block_rx = miner.run(storage1);
        loop {
            let data = data_rx.recv().await;
            if let Some(data) = data {
                match data {
                    Data::Block(block) => {
                        //self.miner.try_add_block(block);
                    }
                    Data::Transaction(transaction) => {
                        miner.add_transaction_to_pool(transaction);
                    }
                    Data::Peer(peer) => {
                        // sender.add_peer(node)
                    }
                    Data::Peers(peers) => {
                        // sender.send_peers(peers) ???
                    }
                }
            }
        }
    }

    // fn storage_guard(&mut self) -> MutexGuard<Storage> {
    //     self.storage.lock().await
    // }
}