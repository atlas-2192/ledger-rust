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
use crate::connector::{Connect, Connector};

const LOCAL_HOST: &str = "127.0.0.1:";

pub struct Node {
    miner: Miner,
    sender: Sender,
    receiver: Receiver,
}

impl Node {

    pub async fn start(local_port: &str) { //&mut self
        let address = SocketAddr::from_str((String::from(LOCAL_HOST) + local_port).as_str()).unwrap();

        let mut receiver = Receiver::new(address).await;
        let mut sender = Sender::new();
        let mut miner = Miner::new();

        let connector = Arc::new(Mutex::new(Connector::new()));
        let c1 = connector.clone();
        let c2 = connector.clone();
        let c3 = connector.clone();

        miner.connect(c1).await;
        sender.connect(c2).await;
        receiver.connect(c3).await;

        tokio::spawn(async move { receiver.run().await } );
        tokio::spawn(async move { sender.run().await } );
        tokio::spawn(async move { miner.run().await } );
    }
}