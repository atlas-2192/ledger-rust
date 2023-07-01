use std::collections::HashMap;
use crate::miner::Miner;
use crate::receiver::Receiver;
use crate::sender::Sender;
use crate::storage::Storage;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::Mutex;

const LOCAL_HOST: String = String::from("127.0.0.1:");

pub struct Peer {
    storage: Storage,
    miner: Miner,
    sender: Sender,
    receiver: Receiver,
}

impl Peer {
    #[tokio::main]
    pub async fn start(local_port: &str) {
        let address = SocketAddr::from_str((LOCAL_HOST + local_port).as_str()).unwrap();
        let miner = Miner::new();
        let storage = Arc::new(Mutex::new(Storage::new()));
        let storage1 = storage.clone();
        let mut receiver = Receiver::new(address, storage1).await;
        receiver.run().await;
        let storage2 = storage.clone();
        let initial_peers = InitialPeers::new();
        let mut sender = Sender::new(initial_peers.addresses, storage2);
        sender.run().await;
    }
}

struct InitialPeers {
    addresses: HashMap<u32, SocketAddr>,
}

impl InitialPeers {
    fn new() -> Self {
        Self {
            addresses: HashMap::from([
                (1, SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 1234)),
                (2, SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 1235)),
                (3, SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 1236))
            ])
        }
    }
}