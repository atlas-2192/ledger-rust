use std::net::{SocketAddr};
use std::str::FromStr;
use std::sync::{Arc};
//use std::sync::Mutex;
use std::time::Duration;
use bincode::serialize;

use tokio::io::{ AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::{Mutex};
use tracing::{debug, error, event, info, Level, span};

use network::{Data, p2p::process_incoming_data, serialize_data};
use network::client2node::{RequestType, node_response};
use state::Block;

use crate::connector::{Connect, Connector};
use crate::miner::Miner;
use crate::receiver::Receiver;
use crate::sender::Sender;
use crate::storage::Storage;

const LOCAL_HOST: &str = "127.0.0.1:";

pub struct Node {
    node_id: u64,
    peer_address: SocketAddr,
    receiver: Arc<Mutex<Receiver>>,
    sender: Arc<Mutex<Sender>>,
    miner: Arc<Mutex<Miner>>,
}

impl Node {

    pub async fn new(node_id: u64, local_port: &str) -> Self {
        let addr = SocketAddr::from_str((String::from(LOCAL_HOST) + local_port).as_str()).unwrap();
        Self {
            node_id,
            peer_address: addr,
            receiver: Arc::new(Mutex::new(Receiver::new(addr).await)),
            sender: Arc::new(Mutex::new(Sender::new(addr))),
            miner: Arc::new(Mutex::new(Miner::new(local_port.parse().unwrap()))),
        }
    }

    pub async fn start(&self) {
        let port = self.peer_address.port();
        let receiver = self.receiver.clone();
        let sender = self.sender.clone();
        let miner = self.miner.clone();

        let receiver1 = receiver.clone();
        let sender1 = sender.clone();
        let miner1 = miner.clone();
        let receiver2 = receiver.clone();
        let sender2 = sender.clone();
        let miner2 = miner.clone();

        let connector = Arc::new(Mutex::new(Connector::new()));
        let c1 = connector.clone();
        let c2 = connector.clone();
        let c3 = connector.clone();

        miner1.lock().await.connect(c1).await;
        sender1.lock().await.connect(c2).await;
        receiver1.lock().await.connect(c3).await;
        connector.lock().await.start().await;

        tokio::spawn(async move {
            let mut receiver = receiver2.lock().await;
            receiver.run().await
        });
        tokio::spawn(async move {
            let mut sender = sender2.lock().await;
            sender.run().await
        });
        tokio::spawn(async move {
            let miner = miner2.lock().await;
            miner.run().await;
        });

        event!(Level::INFO, "node started on 127.0.0.1:{}", port);

        Self::listen_api_requests(self, port).await;
    }

    async fn listen_api_requests(&self, mut port: u16) {
        port += 10;
        let addr = String::from(LOCAL_HOST) + port.to_string().as_str();
        let listener = TcpListener::bind(addr.as_str()).await.unwrap();
        info!("listen_api_requests started on {}", &addr);
        loop {
            if let Ok((mut socket, _)) = listener.accept().await {
                let miner = self.miner.clone();
                if let Err(e) = node_response(
                    &mut socket,
                    |m| get_blockchain_data(m),
                    miner)
                    .await {
                    error!("api request error: {}", e);
                    continue
                }
            }
        }
    }
}

async fn get_blockchain_data(miner: Arc<Mutex<Miner>>, request_type: RequestType) -> Vec<u8>
{
    let miner = miner.lock().await;
    let storage = miner.storage.clone();
    loop {
        match storage.try_lock() {
            Ok(storage) => {
                match request_type {
                    RequestType::NodeBlockchain { height } => {
                        let blockchain = storage.get_blockchain_by_ref();
                        let blockchain_of_required_length =
                            blockchain.iter().rev().take(height as usize).collect::<Vec<&Block>>();
                        return serialize_data(blockchain_of_required_length);
                    }
                    RequestType::Block { ref hash } => { todo!() }
                    RequestType::Transaction { ref hash } => { todo!() }
                }
            }
            Err(_) => {
                debug!("storage is locked yet");
                tokio::time::sleep(Duration::from_millis(300)).await
            }
        }
    }
}