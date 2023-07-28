use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::str::FromStr;
use std::sync::{Arc, MutexGuard};
//use std::sync::Mutex;
use std::thread;
use std::time::Duration;
use bincode::serialize;

use queues::{Queue, queue};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::{Mutex, TryLockError};
use tracing::{debug, event, info, Level, span};
use tracing::field::debug;

use network::Data;

use crate::connector::{Connect, Connector};
use crate::miner::Miner;
use crate::receiver::Receiver;
use crate::sender::Sender;
use crate::storage::Storage;

const LOCAL_HOST: &str = "127.0.0.1:";

pub struct Node {
    peer_address: SocketAddr,
    receiver: Arc<Mutex<Receiver>>,
    sender: Arc<Mutex<Sender>>,
    miner: Arc<Mutex<Miner>>,
}

impl Node {

    pub async fn new(local_port: &str) -> Self {
        let addr = SocketAddr::from_str((String::from(LOCAL_HOST) + local_port).as_str()).unwrap();
        Self {
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
                let miner = miner.lock().await;
                let storage = miner.storage.clone();
                loop {
                    match storage.try_lock() {
                        Ok(storage) => {
                            let blockchain = storage.get_blockchain_by_ref();
                            let buf = serialize(blockchain).unwrap();
                            let len = socket.write(buf.as_slice()).await.unwrap();
                            debug!("{} bytes has written", len);
                            socket.flush().await.expect("could not flush buffer");
                            break
                        }
                        Err(_) => {
                            debug!("storage is locked yet");
                            tokio::time::sleep(Duration::from_millis(300)).await
                        }
                    }
                }
            }
        }
    }
}

//                 let mut buf: [u8; 1] = [0; 1];
//                 socket.read(&mut buf).await.expect("could not read request command");
//                 if buf[0] != 5 {
//                     let err_resp: [u8; 1] = [0; 1];
//                     socket.write(err_resp.as_slice()).await.expect("could not write error response");
//                     continue
//                 } else {
//                     let resp = [1u8];
//                     socket.write(resp.as_slice()).await.expect("could not write response");
//                 }