use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::sync::Mutex;
//use std::sync::Mutex;
use network::{Data, SendEvent, serialize_data};
use state::Block;
use crate::connector::{Connect, Connector};
use async_trait::async_trait;
use tokio::sync::mpsc::{
    channel,
    Receiver as Rx,
    Sender as Tx
};

#[derive(Debug)]
pub(crate) struct Sender {
    peers: Peers,
    pub(crate) connector_rx: Option<tokio::sync::mpsc::Receiver<Data>>,
}

impl Sender {

    pub fn new() -> Self {
        Self {
            peers: Peers::new(),
            connector_rx: None
        }
    }

    pub async fn start(&mut self) {
        loop {
            let peers = &self.peers;
            if let Some(connector_rx) = self.connector_rx.as_mut() {
                while let Some(data) = connector_rx.recv().await {
                    match data {
                        Data::Block(block) => {
                            Self::send_block_to_network(peers, block).await;
                        }
                        Data::Transaction(_) => {
                            println!("error: transaction is not intended to be sent by peer")
                        }
                        Data::Peer(_) => { todo!() }
                        Data::Peers(_) => { todo!() }
                    }
                }
            }
        }
    }

    async fn send_block_to_network(peers: &Peers, block: Block) {
        for (_, socket_addr) in peers.addresses.iter() {
            let block = block.clone();
            let socket = TcpStream::connect(socket_addr).await;
            if let Ok(socket) = socket {
                tokio::spawn(async move {
                    let data = Data::Block(block);
                    let res = network::send_data(
                        &socket,
                        serialize_data(&data), SendEvent::SendBlock)
                        .await;
                    if res.is_err() {
                        println!("error while sending block to peer: {}", res.err().unwrap());
                    }
                });
            } else {
                println!("could not establish connection: {}", socket_addr)
            }
        }
    }
}

#[async_trait]
impl Connect for Sender {
    async fn connect(&mut self, connector: Arc<Mutex<Connector>>) {
        let mut connector = connector.lock().await;
        let (tx, rx): (Tx<Data>, Rx<Data>) = channel(10);
        self.connector_rx = Some(rx);
        connector.sender_tx = Arc::new(Mutex::new(Some(tx)));
    }
}

#[derive(Debug)]
struct Peers {
    addresses: HashMap<u32, SocketAddr>,
}

impl Peers {
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