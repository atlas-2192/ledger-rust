use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::sync::Mutex;
//use std::sync::Mutex;
use network::{Data, p2p::SendEvent, serialize_data};
use state::Block;
use crate::connector::{Connect, Connector};
use async_trait::async_trait;
use tokio::sync::mpsc::{
    channel,
    Receiver as Rx,
    Sender as Tx
};
use tracing::{error, trace};

#[derive(Debug)]
pub(crate) struct Sender {
    peer_address: SocketAddr,
    peers: Peers,
    pub(crate) connector_rx: Option<tokio::sync::mpsc::Receiver<Data>>,
}

impl Sender {

    pub fn new(peer_address: SocketAddr) -> Self {
        Self {
            peer_address,
            peers: Peers::new(),
            connector_rx: None
        }
    }

    pub async fn run(&mut self) {
        loop {
            let peers = &self.peers;
            let address = &self.peer_address;
            if let Some(connector_rx) = self.connector_rx.as_mut() {
                while let Some(data) = connector_rx.recv().await {
                    match data {
                        Data::Block(block) => {
                            //trace!("get block from connector: {}", &block);
                            Self::send_block_to_network(address, peers, block).await;
                        }
                        Data::Transaction(_) => {
                            error!("error: transaction is not intended to be sent by peer")
                        }
                        Data::Peer(peer) => { todo!() }
                        Data::Peers(peers) => { todo!() }
                        Data::Blockchain(blocks) => { todo!() }
                        Data::NodeResponse(_) => todo!()
                    }
                }
            }
        }
    }

    async fn send_block_to_network(peer_address: &SocketAddr, peers: &Peers, block: Block) {
        for (_, socket_addr) in peers.addresses.iter() {
            if socket_addr.eq(peer_address) {
                continue
            }
            let block = block.clone();
            let socket = TcpStream::connect(socket_addr).await;
            if let Ok(mut socket) = socket {
                tokio::spawn(async move {
                    let res = network::p2p::send_data(
                        &mut socket,
                        serialize_data(&block), SendEvent::SendBlock)
                        .await;
                    if res.is_err() {
                        error!("error while sending block to peer: {}", res.err().unwrap());
                    }
                });
            } else {
                error!("could not establish connection: {}", socket_addr)
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
                (1, utils::socket_addr("1234")),
                (2, utils::socket_addr("1235")),
                (3, utils::socket_addr("1236"))
            ])
        }
    }
}