use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use tokio::net::TcpStream;
//use std::sync::Mutex;
use network::{Data, SendEvent, serialize_data};
use state::Block;

#[derive(Debug)]
pub(crate) struct Sender {
    peers: HashMap<u32, SocketAddr>,
    pub(crate) connector_rx: Option<tokio::sync::mpsc::Receiver<Data>>,
}

impl Sender {

    pub fn new() -> Self {
        Self {
            peers: Peers::new().addresses,
            connector_rx: None
        }
    }

    async fn send_block_to_network(&self, block: Block) {
        for (_, socket_addr) in &self.peers {
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