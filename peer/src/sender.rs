use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::sync::Mutex;
//use std::sync::Mutex;
use network::{Data, SendEvent, serialize_data};
use crate::storage::Storage;

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

    pub async fn run(&mut self) {
        // for (_, socket_addr) in &self.peers {
        //     let storage = self.storage.clone();
        //     let socket = TcpStream::connect(socket_addr).await;
        //     if let Ok(mut socket) = socket {
        //         tokio::spawn(async move {
        //             Self::transmit_blocks(&mut socket, storage).await;
        //         });
        //     } else {
        //         println!("could not establish connection: {}", socket_addr)
        //     }
        // }
    }

    async fn transmit_blocks(
        socket: &mut TcpStream,
        storage: Arc<Mutex<Storage>>,
    ) {
        // let storage = storage.lock().unwrap();
        // let last_block = storage.get_blockchain_reference().last().unwrap();
        // network::send_data(
        //     socket,
        //     serialize_data(&last_block).as_slice(),
        //     SendEvent::SendBlock)
        //     .await
        //     .unwrap()

        // for block in blocks.iter() {
        //     network::send_data(
        //         socket,
        //         serialize_data(block).as_slice(),
        //         SendEvent::SendBlock)
        //         .await
        //         .unwrap();
        // };
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