use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio::sync::mpsc::{
    self,
    Sender as ChannelSender,
    Receiver as ChannelReceiver
};
use network::{Command, serialize_block};
use crate::storage::Storage;

#[derive(Debug)]
pub struct Sender {
    peers: HashMap<u32, SocketAddr>,
    storage: Arc<Mutex<Storage>>,
}

impl Sender {

    pub fn new(peers: HashMap<u32, SocketAddr>, storage: Arc<Mutex<Storage>>) -> Self {
        Self {
            peers,
            storage
        }
    }

    pub async fn run(&mut self) {
        for (_, socket_addr) in &self.peers {
            let storage = self.storage.clone();
            let socket = TcpStream::connect(socket_addr).await;
            if let Ok(mut socket) = socket {
                tokio::spawn(async move {
                    Self::transmit_blocks(&mut socket, storage).await;
                });
            } else {
                println!("could not establish connection: {}", socket_addr)
            }
        }
    }

    async fn transmit_blocks(
        socket: &mut TcpStream,
        storage: Arc<Mutex<Storage>>,
    ) {
        let storage = storage.lock().await;
        let blocks = storage.get_uncommitted_blocks();
        for block in blocks.iter() {
            network::send_command_async(
                &socket,
                Some(serialize_block(block).as_slice()),
                Command::TransmitBlock)
                .await
                .unwrap();
        };
    }
}