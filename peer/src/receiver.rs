use std::borrow::BorrowMut;
use std::net::SocketAddr;
use std::sync::{Arc };
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::{
    self,
    Sender as ChannelSender,
    Receiver as ChannelReceiver
};
use tokio::sync::Mutex;
use errors::LedgerError;
use state::{Block, Transaction};
use network::{Command, deserialize_block};
use crate::storage::Storage;


#[derive(Debug)]
pub struct Receiver {
    listener: TcpListener,
    storage: Arc<Mutex<Storage>>
}

impl Receiver {

    pub async fn new(address: SocketAddr, storage: Arc<Mutex<Storage>>) -> Self {
        Self {
            listener: TcpListener::bind(address).await.unwrap(),
            storage
        }
    }

    pub async fn run(&mut self) {  //  -> Result<Block, BlockError>
        let (block_tx, mut block_rx) :
            (ChannelSender<Vec<u8>>, ChannelReceiver<Vec<u8>>) = mpsc::channel(20);
        let storage = self.storage.clone();
        tokio::spawn(async move {
            Self::store_block(&mut block_rx, storage).await;
        });
        loop {
            match self.listener.accept().await {
                Ok((mut socket, addr)) => {
                    let sender = block_tx.clone();
                    println!("accepted socket : {addr}");
                    let processed = Self::process_incoming(&mut socket, sender).await;
                    if processed.is_err() {
                        println!("error processing incoming data")
                    }
                }
                Err(e) => {
                    println!("couldn't get client: {:?}", e)
                }
            }
        }
    }

    async fn process_incoming(socket: &mut TcpStream, tx: ChannelSender<Vec<u8>>) -> Result<(), LedgerError> {
        let command_bytes = network::receive_command_async(socket).await;
        if command_bytes.is_err() {
            return Err(LedgerError::CommandError)
        }
        match command_bytes.unwrap() {
            (Command::TransmitBlock, bytes) => {
                let sended_bytes = tx.send(bytes).await;
                if sended_bytes.is_err() {
                    let e = sended_bytes.err().unwrap();
                    println!("error while sending block to channel {}", e);
                    return Err(LedgerError::SyncError)
                }
                Ok(())
            },
            (Command::GetPeers, bytes) => {
                todo!()
            }
            (Command::SendTransaction, bytes) => {
                todo!()
            }
        }
    }

    async fn store_block(rx: &mut ChannelReceiver<Vec<u8>>, storage: Arc<Mutex<Storage>>) {
        loop {
            let block_bytes = rx.recv().await;
            if block_bytes.is_none() {
                println!("all channels are closed");
                return;
            };
            let block = deserialize_block(block_bytes.unwrap());
            storage.lock().await.add_block(block);
        }
    }
}