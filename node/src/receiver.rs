use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
use tokio::sync::mpsc::{
    channel,
    Receiver as Rx,
    Sender as Tx
};
use async_trait::async_trait;
use errors::LedgerError;
use network::{receive_data, Data};
use crate::connector::{Connect, Connector};


#[derive(Debug)]
pub(crate) struct Receiver {
    address: SocketAddr,
    listener: TcpListener,
    pub(crate) connector_tx: Option<Tx<Data>>
}

impl Receiver {

    pub async fn new(address: SocketAddr) -> Self {
        Self {
            address,
            listener: TcpListener::bind(address).await.unwrap(),
            connector_tx: None
        }
    }

    pub async fn run(&mut self) {
        loop {
            match self.listener.accept().await {
                Ok((mut socket, addr)) => {
                    println!("accepted socket : {addr}");
                    let processed = Self::process_incoming(self, &mut socket).await;
                    if processed.is_err() {
                        println!("error processing incoming data")
                    }
                }
                Err(e) => {
                    println!("couldn't get client: {:?}", e);
                    loop {
                        if let Ok(new_listener) = TcpListener::bind(&self.address).await {
                            self.listener = new_listener;
                            break;
                        }
                    }
                }
            }
        }
    }

    async fn process_incoming(&mut self, socket: &mut TcpStream)
        -> Result<(), LedgerError> // tokio::sync::mpsc::Receiver<Data>
    {
        let data = receive_data(socket).await;
        if data.is_ok() {
            let tx = self.connector_tx.as_ref().unwrap();
            let sent = tx.send(data.unwrap()).await;
            if sent.is_err() {
                println!("channel has been closed {}", sent.err().unwrap());
                return Err(LedgerError::SyncError)
            }
        } else {
            println!("failed to receive data from network {}", data.err().unwrap());
            return Err(LedgerError::NetworkError)
        }
        Ok(())
    }
}

#[async_trait]
impl Connect for Receiver {
    async fn connect(&mut self, connector: Arc<Mutex<Connector>>) {
        let mut connector = connector.lock().await;
        let (tx, rx): (Tx<Data>, Rx<Data>) = channel(10);
        self.connector_tx = Some(tx);
        connector.receiver_rx = Arc::new(Mutex::new(Some(rx)));
    }
}