use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use tokio::net::{TcpStream};
use tracing::{debug, error};
use errors::LedgerError;
use network::{Data, process_incoming_data, send_request, SendEvent};
use state::Block;


pub struct Client {
    peers: HashMap<u32, SocketAddr>,
}

impl Client {

    pub fn new() -> Self {
        Self { peers: get_initial_peers() }
    }

    pub fn add_peer() {
        todo!()
    }

    pub async fn send_transaction_to_network(&mut self, transaction: Vec<u8>) {
        let mut addresses = Vec::new();
        self.peers.values().for_each(|addr| addresses.push(addr.clone()));
        for (_, addr) in addresses.iter().enumerate() {
            let addr = addr.clone();
            let transaction_clone = transaction.clone();
            // if !addr.eq(&SocketAddr::from_str("127.0.0.1:1234").unwrap()) {  // for test
            //     continue
            // }
            let a = tokio::spawn(async move {
                let stream = TcpStream::connect(addr.clone()).await;
                if let Ok(mut stream) = stream {
                    let res = send_request(
                        &mut stream,
                        transaction_clone.as_slice(),
                        SendEvent::SendTransaction)
                        .await;
                    if res.is_err() {
                        error!("error while sending command to peers : {}", res.err().unwrap());
                    }
                } else {
                    error!("could not connect to node");
                }
            }) //;
                .await;
            error!("{}", a.is_err());
        }
    }

    pub async fn get_node_blockchain(addr: SocketAddr) -> Result<Vec<Block>, LedgerError> {
        let stream = TcpStream::connect(addr).await;
        return if let Ok(mut stream) = stream {
            let buf: [u8; 0] = [0; 0]; // no need to send data apart from command
            let request = send_request(
                &mut stream,
                buf.as_slice(),
                SendEvent::SendChain)
                .await;
            let response;
            if let Ok(_) = request {
                response = process_incoming_data(&mut stream).await;
                if response.is_err() {
                    error!("error receiving blockchain : {}", response.err().unwrap());
                    Err(LedgerError::ApiError)
                } else {
                    match response.unwrap() {
                        Data::Blockchain(blockchain) => Ok(blockchain),
                        _ => unreachable!()
                    }
                }
            } else {
                let e = request.err().unwrap();
                error!("client request error: {}", e);
                Err(LedgerError::NetworkError)
            }
        } else {
            error!("could not connect to node");
            Err(LedgerError::NetworkError)
        };
    }
}

fn get_initial_peers() -> HashMap<u32, SocketAddr> {
    HashMap::from([
                (1, SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 1234)),
                (2, SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 1235)),
                (3, SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 1236))
            ])
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;
    use std::str::FromStr;
    use tracing::{error, info};
    use network::{ serialize_data};
    use state::{Transaction};
    use utils::LOCAL_HOST;
    use crate::Client;

    #[tokio::test]
    async fn send_transaction_would_return_success() {
        let transaction = create_account_transaction();
        let mut client = Client::new();
        client.send_transaction_to_network(serialize_data(&transaction)).await;
    }

    #[tokio::test]
    async fn get_blockchain_state() {
        let addr = SocketAddr::from_str((String::from(LOCAL_HOST) + "1244").as_str()).unwrap();
        if let Ok(blockchain) = Client::get_node_blockchain(addr).await {
            blockchain.iter().for_each(|b| info!("block : {}", b));
        } else {
            error!("API error")
        }
    }


    fn create_account_transaction() -> Transaction {
        Transaction {
            fee: 333,
            commands: vec![state::Command::CreateAccount {
                public_key: "12345".to_string(),
            }],
        }
    }
}

