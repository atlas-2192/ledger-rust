use std::collections::HashMap;
use std::net::{ SocketAddr};
use tokio::net::{TcpStream};
use tracing::{debug, error};
use errors::LedgerError;
use network::{Data, p2p::process_incoming_data, p2p::send_data, p2p::SendEvent};
use network::client2node::RequestType;


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
            let a = tokio::spawn(async move {
                let stream = TcpStream::connect(addr.clone()).await;
                if let Ok(mut stream) = stream {
                    let res = send_data(
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
            })
                .await;
            error!("{}", a.is_err());
        }
    }

    pub async fn client_request(node_addr: SocketAddr, request_type: RequestType)
                                -> Result<Data, LedgerError>
    {
        let socket = TcpStream::connect(node_addr).await;
        return if let Ok(mut socket) = socket {
            match request_type {
                RequestType::Blockchain { .. } => {
                    return if let Ok(response) =
                        network::client2node::client_request(&mut socket, request_type).await {
                        Ok(response)
                    } else {
                        Err(LedgerError::ApiError)
                    }
                }
                RequestType::Block { .. } => { todo!() }
                RequestType::Transaction { .. } => { todo!() }
            }
        } else {
            error!("could not connect to node");
            Err(LedgerError::NetworkError)
        }
    }
}

fn get_initial_peers() -> HashMap<u32, SocketAddr> {
    HashMap::from([
                (1, utils::socket_addr("1234")),
                (2, utils::socket_addr("1235")),
                (3, utils::socket_addr("1236"))
            ])
}

#[cfg(test)]
mod tests {
    use network::{ serialize_data};
    use state::{Transaction};
    use crate::Client;

    #[tokio::test]
    async fn send_transaction_would_return_success() {
        let transaction = create_account_transaction();
        let mut client = Client::new();
        client.send_transaction_to_network(serialize_data(&transaction)).await;
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

