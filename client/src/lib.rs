use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use tokio::net::{TcpStream};
use network::{Command, send_command_async};
use state::Transaction;

struct Client {
    peers: HashMap<u32, SocketAddr>,
}

impl Client {

    pub fn new() -> Self {
        Self { peers: get_initial_peers() }
    }

    pub fn add_peer() {
        todo!()
    }

    pub async fn send_transaction_to_network(&self, transaction: Vec<u8>) {
        let mut addresses = Vec::new();
        self.peers.values().for_each(|addr| addresses.push(addr.clone()));
        for (_, addr) in addresses.iter().enumerate() {
            let addr = addr.clone();
            let transaction_clone = transaction.clone();
            tokio::spawn(async move {
                let stream = TcpStream::connect(addr.clone()).await;
                if let Ok(stream) = stream {
                    let res = send_command_async(
                        &stream,
                        transaction_clone.as_slice(),
                        Command::SendTransaction)
                        .await;
                    if res.is_err() {
                        println!("error while sending command to peers : {}", res.err().unwrap());
                    }
                } else {
                    println!("could not connect to peer");
                }
            });
        }
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

    use tokio::net::{TcpListener, TcpStream};
    use network::{Command, serialize_block, serialize_transaction};
    use state::{Block, Transaction};
    use crate::Client;

    #[tokio::test]
    async fn send_transaction_would_return_success() {
        let transaction = create_account_transaction();
        let client = Client::new();
        client.send_transaction_to_network(serialize_transaction(&transaction)).await;
    }

    fn create_account_transaction() -> Transaction {
        Transaction {
            commands: vec![state::Command::CreateAccount {
                public_key: "12345".to_string(),
            }],
        }
    }
}

