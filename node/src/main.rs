#![feature(async_closure)]
#![feature(slice_pattern)]
#![feature(let_chains)]
extern crate core;

mod storage;
mod sender;
mod receiver;
mod miner;
mod node;
mod connector;

use tracing_subscriber;
use std::time::Duration;
use crate::node::Node;


fn main() {

    tracing_subscriber::fmt::init();

    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on( async {
        tokio::spawn(async { Node::start("1234").await });
        tokio::spawn(async { Node::start("1235").await });
        tokio::spawn(async { Node::start("1236").await });
        loop {
            tokio::time::sleep(Duration::MAX).await;
        }
    });
}


#[cfg(test)]
mod tests {
    use std::thread;
    use std::time::Duration;
    use rand::{Rng, thread_rng};
    use ursa::signatures::ed25519::Ed25519Sha512;
    use ursa::signatures::SignatureScheme;
    use client::Client;
    use network::serialize_data;
    use state::{Command, Transaction};


    /// Assuming that nodes has started during performing this test
    #[tokio::test]
    async fn start_blockchain_and_send_transactions() {

        let mut client1 = Client::new();
        let mut client2 = Client::new();
        let mut client3 = Client::new();

        let mut transactions1 = Vec::with_capacity(10);
        for _ in 0..transactions1.capacity() {
            transactions1.push(generate_transaction());
        };

        let mut transactions2 = Vec::with_capacity(10);
        for _ in 0..transactions2.capacity() {
            transactions2.push(generate_transaction());
        };

        let mut transactions3 = Vec::with_capacity(10);
        for _ in 0..transactions3.capacity() {
            transactions3.push(generate_transaction());
        };

        let mut count = 0;
        tokio::spawn(async move {
            for i in 0..transactions1.len() {
                let transaction = transactions1.get(i).unwrap();
                client1.send_transaction_to_network(
                    serialize_data(transaction))
                    .await;
                tokio::time::sleep(Duration::from_secs(2)).await;
                count += 1;
            }
        }).await.expect("TODO: panic message 1");;

        let mut count = 0;
        tokio::spawn(async move {
            for i in 0..transactions2.len() {
                let transaction = transactions2.get(i).unwrap();
                client2.send_transaction_to_network(
                    serialize_data(transaction))
                    .await;
                tokio::time::sleep(Duration::from_secs(2)).await;
                count += 1;
            }
        }).await.expect("TODO: panic message 2");;

        let mut count = 0;
        tokio::spawn(async move {
            for i in 0..transactions3.len() {
                let transaction = transactions3.get(i).unwrap();
                client3.send_transaction_to_network(
                    serialize_data(transaction))
                    .await;
                tokio::time::sleep(Duration::from_secs(2)).await;
                count += 1;
            }
        }).await.expect("TODO: panic message 3");
    }

    fn generate_transaction() -> Transaction {
        let mut rng = thread_rng();
        let n1: u8 = rng.gen_range(0..2); // command variant
        let mut n2: u8 = rng.gen_range(2..4); // number of commands in transaction
        let mut commands = vec![];

        while n2 > 0 {
            let command: Command;
            match n1 {
                0 => {
                    let (public_key, _) = Ed25519Sha512::new().keypair(None).unwrap();
                    command = Command::CreateAccount { public_key: public_key.to_string() }
                }
                1 => {
                    command = Command::AddFunds {
                        account_id: rng.gen_range(0..100),
                        value: rng.gen_range(0..1000),
                        asset_id: "TEST".to_string()
                    }
                }
                2 => {
                    command = Command::TransferFunds {
                        account_from_id: rng.gen_range(0..100),
                        account_to_id: rng.gen_range(0..100),
                        value: rng.gen_range(0..1000),
                        asset_id: "TEST2".to_string()
                    }
                }
                _ => { unreachable!() }
            };
            println!("command: {}", &command);
            commands.push(command);
            n2 -= 1;
        }

        Transaction {
            fee: 111,
            commands,
        }
    }
}


