#![feature(let_chains)]
#![feature(io_error_more)]

pub mod p2p;
pub mod client2node;

use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::io::ErrorKind;
use bincode::{DefaultOptions, Options};
use tokio::io;
use tokio::net::TcpStream;
use tracing::error;
use serde::{Deserialize, Serialize};
use errors::LedgerError;
use errors::LedgerError::DeserializationError;
use state::{Block, Transaction};

const DATA_LENGTH: [u8; 4] = [0, 0, 0, 0];

pub(crate) async fn read_exact_async(s: &TcpStream, buf: &mut [u8]) -> io::Result<()> {
    let mut red = 0;
    while red < buf.len() {
        s.readable().await?;
        match s.try_read(&mut buf[red..]) {
            Ok(0) => break,
            Ok(n) => {
                red += n;
            }
            Err(ref e) if e.kind() == ErrorKind::WouldBlock => { continue; }
            Err(e) => return Err(e),
        }
    }
    Ok(())
}

pub(crate) async fn write_all_async(stream: &TcpStream, buf: &[u8]) -> io::Result<()> {
    let mut written = 0;
    while written < buf.len() {
        stream.writable().await?;
        match stream.try_write(&buf[written..]) {
            Ok(0) => break,
            Ok(n) => {
                written += n;
            }
            Err(ref e) if e.kind() == ErrorKind::WouldBlock => { continue; }
            Err(e) => return Err(e),
        }
    }
    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[repr(u8)]
pub enum Data {
    Block(Block) = 1,
    Transaction(Transaction) = 2,
    Peer(String) = 3,
    Peers(HashMap<String, String>) = 4,
    Blockchain(Vec<Block>) = 5,
}

impl Display for Data {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Data::Block(ref b) => {
                write!(f, "data (block) : {}", b)
            }
            Data::Transaction(ref t) => {
                write!(f, "data (transaction) : {}", t)
            }
            Data::Peer(ref p) => {
                write!(f, "data (node) : {}", p)
            }
            Data::Peers(ref p) => {
                write!(f, "data (peers) : {}",
                       p.iter()
                           .map(|p| p.0.clone() + " " + p.1.as_str())
                           .reduce(|acc, s| acc + ", " + s.as_str())
                           .unwrap())
            }
            Data::Blockchain(ref b) => {
                write!(f, "data (blockchain) : {}",
                       b.iter()
                           .map(Block::to_string)
                           .reduce(|acc, s| acc + ", " + s.as_str())
                           .unwrap())
            }
        }
    }
}

impl Data {

    pub fn data_type(&self) -> u8 {
        match self {
            Data::Block(_) => 1,
            Data::Transaction(_) => 2,
            Data::Peer(_) => 3,
            Data::Peers(_) => 4,
            Data::Blockchain(_) => 5
        }
    }
}

pub fn serialize_data<DATA: serde::ser::Serialize>(data: DATA) -> Vec<u8> {
    DefaultOptions::new()
        .with_varint_encoding()
        .serialize(&data).unwrap()
}

pub fn deserialize_data<'a, DATA: serde::de::Deserialize<'a>>(bytes:  &'a [u8])
    -> Result<DATA, LedgerError>
{
    let data = DefaultOptions::new()
        .with_varint_encoding()
        .deserialize::<DATA>(&bytes[..]);
    if let Ok(data) = data {
        Ok(data)
    } else {
        let err = data.err().unwrap();
        error!("network::deserialize_data() error: {}",  err);
        Err(DeserializationError)
    }
}

#[cfg(test)]
mod tests {

    use tokio::net::{TcpListener, TcpStream};
    use state::{Block, Command, Transaction};
    use crate::p2p::send_data;
    use crate::p2p::SendEvent::SendBlock;
    use crate::serialize_data;

    #[tokio::test]
    async fn transfer_block() {
        let block = generate_block();
        let mut sender = TcpStream::connect("127.0.0.1:1234").await.unwrap();
        let _ = send_data(&mut sender, serialize_data::<&Block>(&block).as_slice(), SendBlock).await;
    }

    fn generate_block() -> Block {
        Block {
            id: 1,
            timestamp: 0,
            transactions: vec![Transaction {
                fee: 555,
                commands: vec![Command::CreateAccount {
                    public_key: "12345".to_string(),
                }],
            }],
            signature: vec![1, 2, 3, 4, 5],
            hash: vec![],
            previous_block_hash: None,
            nonce: 0
        }
    }
}
