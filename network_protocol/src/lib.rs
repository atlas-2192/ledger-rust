#![feature(let_chains)]

use std::io::{self, Error, ErrorKind};
use bincode::{DefaultOptions, Options};
use tokio::net::{TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use std::convert::{TryFrom};
use errors::LedgerError;
use errors::LedgerError::CommandError;
use state::{Block, Transaction};

pub const NO_DATA: &str = "no data";
const CMD_LEN: [u8; 1] = 1u8.to_be_bytes();
const DATA_LEN: [u8; 4] = [0, 0, 0, 0];

pub enum Command {
    SendTransaction,
    TransmitBlock,
    GetPeers,
}

impl TryFrom<u8> for Command {
    type Error = LedgerError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1u8 => {
                println!("COMMAND = TransmitBlock");
                Ok(Command::TransmitBlock)
            },
            2u8 => {
                println!("COMMAND = GetPeers");
                Ok(Command::GetPeers)
            },
            _ => {
                println!("COMMAND ERROR");
                Err(CommandError)
            },
        }
    }
}

async fn read_exact_async(s: &TcpStream, buf: &mut [u8]) -> io::Result<()> {
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

async fn write_all_async(stream: &TcpStream, buf: &[u8]) -> io::Result<()> {
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

pub async fn send_command_async<DATA: AsRef<[u8]>>(
    socket: &TcpStream,
    data: DATA,
    command: Command
)
     -> Result< (), io::Error>
{
    let buf = data.as_ref();
    match command {
        Command::TransmitBlock => {
            let cmd_buf: [u8; 1] = [1u8];
            write_all_async(socket, &cmd_buf).await?;
            let buf_len = (buf.len() as u32).to_be_bytes();
            write_all_async(socket, &buf_len).await?;
            write_all_async(socket, buf).await?;
            let mut buf: [u8; 1] = [0u8];
            read_exact_async(socket, &mut buf).await?;
            if buf[0] == 1 {
                println!("success")
            } else {
                println!("failure")
            }
        },
        Command::GetPeers => {
            let cmd_buf: [u8; 1] = [2u8];
            write_all_async(socket, &CMD_LEN).await?;
            write_all_async(socket, &cmd_buf).await?;
        },
        Command::SendTransaction => {
            let cmd_buf: [u8; 1] = [3u8];
            write_all_async(socket, &cmd_buf).await?;
            let buf_len = (buf.len() as u32).to_be_bytes();
            write_all_async(socket, &buf_len).await?;
            write_all_async(socket, buf).await?;
            let mut buf: [u8; 1] = [0u8];
            read_exact_async(socket, &mut buf).await?;
            if buf[0] == 1 {
                println!("success")
            } else {
                println!("failure")
            }
        }
    }
    Ok(())
}

pub async fn receive_command_async(socket: &TcpStream) -> Result<(Command, Vec<u8>), io::Error>
{
    let mut cmd_buf: [u8; 1] = [0u8];
    read_exact_async(socket, &mut cmd_buf).await?;
    let command = Command::try_from(cmd_buf[0]);
    if command.is_err() {
        return Err(Error::from(ErrorKind::InvalidInput))
    }
    let command = command.unwrap();
    match command {
        Command::TransmitBlock => {
            let mut buf = DATA_LEN.clone();
            read_exact_async(socket, &mut buf).await?;
            let len = u32::from_be_bytes(buf);
            let mut block_buf = vec![0; len as _];
            read_exact_async(socket, &mut block_buf).await?;
            //let block = deserialize_block(block_buf);
            println!("block has been received...");
            let mut buf: [u8; 1] = [1u8];
            write_all_async(socket, &mut buf).await?;
            Ok((Command::TransmitBlock, block_buf.to_vec()))
        }
        Command::GetPeers => {
            todo!()
        },
        Command::SendTransaction => {
            todo!()
        }
    }

}

pub fn serialize_block(block: &Block) -> Vec<u8> {
    DefaultOptions::new()
        .with_varint_encoding()
        .serialize(block).unwrap()
}

pub fn serialize_transaction(transaction: &Transaction) -> Vec<u8> {
    DefaultOptions::new()
        .with_varint_encoding()
        .serialize(transaction).unwrap()
}

pub fn deserialize_block(bytes: Vec<u8>) -> Block {
    DefaultOptions::new()
        .with_varint_encoding()
        .deserialize::<Block>(&bytes[..]).unwrap()
}

fn println_bytes(buf: &[u8]) -> String{
    buf.iter()
        .map(|b| b.to_string())
        .reduce(|acc, b| acc.to_string() + " " + b.to_string().as_str())
        .unwrap()
}

#[cfg(test)]
mod tests {
    use std::thread;
    use std::time::Duration;
    use tokio::net::{TcpListener, TcpStream};
    use state::{Block, Command, Transaction};
    use crate::{Command as cmd, receive_command_async};
    use crate::{send_command_async, serialize_block};

    #[tokio::test]
    async fn transfer_block() {
        let block = generate_block();
        // tokio::spawn(async {
        //     let listener = TcpListener::bind("127.0.0.1:1234").await.unwrap();
        //     let receiver = receiver(listener).await;
        //     receive_command_async(&receiver).await;
        // });
        //thread::sleep(Duration::from_secs(2));
        let sender = TcpStream::connect("127.0.0.1:1234").await.unwrap();
        send_command_async(&sender, serialize_block(&block).as_slice(), cmd::TransmitBlock).await;
    }

    async fn receiver(listener: TcpListener) -> TcpStream {
        let listener = listener.accept().await.unwrap().0;
        println!("listener: {}", &listener.peer_addr().unwrap());
        listener
    }

    fn generate_block() -> Block {
        Block {
            transactions: vec![Transaction {
                commands: vec![Command::CreateAccount {
                    public_key: "12345".to_string(),
                }],
            }],
            signature: vec![1, 2, 3, 4, 5],
            previous_block_hash: None,
        }
    }
}

