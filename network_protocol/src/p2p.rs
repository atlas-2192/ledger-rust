use std::io::{Error, ErrorKind};
use tokio::net::{TcpStream};
use std::convert::{TryFrom};
use derive_more::{Display};
use tracing::{error, trace};
use errors::LedgerError;
use errors::LedgerError::*;
use crate::{Data, DATA_LENGTH, deserialize_data, read_exact_async, write_all_async};

pub const NO_DATA: &str = "no data";

/// protocol : (byte1, byte2) = NetworkEvent(SendEvent/ReceiveEvent)
#[derive(Display)]
#[repr(u8)]
pub enum NetworkEvent {
    Send(SendEvent) = 1,
    Receive(ReceiveEvent) = 2,
}

#[derive(Display)]
#[repr(u8)]
pub enum SendEvent {
    SendBlock = 1,
    SendTransaction = 2,
    InitPeer = 3,
    SendPeers = 4,
    SendChain = 5,
    //SendProveBlock,
}

impl SendEvent {
    pub fn value(&self) -> u8 {
        match self {
            SendEvent::SendBlock => 1,
            SendEvent::SendTransaction => 2,
            SendEvent::InitPeer => 3,
            SendEvent::SendPeers => 4,
            SendEvent::SendChain => 5,
        }
    }
}

#[derive(Display)]
pub enum ReceiveEvent {
    ReceiveBlock = 1,
    ReceiveTransaction = 2,
    AddPeer = 3,
    ReceivePeers = 4,
    ReceiveChain = 5,
    //ReceiveProveBlock,
}

impl ReceiveEvent {
    pub fn from_value(value: u8) -> Result<Self, LedgerError> {
        match value {
            1 => Ok(ReceiveEvent::ReceiveBlock),
            2 => Ok(ReceiveEvent::ReceiveTransaction),
            3 => Ok(ReceiveEvent::AddPeer),
            4 => Ok(ReceiveEvent::ReceivePeers),
            5 => Ok(ReceiveEvent::ReceiveChain),
            _ => Err(WrongCommandError)
        }
    }
}

impl TryFrom<(u8, u8)> for NetworkEvent {
    type Error = LedgerError;

    fn try_from(value: (u8, u8)) -> Result<Self, Self::Error> {
        match value.0 {
            1u8 => {
                trace!("SendEvent");
                let send_event = match value.1 {
                    1 => SendEvent::SendBlock,
                    2 => SendEvent::SendTransaction,
                    3 => SendEvent::InitPeer,
                    4 => SendEvent::SendPeers,
                    5 => SendEvent::SendChain,
                    _ => {
                        println!("NetworkEvent ERROR");
                        return Err(WrongCommandError)
                    },
                };
                trace!("NetworkEvent = {}", &send_event);
                Ok(NetworkEvent::Send(send_event))
            },
            2u8 => {
                trace!("ReceiveEvent");
                let receive_event = match value.1 {
                    1 => ReceiveEvent::ReceiveBlock,
                    2 => ReceiveEvent::ReceiveTransaction,
                    3 => ReceiveEvent::AddPeer,
                    4 => ReceiveEvent::ReceivePeers,
                    5 => ReceiveEvent::ReceiveChain,
                    _ => {
                        println!("NetworkEvent ERROR");
                        return Err(WrongCommandError)
                    },
                };
                trace!("NetworkEvent = {}", &receive_event);
                Ok(NetworkEvent::Receive(receive_event))
            },
            _ => {
                trace!("NetworkEvent ERROR");
                Err(WrongCommandError)
            },
        }
    }
}

/// 1 byte - event, 2 byte - len of data, 3..len bytes - data
pub async fn send_data<DATA: AsRef<[u8]>>(
    socket: &mut TcpStream,
    data: DATA,
    event: SendEvent)
    -> Result< (), Error>
{
    let buf = data.as_ref();
    match event {
        _ => { send_command_and_data(event, buf, socket).await? }
    }
    Ok(())
}

async fn send_command_and_data(
    send_event: SendEvent,
    buf: &[u8],
    socket: &mut TcpStream)
    -> Result<(), Error>
{
    let cmd_buf: [u8; 1] = [send_event.value()];
    write_all_async(socket, &cmd_buf).await?;
    let buf_len = (buf.len() as u32).to_be_bytes();
    write_all_async(socket, &buf_len).await?;
    write_all_async(socket, buf).await?;
    let mut buf: [u8; 1] = [0u8];
    read_exact_async(socket, &mut buf).await?;
    return if buf[0] == 1 {
        Ok(())
    } else {
        error!("sending data failure");
        Err(Error::from(ErrorKind::NetworkDown))
    };
}

/// 1 byte - event, 2 byte - len of data, 3..len bytes - data
pub async fn process_incoming_data(socket: &TcpStream) -> Result<Data, LedgerError>
{
    let mut cmd_buf: [u8; 1] = [0u8];
    let read = read_exact_async(socket, &mut cmd_buf).await;
    return if let Ok(_) = read {
        let event = ReceiveEvent::from_value(cmd_buf[0]);
        if event.is_err() {
            return Err(WrongCommandError)
        }
        let event = event.unwrap();
        match event {
            _ => {
                let data = process_event(event, socket).await?;
                Ok(data)
            }
        }
    } else {
        Err(WrongCommandError)
    }
}

async fn process_event(
    event: ReceiveEvent,
    socket: &TcpStream)
    -> Result<Data, LedgerError>
{
    let mut len_buf = DATA_LENGTH;
    if let Err(_) = read_exact_async(socket, &mut len_buf).await {
        return Err(WrongCommandError);
    }
    let len = u32::from_be_bytes(len_buf);
    let mut data_buf = vec![0; len as _];
    if let Err(e) = read_exact_async(socket, &mut data_buf).await {
        error!("receive_event_and_data() data_buf error: {}", e);
        return Err(NetworkError);
    }
    let data;
    match event {
        ReceiveEvent::ReceiveBlock => {
            let block = deserialize_data(data_buf.as_slice());
            if block.is_ok() {
                data = Data::Block(block.unwrap());
            } else {
                return Err(DeserializationError);
            }
        }
        ReceiveEvent::ReceiveTransaction => {
            let transaction = deserialize_data(data_buf.as_slice());
            if transaction.is_ok() {
                data = Data::Transaction(transaction.unwrap());
            } else {
                return Err(DeserializationError);
            }
        }
        ReceiveEvent::AddPeer => {
            let peer = deserialize_data(data_buf.as_slice());
            if peer.is_ok() {
                data = Data::Peer(peer.unwrap());
            } else {
                return Err(DeserializationError);
            }
        }
        ReceiveEvent::ReceivePeers => {
            let peers = deserialize_data(data_buf.as_slice());
            if peers.is_ok() {
                data = Data::Peers(peers.unwrap());
            } else {
                return Err(DeserializationError);
            }
        }
        ReceiveEvent::ReceiveChain => {
            let blockchain = deserialize_data(data_buf.as_slice());
            if blockchain.is_ok() {
                data = Data::Blockchain(blockchain.unwrap());
            } else {
                let e = blockchain.err().unwrap();
                error!("deserialization error: {}", e);
                return Err(DeserializationError);
            }
        }
    }
    if write_response(socket).await == false {
        return Err(SyncError);
    };
    Ok(data)
}

async fn write_response(socket: &TcpStream) -> bool {
    let mut buf: [u8; 1] = [1u8];
    let res = write_all_async(socket, &mut buf).await;
    if res.is_ok() {
        true
    } else {
        trace!("write_response failed");
        false
    }
}

