use std::future::Future;
use std::io::{Error, ErrorKind};
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tracing::error;
use byteorder::{BigEndian, ReadBytesExt};
use errors::LedgerError::{DeserializationError, WrongCommandError};
use crate::{Data, DATA_LENGTH, deserialize_data, read_exact_async, write_all_async};

#[repr(u8)]
pub enum RequestType {

    NodeBlockchain { height: u64, } = 1, //

    Block { hash: Vec<u8>, } = 2, //

    Transaction{ hash: Vec<u8>, } = 3, //
}

/// 1-st byte - request type, 2-nd byte = length of second value, 3-rd - second value,
/// 4-th byte - length of 3-rd value, 5-th byte 3-rd value ...
pub async fn client_request(socket: &TcpStream, request_type: RequestType)
                            -> Result<Data, Error>
{
    match request_type {
        RequestType::NodeBlockchain { height } => {
            let cmd_buf = [1u8];
            write_all_async(socket, &cmd_buf).await?;

            let height_buf = height.to_be_bytes();
            write_all_async(socket, &height_buf).await?;

            let mut len_buf = DATA_LENGTH;
            read_exact_async(socket, &mut len_buf).await?;
            let len = u32::from_be_bytes(len_buf);
            let mut data_buf = vec![0; len as _];
            read_exact_async(socket, &mut data_buf).await?;

            let response = deserialize_data(data_buf.as_slice());
            return if let Ok(blockchain) = response {
                Ok(blockchain)
            } else {
                let err = response.err().unwrap();
                error!("error response api: {}", &err);
                Err(Error::from(ErrorKind::InvalidInput))
            }
        }
        RequestType::Block { hash } => {
            todo!()
        }
        RequestType::Transaction { hash } => {
            todo!()
        }
    }
}

pub async fn node_response<Miner, Func, Fut>(socket: &mut TcpStream,
                                             miner: Arc<Mutex<Miner>>,
                                             fn_blockchain_data: Func)
                                             -> Result<(), Error>
    where Func: Fn(Arc<Mutex<Miner>>, Option<RequestType>) -> Fut,
          Fut: Future<Output = Vec<u8>>
{
    let mut cmd_buf = [0u8];
    read_exact_async(socket, &mut cmd_buf).await?;
    match cmd_buf[0] {
        1u8 => {
            let mut height_buf = [0u8; 8]; //vec![0; len as _];
            read_exact_async(socket, &mut height_buf).await?;
            let height = height_buf.as_slice().read_u64::<BigEndian>();
            if let Ok(height) = height {
                let request_type = RequestType::NodeBlockchain { height };
                let response_buf = fn_blockchain_data(miner, Some(request_type)).await;
                let response_buf_len = (response_buf.len() as u32).to_be_bytes();
                write_all_async(socket, &response_buf_len).await?;
                write_all_async(socket, response_buf.as_slice()).await?;
                return Ok(())
            } else {
                let err = height.err().unwrap();
                error!("read_u64::<BigEndian>() error : {}", err);
                return Err(Error::from(ErrorKind::InvalidInput))
            }
        }
        2u8 => {
            todo!()
            //Ok(())
        }
        3u8 => {
            todo!()
            //Ok(())
        }
        _ => {
            error!("Api request error");
            Err(Error::from(ErrorKind::InvalidInput))
        }
    }
}
