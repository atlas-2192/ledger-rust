use std::future::Future;
use std::io::{Error, ErrorKind};
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tracing::error;
use byteorder::{BigEndian, ByteOrder, ReadBytesExt};
use crate::{Data, DATA_LENGTH, deserialize_data, read_exact_async, serialize_data, write_all_async};

pub enum RequestType {

    Blockchain { height: u64 },

    Block { hash: Vec<u8> },

    Transaction { hash: Vec<u8> },

}

/// 1-st byte - request type, 2-nd byte = length of second value, 3-rd - second value,
/// 4-th byte - length of 3-rd value, 5-th byte 3-rd value ...
pub async fn client_request(mut socket: &mut TcpStream, request_type: RequestType)
                            -> Result<Data, Error>
{
    match request_type {
        RequestType::Blockchain { height } => {
            let cmd_buf = [1u8];
            write_all_async(socket, &cmd_buf).await?;
            let height_buf = height.to_be_bytes();
            write_all_async(socket, &height_buf).await?;
            let data_buf = read_node_response(socket).await?;
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

async fn write_u32_to_buf(socket: &mut TcpStream, data: u32) -> Result<(), Error>
{
    let mut buf  = [0u8; 4];
    <BigEndian as ByteOrder>::write_u32(&mut buf.as_mut(), data);
    write_all_async(socket, buf.as_slice()).await?;
    Ok(())
}

async fn write_string_to_buf(socket: &mut TcpStream, data: &str) -> Result<(), Error>
{
    let data_buf = data.as_bytes();
    let data_buf_len = (data_buf.len() as u32).to_be_bytes();
    write_all_async(socket, &data_buf_len).await?;
    write_all_async(socket, data_buf).await?;
    Ok(())
}

// TODO prevent copying bytes to vec
async fn read_node_response(socket: &mut TcpStream)  -> Result<Vec<u8>, Error>  // , mut buf: &mut [u8]
{
    let mut len_buf = DATA_LENGTH;
    read_exact_async(socket, &mut len_buf).await?;
    let len = u32::from_be_bytes(len_buf);
    let mut data_buf = vec![0; len as _];
    read_exact_async(socket, &mut data_buf).await?;
    Ok(data_buf)
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
            let mut height_buf = [0u8; 8];
            read_exact_async(socket, &mut height_buf).await?;
            let height = height_buf.as_slice().read_u64::<BigEndian>();
            return if let Ok(height) = height {
                let request_type = RequestType::Blockchain { height };
                let response_buf = fn_blockchain_data(miner, Some(request_type)).await;
                let response_buf_len = (response_buf.len() as u32).to_be_bytes();
                write_all_async(socket, &response_buf_len).await?;
                write_all_async(socket, response_buf.as_slice()).await?;
                Ok(())
            } else {
                Err(Error::from(ErrorKind::InvalidInput))
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

//     MintTokens {
//         account_from_id: u32,
//         account_to_id: u32,
//         value: u32,
//         asset_id: String
//     },

//     RequestType::MintTokens { account_from_id, account_to_id, value, asset_id } => {
//         let cmd_buf = [4u8];
//         write_all_async(socket, &cmd_buf).await?;
//         write_u32_to_buf(&mut socket, account_from_id).await?;
//         write_u32_to_buf(&mut socket, account_to_id).await?;
//         write_u32_to_buf(&mut socket, value).await?;
//         write_string_to_buf(&mut socket,asset_id.as_str()).await?;
//         let response = read_node_response(&mut socket).await?;
//         let data = deserialize_data(&response).unwrap();
//         Ok(data)
//     }
