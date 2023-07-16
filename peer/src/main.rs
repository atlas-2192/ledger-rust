#![feature(async_closure)]
mod storage;
mod sender;
mod receiver;
mod miner;
mod peer;
mod connector;

use crate::peer::Peer;


#[tokio::main]
async fn main() {
    Peer::start("7777").await;
}

