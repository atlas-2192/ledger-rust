#![feature(async_closure)]
mod storage;
mod sender;
mod receiver;
mod miner;
mod node;
mod connector;

use crate::node::Node;


#[tokio::main]
async fn main() {
    Node::start("7777").await;
}

