mod storage;
mod sender;
mod receiver;
mod miner;
mod peer;

use std::collections::{HashMap, HashSet};
use std::env;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::str::FromStr;
use std::sync::Arc;

use crate::receiver::Receiver;
use crate::sender::Sender;
use crate::storage::Storage;


async fn main() {
    let args: Vec<String> = env::args().collect();
    let address = String::from("127.0.0.1:") + args.last().unwrap().as_str();
    let address = SocketAddr::from_str(address.as_str()).unwrap();
    
    
}
