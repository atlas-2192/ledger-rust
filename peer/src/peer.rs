use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use crate::peer::receiver::Receiver;
use crate::peer::sender::Sender;

#[derive(Debug)]
pub struct Peer {
    id: u32,
    peers: HashMap<u32, SocketAddr>,
    receiver: Receiver,
    sender: Sender,
    storage: storage::Storage,
}

const INITIAL_PEERS: HashMap<u32, SocketAddr> = HashMap::from([
    (1, SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 1234)),
    (2, SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 1235)),
    (3, SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 1236))
]);

impl Peer {

    pub fn new(id: u32, rx: Receiver, tx: Sender) -> Self {
        Self {
            id,
            peers: Default::default(),
            receiver: rx,
            sender: tx,
            storage: Default::default(),
        }
    }

}

mod receiver {
    
    use std::net::SocketAddr;
    use tokio::net::{TcpListener, TcpStream};
    use errors::LedgerError;
    use state::Block;
    use errors::LedgerError::BlockError;

    #[derive(Debug)]
    pub struct Receiver {
        listener: TcpListener
    }

    impl Receiver {

        pub async fn new(address: SocketAddr) -> Self {
            Self {
                listener: TcpListener::bind(address).await.unwrap()
            }
        }

        pub async fn run(&self) {  //  -> Result<Block, BlockError>
            loop {
                match self.listener.accept().await {
                    Ok((mut socket, addr)) => {
                        println!("accepted socket : {addr}");
                        let processed = Self::process(&mut socket).await;
                        if processed.is_err() {
                            println!("error processing incoming data")
                        }
                    }
                    Err(e) => {
                        println!("couldn't get client: {:?}", e)
                    }
                }
            }
        }

        async fn process(socket: &mut TcpStream) -> Result<(), LedgerError> {
            let block = network::receive_command_async(socket).await;
            if block.is_err() {
                return Err(LedgerError::CommandError)
            }
            Ok(())
        }
    }
}

mod sender {
    #[derive(Debug)]
    pub struct Sender {

    }
}

mod storage {

    use std::collections::HashMap;
    use state::{Accounts, Assets, Block};

    use crate::crypto;

    #[derive(Debug, Clone, Default)]
    pub struct Storage {
        pub blockchain: Vec<Block>,  // TODO persistency
        pub accounts: Accounts,
        /// Key is a tuple of format (account_id, asset_id)
        pub assets: Assets,
    }

    impl Storage {
        pub fn new() -> Self {
            Self {
                blockchain: Vec::new(),
                accounts: HashMap::new(),
                assets: HashMap::new(),
            }
        }

        pub fn add_block(&mut self, mut block: Block) {
            for commands in block.data.iter().map(|transaction| &transaction.command) {
                for command in commands {
                    command.execute(&mut self.accounts, &mut self.assets);
                }
            }
            if let Some(last_block) = self.blockchain.last() {
                block.previous_block_hash = Some(crypto::hash(last_block));
            }
            self.blockchain.push(block);
        }
    }
}
