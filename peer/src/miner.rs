use blake2::{Blake2s, Blake2s256, Digest};
use chrono::{Timelike, Utc};
use ursa::keys::PublicKey;
use ursa::keys::PrivateKey;
use ursa::signatures::ed25519::Ed25519Sha512;
use ursa::signatures::SignatureScheme;
use crypto::Hash;
use state::{Block, Transaction};

pub struct Miner {
    pub public_key: PublicKey,
    pub private_key: PrivateKey,
}

impl Miner {

    pub fn new() -> Self {
        let (public_key, private_key) = Ed25519Sha512::new().keypair(None).unwrap();
        Self {
            public_key,
            private_key
        }
    }

    pub fn mine_block(&self, target_hash: Hash, previous_block: &Block, transactions: Vec<Transaction>) -> Block {
        let id = previous_block.id + 1;
        let timestamp = Utc::now().timestamp();
        let signature = Ed25519Sha512::new()
            .sign(format!("{:?}", &transactions).as_bytes(), &self.private_key)
            .unwrap();
        let mut nonce = 0;
        let mut hash: Hash = previous_block.hash.clone(); //Vec::default();
        let mut block = Default::default();
        let start = Utc::now();
        while !is_current_hash_less_then_target(&hash, &target_hash) {
       //for _ in 0..100 {
            let mut hasher = crypto::hasher();
            block = Block {
                id,
                timestamp,
                nonce,
                signature: signature.clone(),
                hash: hash.clone(),
                previous_block_hash: Some(previous_block.hash.clone()),
                transactions: transactions.clone()
            };
            hasher.update(format!("{:?}", block).as_bytes());
            let h = hasher.finalize();
            //println!("hash of block: {}", hash_to_string(&hash));
            if hash_to_string(&hash).starts_with("00") {
                let finish = Utc::now();
                println!("success!!!, total time = {} sec", finish.second() - start.second());
                println!("hash: {}, target_hash: {}", hash_to_string(&hash), hash_to_string(&target_hash));
                break;
            }
            hash.clear();
            hash.extend_from_slice(&h);
            nonce += 1;
        };
        block
    }
}

fn is_current_hash_less_then_target(hash: &Hash, target_hash: &Hash) -> bool {
    let current = hash.iter().take_while(|n| **n == 0u8).count();
    let target =  target_hash.iter().take_while(|n| **n == 0u8).count();
    if target == 0 {
        return false
    }
    current <= target
}

fn hash_to_string(hash: &Hash) -> String {
    hash.iter().map(|n| n.to_string()).fold(String::new(), |acc, s| acc + s.as_str())
}

fn string_to_hash(string: &str) -> Hash {
    string.as_bytes().to_vec()
}

#[cfg(test)]
mod tests {
    use rand::prelude::*;
    use chrono::Utc;
    use crypto::hash;
    use state::{Block, Command, Transaction};
    use ursa::signatures::ed25519::Ed25519Sha512;
    use ursa::signatures::SignatureScheme;
    use crate::miner::{hash_to_string, Miner, string_to_hash};

    #[test]
    fn test_mine_block() {
        let miner = Miner::new();
        println!("previous block transactions:");
        let previous_block_transactions = generate_transactions();
        let previous_block = generate_block(2, previous_block_transactions);
        let target_hash = string_to_hash("00056AB355543AF344"); // TODO proper way to calculate hash
        println!("current block transactions:");
        let current_block_transactions = generate_transactions();
        miner.mine_block(target_hash, &previous_block, current_block_transactions);
    }

    fn generate_block(nonce: u32, transactions: Vec<Transaction>) -> Block {
        let (_, private_key) = Ed25519Sha512::new().keypair(None).unwrap();

        let signature = Ed25519Sha512::new()
            .sign(format!("{:?}", &transactions).as_bytes(), &private_key)
            .unwrap();

        let mut block = Block {
            id: 7,
            timestamp: Utc::now().timestamp(),
            nonce,
            signature,
            hash: vec![],
            previous_block_hash: Some(String::from("0004f4544324323323").as_bytes().to_vec()),
            transactions,
        };
        let hash = hash(&block.to_string().as_bytes());
        println!("block hash : {}", hash_to_string(&hash));
        block.hash = hash;
        block
    }

    fn generate_transactions() -> Vec<Transaction> {
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
                    asset_id: "TEST".to_string() }
                }
                2 => {
                    command = Command::TransferFunds {
                    account_from_id: rng.gen_range(0..100),
                    account_to_id: rng.gen_range(0..100),
                    value: rng.gen_range(0..1000),
                    asset_id: "TEST2".to_string() }
                }
                _ => { unreachable!() }
            };
            println!("command: {}", &command);
            commands.push(command);
            n2 -= 1;
        }
        let mut transactions = vec![];
        let transaction = Transaction {
            commands,
        };
        transactions.push(transaction);
        transactions
    }

}

