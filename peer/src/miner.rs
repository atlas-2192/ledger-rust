use blake2::{Blake2s, Blake2s256, Digest};
use chrono::Utc;
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
        let mut hash: Hash = Vec::default();
        let mut block = Default::default();

        while count_zeros(&hash) > count_zeros(&target_hash) {
            let mut hasher = crypto::hasher();
            block = Block {
                id,
                timestamp,
                nonce,
                signature: signature.clone(),
                hash: hash.clone(),
                previous_block_hash: Some(previous_block.hash.clone()),
                data: transactions.clone()
            };
            hasher.update(format!("{:?}", block).as_bytes());
            hasher.finalize();
            println!("hash of block: {}", hash_to_string(&hash));
            nonce += 1;
        };
        block
    }
}

fn count_zeros(hash: &Hash) -> usize {
    hash.iter().take_while(|n| **n == 0u8).count()
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
    use crate::miner::{Miner, string_to_hash};

    #[test]
    fn test_mine_block() {
        let miner = Miner::new();
        let previous_block = generate_block(2);
        let previous_block_transactions = generate_transactions();
        let target_hash = string_to_hash("00056AB355543AF344");
    }

    fn generate_block(nonce: u32) -> Block {
        let (public_key, private_key) = Ed25519Sha512::new().keypair(None).unwrap();
        let mut data = vec![];
        let transaction = Transaction {
            commands: vec![
                Command::CreateAccount { public_key: public_key.to_string() }
            ]
        };
        data.push(transaction);
        let signature = Ed25519Sha512::new()
            .sign(format!("{:?}", &data).as_bytes(), &private_key)
            .unwrap();
        Block {
            id: 7,
            timestamp: Utc::now().timestamp(),
            nonce,
            signature,
            hash: vec![],
            previous_block_hash: Some(String::from("0004f4544324323323").as_bytes().to_vec()),
            data,
        }
    }

    fn generate_transactions() -> Vec<Transaction> {
        let
    }

}

