use blake2::{Blake2s256, Digest};
use crate::Block;

pub type Hash = Vec<u8>;

pub fn hash(block: &Block) -> Hash {
    let mut hasher = Blake2s256::new();
    hasher.update(format!("{:?}", block).as_bytes());
    let res = hasher.finalize();
    let mut vector = Vec::new();
    vector.extend_from_slice(&res);
    vector
}