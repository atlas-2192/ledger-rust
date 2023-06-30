use blake2::{Blake2s, Blake2s256, Digest};
use blake2::digest::typenum::U32;
use ursa::signatures::ed25519::Ed25519Sha512;

pub type Hash = Vec<u8>;
pub const TARGET_HASH_PREFIX: &str = "00"; // TODO changing it depending on network size

pub fn hash(block: &[u8]) -> Hash {
    let mut hasher = Blake2s256::new();
    hasher.update(format!("{:?}", block).as_bytes());
    let res = hasher.finalize();
    let mut vector = Vec::new();
    vector.extend_from_slice(&res);
    vector
}

pub fn hasher() -> Blake2s<U32> {
    Blake2s256::new()
}

pub fn validate_hash(hash: Hash) -> bool {

    true
}

#[cfg(test)]
mod tests {

    use crate::hash;

    #[test]
    fn test_hash_function() {
        dbg!(hash(&generate_block()));
    }

    fn generate_block() -> Vec<u8> {
        String::from("ABRACADABRA!!!").as_bytes().to_vec()
    }

}

