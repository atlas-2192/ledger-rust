use blake2::{ Blake2s256, Digest};
use sha2::Sha256;

pub type Hash = Vec<u8>;
pub const TARGET_HASH_PREFIX: &str = "00"; // TODO changing it depending on network size

// pub fn hash(hash_data: &[u8]) -> Hash {
//     let mut hasher = Blake2s256::new();
//     hasher.update(hash_data); //format!("{:?}", hash_data).as_bytes()
//     let res = hasher.finalize();
//     let mut vector = Vec::new();
//     vector.extend_from_slice(&res);
//     vector
// }

pub fn hash(hash_data: &[u8]) -> Hash {
    let mut hasher = Sha256::new();
    hasher.update(hash_data);
    hasher.finalize().as_slice().to_owned()
}

// pub fn hasher() -> Blake2s<U32> {
//     Blake2s256::new()
// }

pub fn hasher() -> Sha256 {
    Sha256::new()
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

