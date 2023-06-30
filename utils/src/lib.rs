
pub fn bytes_vec_to_string(bytes: &Vec<u8>) -> String {
    bytes.iter().map(|n| n.to_string()).fold(String::new(), |acc, s| acc + s.as_str())
}

pub fn string_to_hash(string: &str) -> Vec<u8> {
    string.as_bytes().to_vec()
}
