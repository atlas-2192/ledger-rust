use chrono::prelude::*;

pub const LOCAL_HOST: &str = "127.0.0.1:";

pub fn print_bytes(bytes: &Vec<u8>) -> String {
    bytes.iter().map(|n| n.to_string()).fold(String::new(), |acc, s| acc + s.as_str())
}

pub fn string_to_hash(string: &str) -> Vec<u8> {
    string.as_bytes().to_vec()
}

pub fn convert_timestamp_to_day_time(timestamp: i64) -> DateTime<Utc> {
    let naive = NaiveDateTime::from_timestamp(timestamp, 0);
    let datetime: DateTime<Utc> = DateTime::from_utc(naive, Utc);
    //let date = datetime.format("%Y-%m-%d %H:%M:%S");
    datetime
}
