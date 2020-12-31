use crate::clients::admin_client::AdminClient;
#[macro_use] extern crate lazy_static;

mod protocol;
mod clients;
mod utils;
use protocol::kafka_error_codes::KAFKA_ERRORS;

fn main() {
    let host = "localhost:9092";
    let mut client = AdminClient::new(host.to_string());
    client.commit_offset(vec!["test"]);
    println!("{:?}", KAFKA_ERRORS.get(&0));
}


