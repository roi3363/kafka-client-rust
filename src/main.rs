#[macro_use] extern crate lazy_static;
// extern crate proc_macro;

mod protocol;
mod clients;
mod utils;
use protocol::kafka_error_codes::KAFKA_ERRORS;
use crate::clients::consumer_client::ConsumerClient;
use std::time::Instant;

fn main() {
    let hosts = vec!["localhost:9092","localhost:9093","localhost:9094"];
    let mut consumer = ConsumerClient::new(hosts);
    let response = consumer.join_group();
    println!("{:#?}", response);
}


