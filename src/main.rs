#[macro_use]
extern crate lazy_static;
// extern crate proc_macro;


use std::time::Instant;

use protocol::kafka_error_codes::KAFKA_ERRORS;

use crate::clients::consumer_client::ConsumerClient;

mod protocol;
mod clients;
mod utils;

fn main() {
    let hosts = vec!["localhost:9092", "localhost:9093", "localhost:9094"];
    let mut consumer = ConsumerClient::new(hosts);
}


