#[macro_use] extern crate lazy_static;
// extern crate proc_macro;

mod protocol;
mod clients;
mod utils;
use protocol::kafka_error_codes::KAFKA_ERRORS;
use crate::clients::consumer_client::ConsumerClient;
use std::time::Instant;

fn main() {
    env_logger::init();
    let time = Instant::now();
    let hosts = vec!["localhost:9092","localhost:9093","localhost:9094"];
    let mut consumer = ConsumerClient::new(hosts);
    // let mut admin = AdminClient::new(hosts.clone());
    // admin.create_topic("test2", 6, 1);
    let topics = vec!["test"];
    consumer.fetch(topics);
    println!("{:#?}", time.elapsed());
}


