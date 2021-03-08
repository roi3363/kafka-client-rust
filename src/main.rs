#[macro_use] extern crate lazy_static;

mod protocol;
mod clients;
mod utils;
mod config;



use std::borrow::Borrow;
use std::collections::HashMap;
use std::str::from_utf8;
use std::time::Instant;
use serde_json::json;
use serde::{Deserialize, Serialize};
use protocol::kafka_error_codes::KAFKA_ERRORS;

use crate::clients::consumer_client::ConsumerClient;
use std::any::Any;
use regex::Regex;
use crate::protocol::primitives::{KafkaString};

fn main() {
    let hosts = vec!["localhost:9092"];
    let topics = vec!["test2"];
    let mut consumer = ConsumerClient::new(hosts);
    consumer.produce("test4", "test".as_bytes().to_vec(), "value".as_bytes().to_vec());
    // consumer.fetch(topics);
}


