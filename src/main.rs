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


#[derive(Serialize, Deserialize, PartialEq, Debug)]
struct Entity {
    name: String,
    age: i8,
}




fn main() {
    // let hosts = vec!["localhost:9092", "localhost:9093", "localhost:9094"];
    // let mut consumer = ConsumerClient::new(hosts);
    let entity = Entity { name: "roi".to_string(), age: 23 };



    struct_value!({
            String: age => "i8".to_string(),
            Vec<String>: a => vec!["roi2".to_string(), "roi".to_string()],
        });

    // println!("{:#?}", Data::fields());
}


