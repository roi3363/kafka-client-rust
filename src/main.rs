#[macro_use]
extern crate lazy_static;

mod protocol;
mod utils;
mod clients;

use crate::clients::admin_client::AdminClient;
use crate::clients::consumer_client::ConsumerClient;
use crate::protocol::primitives::DataTypes;

fn main() {
    let host = "localhost:9092";
    let client = ConsumerClient::new(host.to_string());
    let adm_client = AdminClient::new(host.to_string());
    client.fetch_metadata();
}