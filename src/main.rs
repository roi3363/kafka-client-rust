use crate::clients::admin_client::AdminClient;
use std::thread::sleep;
use std::time::Duration;

mod protocol;
mod clients;


fn main() {
    let host = "localhost:9092";
    let mut client = AdminClient::new(host.to_string());
    // client.create_topic("roi3", 2, 1);
    // client.fetch_metadata(vec!["roi", "roi2"]);
    client.fetch(vec!["test"]);
}
