use crate::protocol::create_topic::CreateTopicRequest;
use std::net::TcpStream;
use std::io::Write;
use crate::protocol::top_level::Request;

pub struct AdminClient {
    pub host: String,
}

impl AdminClient {

    pub fn new(host: String) -> Self {
        Self { host }
    }

    pub fn create_topic(&self, topic: &str, num_partitions: i32, replication_factor: i16) {
        let create_topic = CreateTopicRequest::new(topic, num_partitions, replication_factor);
        let buffer = Request::build(create_topic);
        match TcpStream::connect(self.host.as_str()) {
            Ok(mut stream) => {
                stream.write(buffer.as_slice()).unwrap();
            }
            Err(e) => {
                println!("Failed to connect: {}", e);
            }
        }
    }
}