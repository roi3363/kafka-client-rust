use std::net::TcpStream;
use crate::clients::connection::connect_to_server;

pub struct ConsumerClient  {
    pub host: String,
    pub stream: TcpStream,
}

impl ConsumerClient {
    pub fn new(host: String) -> Self {
        let stream = connect_to_server(host.as_str());
        Self { host, stream }
    }
}