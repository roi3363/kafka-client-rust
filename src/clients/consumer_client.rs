use std::net::TcpStream;
use std::io::{Write, Read, BufReader};
use crate::protocol::fetch::FetchRequest;
use crate::protocol::header::RequestHeader;
use crate::utils::buffer_writer::{BufferWriter};
use crate::protocol::top_level::{ToBytes, Request, FromBytes};
use crate::protocol::api_versions::ApiVersionsRequest;
use std::str::from_utf8;
use byteorder::{ReadBytesExt, BigEndian};
use crate::protocol::primitives::DataTypes;
use crate::utils::buffer_reader::BufferReader;
use crate::protocol::metadata::request::MetadataRequest;
use crate::protocol::metadata::response::MetadataResponse;

pub struct ConsumerClient {
    pub host: String,
}

impl ConsumerClient {
    pub fn new(host: String) -> Self {
        Self { host }
    }

    pub fn get_api_versions(&self) {
        let api_versions = ApiVersionsRequest::new();
        let buffer = Request::build(api_versions);
        match TcpStream::connect(self.host.as_str()) {
            Ok(mut stream) => {
                stream.write(buffer.as_slice()).unwrap();
                let response = BufferReader::read_response(&mut stream);
                println!("{:?}", response);
            }
            Err(e) => {
                println!("Failed to connect: {}", e);
            }
        };
    }

    pub fn fetch_metadata(&self) {
        let metadata = MetadataRequest::new(vec!["roi".to_string()]);
        let buffer = Request::build(metadata);
        match TcpStream::connect(self.host.as_str()) {
            Ok(mut stream) => {
                stream.write(buffer.as_slice()).unwrap();
                let response = MetadataResponse::get_from_bytes(&mut stream);
                println!("{:?}", response);
            }
            Err(e) => {
                println!("Failed to connect: {}", e);
            }
        }
    }

    pub fn fetch(&self) {
        // let fetch = FetchRequest::new("test2");
    }
}