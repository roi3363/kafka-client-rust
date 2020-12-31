



use crate::protocol::primitives::{KafkaPrimitive};
use std::io::Cursor;
use crate::protocol::request::ToBytes;
use crate::protocol::response::FromBytes;

// -------------------------
// -------- Request --------
// -------------------------

#[derive(Debug, Clone)]
pub struct RequestHeader {
    pub api_key: i16,
    pub api_version: i16,
    pub correlation_id: i32,
    pub client_id: String,
}


impl RequestHeader {
    pub fn new(api_key: i16, api_version: i16, correlation_id: i32, client_id: String) -> Self {
        Self { api_key, api_version, correlation_id, client_id, }
    }
}

impl ToBytes for RequestHeader {
    fn get_in_bytes(&self) -> Vec<u8> {
        let mut buffer = Vec::new();
        0.write_to_buffer(&mut buffer); // 4 first bytes reserved for the size of message.
        self.api_key.write_to_buffer(&mut buffer);
        self.api_version.write_to_buffer(&mut buffer);
        self.correlation_id.write_to_buffer(&mut buffer);
        self.client_id.to_string().write_to_buffer(&mut buffer);
        buffer
    }
}

// -------------------------
// -------- Response -------
// -------------------------

#[derive(Debug)]
pub struct ResponseHeader {
    pub correlation_id: i32,
}


impl ResponseHeader {
    pub fn new() -> Self {
        Self {
            correlation_id: 0,
        }
    }
}

impl FromBytes for ResponseHeader {
    fn get_from_bytes(buffer: &mut Cursor<Vec<u8>>) -> Self {
        let mut response = Self {
            correlation_id: 0.read_from_buffer(buffer),
        };
        response
    }
}









