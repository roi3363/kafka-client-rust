use crate::utils::buffer_writer::{BufferWriter};
use serde::{Serialize};
use byteorder::{ReadBytesExt, BigEndian};
use std::str::from_utf8;
use crate::protocol::primitives::DataTypes;
use crate::protocol::top_level::ToBytes;


#[derive(Debug, Clone)]
pub struct RequestHeader {
    pub api_key: i16,
    pub api_version: i16,
    pub correlation_id: i32,
    pub client_id: String,
}

#[derive(Debug)]
pub struct ResponseHeader {
    pub correlation_id: i32,
}

impl RequestHeader {
    pub fn new(api_key: i16, api_version: i16, correlation_id: i32, client_id: String) -> Self {
        Self { api_key, api_version, correlation_id, client_id, }
    }

    pub fn print_header(buffer: &Vec<u8>) {
        println!("size: {}", buffer[0..4].to_vec().as_slice().read_i32::<BigEndian>().unwrap());
        println!("api_key: {}", buffer[4..6].to_vec().as_slice().read_i16::<BigEndian>().unwrap());
        println!("api_version: {}", buffer[6..8].to_vec().as_slice().read_i16::<BigEndian>().unwrap());
        println!("correlation_id: {}", buffer[8..12].to_vec().as_slice().read_i32::<BigEndian>().unwrap());
        println!("client_id_length: {}", buffer[12..13].to_vec().as_slice().read_i32::<BigEndian>().unwrap());
        println!("correlation_id: {}", from_utf8(&buffer[13..16]).unwrap().to_string());
    }
}

impl ToBytes for RequestHeader {
    fn get_in_bytes(&self) -> Vec<u8> {
        let mut buffer_writer = BufferWriter::new();
        buffer_writer.write_int32(0); // 4 first bytes reserved for the size of message.
        buffer_writer.write_int16(self.api_key);
        buffer_writer.write_int16(self.api_version);
        buffer_writer.write_int32(self.correlation_id);
        buffer_writer.write_str(&self.client_id);
        buffer_writer.buffer
    }

    fn header(&self) -> RequestHeader { unimplemented!() }
}









