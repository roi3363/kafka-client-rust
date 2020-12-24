use crate::protocol::header::{RequestHeader};
use crate::utils::buffer_writer::BufferWriter;
use std::net::TcpStream;

pub trait FromBytes {
    fn get_from_bytes(stream: &mut TcpStream) -> Vec<u8>;
}

pub trait ToBytes {
    fn get_in_bytes(&self) -> Vec<u8>;
    fn header(&self) -> RequestHeader;
}

#[derive(Debug)]
pub struct Request<T: ToBytes> {
    pub size: i32,
    pub header: RequestHeader,
    pub body: T,
}

impl<T: ToBytes> Request<T> {
    pub fn build(request: T) -> Vec<u8> {
        let mut buffer = request.header().get_in_bytes();
        let body = request.get_in_bytes();
        buffer.extend(body);
        BufferWriter::add_size(&mut buffer);
        buffer
    }
}