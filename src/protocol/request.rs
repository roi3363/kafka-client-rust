use crate::protocol::header::RequestHeader;

pub trait ToBytes {
    fn get_in_bytes(&self) -> Vec<u8>;
}

#[derive(Debug)]
pub struct Request<T: ToBytes> {
    pub size: i32,
    pub header: RequestHeader,
    pub body: T,
    pub buffer: Vec<u8>,
}

impl<T: ToBytes> Request<T> {
    pub fn new(header: RequestHeader, body: T) -> Self {
        let mut buffer = header.get_in_bytes();
        buffer.extend(body.get_in_bytes());
        let size = Self::add_size(&mut buffer);
        Self {
            size,
            header,
            body,
            buffer,
        }
    }

    pub fn add_size(buffer: &mut Vec<u8>) -> i32 {
        let buffer_length = (buffer.len() - 4) as i32; // 4 bytes kept for the size of the message
        buffer.splice(0..4, buffer_length.to_be_bytes().to_vec());
        buffer_length
    }
}


