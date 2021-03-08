use crate::protocol::header::ResponseHeader;
use std::io::{Cursor, Read};
use std::net::TcpStream;
use byteorder::{ReadBytesExt, BE};
use std::process::exit;
use std::any::{Any, type_name};

pub trait FromBytes {
    fn get_from_bytes(buffer: &mut Cursor<Vec<u8>>) -> Self;
}

#[derive(Debug)]
pub struct Response<T: FromBytes> {
    pub message_size: i32,
    pub header: ResponseHeader,
    pub body: T,
}

impl<T: FromBytes> Response<T> {
    pub fn build(stream: &mut TcpStream) -> Self {
        let mut buffer = vec![0 as u8; 4]; // Buffer for the size of the message
        let message_size: i32;
        buffer = match stream.read_exact(&mut buffer) {
            Ok(_) => {
                message_size = buffer.as_slice().read_i32::<BE>().unwrap();
                buffer = vec![0 as u8; message_size as usize]; // Buffer for the message itself
                stream.read_exact(&mut buffer).unwrap();
                buffer
            }
            Err(e) => {
                println!("{:#?}", type_name::<T>());
                eprintln!("Message could not be retrieved. \nError: {}", e);
                exit(1)
            }
        };

        let mut buffer = Cursor::new(buffer);
        let header = ResponseHeader::get_from_bytes(&mut buffer);
        let body = T::get_from_bytes(&mut buffer);
        Self {
            message_size,
            header,
            body,
        }
    }
}



