use std::str::from_utf8;
use crate::protocol::primitives::KafkaPrimitive;
use std::io::{Cursor, Read};
use byteorder::ReadBytesExt;

pub struct BufferUtils {}

impl BufferUtils {
    pub fn read_string(length: i32, buffer: &mut Cursor<Vec<u8>>) -> String {
        let mut key_buffer = vec![0 as u8; length as usize];
        key_buffer = key_buffer.read_from_buffer(buffer);
        let key = from_utf8(key_buffer.as_slice()).unwrap();
        key.to_string()
    }

    pub fn read_varint(buffer: &mut Cursor<Vec<u8>>) -> i32 {
        let mut i = 0;
        let mut value = 0;
        let mut b = buffer.read_u8().unwrap() as i32;
        while (b & 0x80) != 0 {
            value |= (b & 0x7f) << i;
            i += 7;
            b = buffer.read_u8().unwrap() as i32;
        }
        value |= b << i;
        value >>= 1;
        value
    }

    pub fn read_rest(buffer: &mut Cursor<Vec<u8>>) {
        let mut buffer_rest = Vec::new();
        buffer.read_to_end(&mut buffer_rest).unwrap();
        println!("{:?}", buffer_rest.as_slice());
        // println!("{:?}", from_utf8(buffer_rest.as_slice()));
    }
}