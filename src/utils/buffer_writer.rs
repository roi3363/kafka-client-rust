use byteorder::{WriteBytesExt, BigEndian};
use std::io::{Write, Read, Cursor};



pub struct BufferWriter {
    pub buffer: Vec<u8>
}

impl BufferWriter {
    pub fn new() -> Self {
        Self { buffer: Vec::new() }
    }

    pub fn write_bool(&mut self, value: bool) {
        self.buffer.write_i8(i8::from(value)).unwrap();
    }

    pub fn write_int8(&mut self, value: i8) {
        self.buffer.write_i8(value).unwrap();
    }

    pub fn write_int16(&mut self, value: i16) {
        self.buffer.write_i16::<BigEndian>(value).unwrap();
    }

    pub fn write_int32(&mut self, value: i32) {
        self.buffer.write_i32::<BigEndian>(value).unwrap()
    }

    pub fn write_int64(&mut self, value: i64) {
        self.buffer.write_i64::<BigEndian>(value).unwrap()
    }

    pub fn write_str(&mut self, value: &str) {
        self.buffer.write_i16::<BigEndian>(value.len() as i16).unwrap();
        self.buffer.write_all(value.as_bytes()).unwrap();
    }

    pub fn write_compact_str(&mut self, value: &str) {
        self.buffer.write_u32::<BigEndian>((value.len() + 1) as u32).unwrap();
        self.buffer.write_all(value.as_bytes()).unwrap();
    }

    pub fn add_size(buffer: &mut Vec<u8>) {
        let buffer_length_bytes = &(buffer.len() - 4).to_be_bytes()[4..];
        buffer.splice(0..4, buffer_length_bytes.iter().cloned());
    }
}