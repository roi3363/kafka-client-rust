use crate::utils::buffer_writer::BufferWriter;
use byteorder::{ReadBytesExt, BigEndian};
use std::net::TcpStream;
use std::process::exit;
use std::io::{Read, BufReader};
use crate::protocol::primitives::DataTypes;
use std::str::from_utf8;

pub struct BufferReader {
    pub buffer: Vec<u8>
}

impl BufferReader {
    pub fn new() -> Self {
        Self { buffer: Vec::new() }
    }

    pub fn read_response(stream: &mut TcpStream) -> Vec<u8> {
        let size = BufferReader::get_response_size(stream);
        let mut buffer = vec![0 as u8; size];
        stream.read_exact(&mut buffer).unwrap();
        buffer
    }

    pub fn get_response_size(stream: &mut TcpStream) -> usize {
        let mut buffer = vec![0 as u8; 4];
        match stream.read_exact(&mut buffer) {
            Ok(_) => {
                return buffer.as_slice().read_i32::<BigEndian>().unwrap() as usize;
            }
            Err(e) => {
                println!("Failed to receive data: {}", e);
                exit(1);
            }
        }
    }
    
    pub fn get_response_size2(buffer_reader: &mut BufReader<&mut TcpStream>) -> i32 {
        buffer_reader.read_i32::<BigEndian>().unwrap()
    }

    pub fn read_int(stream: &mut TcpStream, int_types: DataTypes) -> usize {
        match int_types {
            DataTypes::Int8 => {
                let mut buffer = vec![0; 1];
                stream.read_exact(&mut buffer).unwrap();
                buffer.as_slice().read_i8().unwrap() as usize
            }
            DataTypes::Int16 => {
                let mut buffer = vec![0; 2];
                stream.read_exact(&mut buffer).unwrap();
                buffer.as_slice().read_i16::<BigEndian>().unwrap() as usize
            }
            DataTypes::Int32 => {
                let mut buffer = vec![0; 4];
                stream.read_exact(&mut buffer).unwrap();
                buffer.as_slice().read_i32::<BigEndian>().unwrap() as usize
            }
            DataTypes::Int64 => {
                let mut buffer = vec![0; 8];
                stream.read_exact(&mut buffer).unwrap();
                buffer.as_slice().read_i64::<BigEndian>().unwrap() as usize
            }
        }
    }

    pub fn read_str(stream: &mut TcpStream) -> String {
        let size = Self::read_int(stream, DataTypes::Int16);
        let mut buffer = vec![0 as u8; size];
        stream.read_exact(&mut buffer).unwrap();
        let value = from_utf8(&buffer).unwrap().to_string();
        value
    }

    pub fn read_array_int(stream: &mut TcpStream, int_types: DataTypes) -> Vec<usize> {
        let size = Self::read_int(stream, DataTypes::Int32);
        let mut array_int: Vec<usize> = Vec::new();
        for _ in 0..size {
            array_int.push(Self::read_int(stream, int_types));
        }
        array_int
    }

    pub fn read_array_str(stream: &mut TcpStream) -> Vec<String> {
        let size = Self::read_int(stream, DataTypes::Int32);
        let mut array_str: Vec<String> = Vec::new();
        for _ in 0..size {
            array_str.push(Self::read_str(stream));
        }
        array_str
    }
}