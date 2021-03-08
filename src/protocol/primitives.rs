use std::io::{Cursor, Read, Write};
use std::process::exit;
use std::str::from_utf8;
use serde::{Serialize, Deserialize};

use byteorder::{BE, ReadBytesExt, WriteBytesExt};

pub trait KafkaPrimitive {
    fn write_to_buffer(&self, buffer: &mut Vec<u8>);
    fn read_from_buffer(buffer: &mut Cursor<Vec<u8>>) -> Self;
    fn length(&self) -> usize;
}



#[derive(Debug, Clone, Serialize)]
pub struct VarInt(pub i32);

impl KafkaPrimitive for VarInt {
    fn write_to_buffer(&self, buffer: &mut Vec<u8>) {
        let mut value = self.0.clone();
        while (value as u32 & 0xffffff80) != 0 {
            let b: u8 = ((value & 0x7f) | 0x80) as u8;
            buffer.write_u8(b).unwrap();
            value >>= 7;
        }
        buffer.write_u8(value as u8).unwrap();
    }

    fn read_from_buffer(buffer: &mut Cursor<Vec<u8>>) -> Self {
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
        Self(value)
    }

    fn length(&self) -> usize {
        unimplemented!()
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct KafkaValue(pub Vec<u8>);

impl KafkaPrimitive for KafkaValue {
    fn write_to_buffer(&self, buffer: &mut Vec<u8>) {
        let value_len = VarInt(self.0.len() as i32);
        value_len.write_to_buffer(buffer);
        buffer.write_all(self.0.as_slice()).unwrap();
    }

    fn read_from_buffer(buffer: &mut Cursor<Vec<u8>>) -> Self {
        let value_len = VarInt::read_from_buffer(buffer);
        let mut value_buffer = vec![0 as u8; value_len.0 as usize];
        buffer.read_exact(&mut value_buffer).unwrap();
        Self(value_buffer)
    }

    fn length(&self) -> usize {
        unimplemented!()
    }
}



#[derive(Debug, Clone)]
pub struct KafkaCompactString(pub String);

impl KafkaPrimitive for KafkaCompactString {
    fn write_to_buffer(&self, buffer: &mut Vec<u8>) {
        let kafka_string = &self.0;
        buffer.write_i16::<BE>((kafka_string.len() + 1) as i16).unwrap();
        buffer.write_all(kafka_string.as_bytes()).unwrap();
    }

    fn read_from_buffer(buffer: &mut Cursor<Vec<u8>>) -> Self {
        let string_len = VarInt::read_from_buffer(buffer).0;
        // if string_len == -1 {
        //     return KafkaCompactString("".to_string());
        // }
        let mut string_buffer = vec![0 as u8; string_len as usize];
        buffer.read_exact(&mut string_buffer).unwrap();
        let kafka_string = from_utf8(string_buffer.as_slice()).unwrap().to_string();
        KafkaCompactString(kafka_string)
    }


    fn length(&self) -> usize {
        unimplemented!()
    }
}

#[derive(Debug, Clone)]
pub struct KafkaString(pub String);

impl KafkaPrimitive for KafkaString {
    fn write_to_buffer(&self, buffer: &mut Vec<u8>) {
        let kafka_string = &self.0;
        buffer.write_i16::<BE>(kafka_string.len() as i16).unwrap();
        buffer.write_all(kafka_string.as_bytes()).unwrap();
    }


    fn read_from_buffer(buffer: &mut Cursor<Vec<u8>>) -> Self {
        let string_len = buffer.read_i16::<BE>().unwrap();
        if string_len == -1 {
            return KafkaString("".to_string());
        }
        let mut string_buffer = vec![0 as u8; string_len as usize];
        buffer.read_exact(&mut string_buffer).unwrap();
        let kafka_string = from_utf8(string_buffer.as_slice()).unwrap().to_string();
        KafkaString(kafka_string)
    }


    fn length(&self) -> usize {
        unimplemented!()
    }
}

#[derive(Debug, Clone)]
pub struct KafkaNullableString(pub Option<String>);

impl KafkaPrimitive for KafkaNullableString {
    fn write_to_buffer(&self, buffer: &mut Vec<u8>) {
        if self.0.is_none() {
            buffer.write_i16::<BE>(-1).unwrap();
            return;
        }
        let kafka_string = self.0.as_ref().unwrap();
        buffer.write_i16::<BE>(kafka_string.len() as i16).unwrap();
        buffer.write_all(kafka_string.as_bytes()).unwrap();
    }

    fn read_from_buffer(buffer: &mut Cursor<Vec<u8>>) -> Self {
        let string_len = buffer.read_i16::<BE>().unwrap();
        if string_len == -1 {
            return KafkaNullableString(None);
        }
        let mut string_buffer = vec![0 as u8; string_len as usize];
        buffer.read_exact(&mut string_buffer).unwrap();
        let kafka_string = from_utf8(string_buffer.as_slice()).unwrap().to_string();
        KafkaNullableString(Some(kafka_string))
    }


    fn length(&self) -> usize {
        unimplemented!()
    }
}


#[derive(Debug, Clone)]
pub struct KafkaArray<T: KafkaPrimitive>(pub Vec<T>);

impl<T: KafkaPrimitive> KafkaPrimitive for KafkaArray<T> {
    fn write_to_buffer(&self, buffer: &mut Vec<u8>) {
        if self.0.len() == 0 {
            buffer.write_i32::<BE>(-1).unwrap();
            return;
        }
        let kafka_array = &self.0;
        buffer.write_i32::<BE>(kafka_array.len() as i32).unwrap();
        for i in kafka_array {
            i.write_to_buffer(buffer)
        }
    }

    fn read_from_buffer(buffer: &mut Cursor<Vec<u8>>) -> Self {
        let array_len = buffer.read_i32::<BE>().unwrap();
        let mut array_buffer = Vec::with_capacity(*&array_len as usize);
        buffer.read_exact(&mut array_buffer).unwrap();

        let mut kafka_array = vec![];
        for _ in 0..array_len {
            kafka_array.push(KafkaPrimitive::read_from_buffer(buffer));
        }
        KafkaArray(kafka_array)
    }


    fn length(&self) -> usize {
        unimplemented!()
    }
}


impl KafkaPrimitive for bool {
    fn write_to_buffer(&self, buffer: &mut Vec<u8>) {
        buffer.write_i8(*self as i8).unwrap();
    }

    fn read_from_buffer(buffer: &mut Cursor<Vec<u8>>) -> Self {
        buffer.read_i8().unwrap() != 0
    }


    fn length(&self) -> usize {
        1
    }
}

impl KafkaPrimitive for i8 {
    fn write_to_buffer(&self, buffer: &mut Vec<u8>) {
        buffer.write_i8(*self).unwrap();
    }

    fn read_from_buffer(buffer: &mut Cursor<Vec<u8>>) -> Self {
        buffer.read_i8().unwrap()
    }


    fn length(&self) -> usize {
        1
    }
}

impl KafkaPrimitive for i16 {
    fn write_to_buffer(&self, buffer: &mut Vec<u8>) {
        buffer.write_i16::<BE>(*self).unwrap();
    }

    fn read_from_buffer(buffer: &mut Cursor<Vec<u8>>) -> Self {
        buffer.read_i16::<BE>().unwrap()
    }


    fn length(&self) -> usize {
        2
    }
}

impl KafkaPrimitive for i32 {
    fn write_to_buffer(&self, buffer: &mut Vec<u8>) {
        buffer.write_i32::<BE>(*self).unwrap();
    }

    fn read_from_buffer(buffer: &mut Cursor<Vec<u8>>) -> Self {
        buffer.read_i32::<BE>().unwrap()
    }


    fn length(&self) -> usize {
        4
    }
}

impl KafkaPrimitive for i64 {
    fn write_to_buffer(&self, buffer: &mut Vec<u8>) {
        buffer.write_i64::<BE>(*self).unwrap();
    }

    fn read_from_buffer(buffer: &mut Cursor<Vec<u8>>) -> Self {
        buffer.read_i64::<BE>().unwrap()
    }


    fn length(&self) -> usize {
        8
    }
}

impl KafkaPrimitive for &str {
    fn write_to_buffer(&self, buffer: &mut Vec<u8>) {
        buffer.write_all(self.as_bytes()).unwrap();
    }

    fn read_from_buffer(buffer: &mut Cursor<Vec<u8>>) -> Self {
        unimplemented!()
    }


    fn length(&self) -> usize {
        self.len()
    }
}

impl KafkaPrimitive for String {
    fn write_to_buffer(&self, buffer: &mut Vec<u8>) {
        buffer.write_all(self.as_bytes()).unwrap();
    }

    fn read_from_buffer(buffer: &mut Cursor<Vec<u8>>) -> Self {
        unimplemented!()
    }

    fn length(&self) -> usize {
        self.len()
    }
}

impl KafkaPrimitive for Vec<u8> {
    fn write_to_buffer(&self, buffer: &mut Vec<u8>) {
        buffer.write_all(self.as_slice()).unwrap();
    }

    fn read_from_buffer(buffer: &mut Cursor<Vec<u8>>) -> Self {
        let mut bytes_buffer = Vec::new();
        buffer.read_exact(&mut bytes_buffer).unwrap();
        bytes_buffer
    }


    fn length(&self) -> usize {
        unimplemented!()
    }
}

impl<T: KafkaPrimitive + Copy + Default> KafkaPrimitive for Vec<T> {
    fn write_to_buffer(&self, buffer: &mut Vec<u8>) {
        for i in self.iter() {
            i.write_to_buffer(buffer)
        }
    }

    fn read_from_buffer(buffer: &mut Cursor<Vec<u8>>) -> Self {
        unimplemented!()
    }


    fn length(&self) -> usize {
        unimplemented!()
    }
}

impl<T: KafkaPrimitive + Copy + Default> KafkaPrimitive for &[T] {
    fn write_to_buffer(&self, buffer: &mut Vec<u8>) {
        for i in *self {
            i.write_to_buffer(buffer)
        }
    }

    fn read_from_buffer(buffer: &mut Cursor<Vec<u8>>) -> Self {
        unimplemented!()
    }


    fn length(&self) -> usize {
        unimplemented!()
    }
}
