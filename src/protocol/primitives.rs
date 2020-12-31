use byteorder::{WriteBytesExt, BE, ReadBytesExt};
use std::io::{Write, Cursor, Read};
use std::str::from_utf8;



pub trait KafkaPrimitive {
    fn write_to_buffer(self, buffer: &mut Vec<u8>);
    fn read_from_buffer(self, buffer: &mut Cursor<Vec<u8>>) -> Self;
}


impl KafkaPrimitive for bool {

    fn write_to_buffer(self, buffer: &mut Vec<u8>) {
        buffer.write_i8(self as i8).unwrap();
    }

    fn read_from_buffer(self, buffer: &mut Cursor<Vec<u8>>) -> Self {
        buffer.read_i8().unwrap() != 0
    }
}

impl KafkaPrimitive for i8 {

    fn write_to_buffer(self, buffer: &mut Vec<u8>) {
        buffer.write_i8(self).unwrap();
    }

    fn read_from_buffer(self, buffer: &mut Cursor<Vec<u8>>) -> Self {
        buffer.read_i8().unwrap()
    }
}

impl KafkaPrimitive for i16 {

    fn write_to_buffer(self, buffer: &mut Vec<u8>) {
        buffer.write_i16::<BE>(self).unwrap();
    }

    fn read_from_buffer(self, buffer: &mut Cursor<Vec<u8>>) -> Self {
        buffer.read_i16::<BE>().unwrap()
    }
}

impl KafkaPrimitive for i32 {
    fn write_to_buffer(self, buffer: &mut Vec<u8>) {
        buffer.write_i32::<BE>(self).unwrap();
    }

    fn read_from_buffer(self, buffer: &mut Cursor<Vec<u8>>) -> Self {
        buffer.read_i32::<BE>().unwrap()
    }
}

impl KafkaPrimitive for i64 {

    fn write_to_buffer(self, buffer: &mut Vec<u8>) {
        buffer.write_i64::<BE>(self).unwrap();
    }

    fn read_from_buffer(self, buffer: &mut Cursor<Vec<u8>>) -> Self {
        buffer.read_i64::<BE>().unwrap()
    }
}

impl KafkaPrimitive for &str {

    fn write_to_buffer(self, buffer: &mut Vec<u8>) {
        buffer.write_i16::<BE>(self.len() as i16).unwrap();
        buffer.write_all(self.as_bytes()).unwrap();
    }

    fn read_from_buffer(mut self, buffer: &mut Cursor<Vec<u8>>) -> Self {
        unimplemented!()
    }
}

impl KafkaPrimitive for String {

    fn write_to_buffer(self, buffer: &mut Vec<u8>) {
        buffer.write_i16::<BE>(self.len() as i16).unwrap();
        buffer.write_all(self.as_bytes()).unwrap();
    }

    fn read_from_buffer(mut self, buffer: &mut Cursor<Vec<u8>>) -> Self {
        let string_len = buffer.read_i16::<BE>().unwrap();
        if string_len == -1 {
            return "".to_string();
        }
        let mut string_buffer = vec![0 as u8; string_len as usize];
        buffer.read_exact(&mut string_buffer).unwrap();
        self = from_utf8(string_buffer.as_slice()).unwrap().to_string();
        self
    }
}

impl KafkaPrimitive for Vec<u8> {
    fn write_to_buffer(self, buffer: &mut Vec<u8>) {
        buffer.write_all(self.as_slice()).unwrap();
    }

    fn read_from_buffer(mut self, buffer: &mut Cursor<Vec<u8>>) -> Self {
        buffer.read_exact(&mut self).unwrap();
        self
    }
}

impl <T: KafkaPrimitive + Copy + Default> KafkaPrimitive for Vec<T> {
    fn write_to_buffer(self, buffer: &mut Vec<u8>) {
        buffer.write_i32::<BE>(self.len() as i32).unwrap();
        for i in self.iter() {
            i.write_to_buffer(buffer)
        }
    }

    fn read_from_buffer(mut self, buffer: &mut Cursor<Vec<u8>>) -> Self {
        let array_len = buffer.read_i32::<BE>().unwrap();
        let mut array_buffer = Vec::with_capacity(array_len as usize);
        buffer.read_exact(&mut array_buffer).unwrap();
        for i in vec![T::default(); array_len as usize] {
            self.push(i.read_from_buffer(buffer));
        }
        self
    }
}

impl <T: KafkaPrimitive + Copy + Default> KafkaPrimitive for &[T] {
    fn write_to_buffer(self, buffer: &mut Vec<u8>) {
        buffer.write_i32::<BE>(self.len() as i32).unwrap();
        for i in self {
            i.write_to_buffer(buffer)
        }
    }

    fn read_from_buffer(self, buffer: &mut Cursor<Vec<u8>>) -> Self {
        unimplemented!()
    }
}
