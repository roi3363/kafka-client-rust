use std::io::{Cursor, Read};


pub fn read_rest(buffer: &mut Cursor<Vec<u8>>) {
    let mut buffer_rest = Vec::new();
    buffer.read_to_end(&mut buffer_rest).unwrap();
    println!("{:?}", buffer_rest.as_slice());
    // println!("{:?}", from_utf8(buffer_rest.as_slice()));
}