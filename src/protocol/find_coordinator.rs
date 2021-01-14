use crate::protocol::primitives::{KafkaString, KafkaPrimitive, KafkaCompactString};
use crate::protocol::request::ToBytes;
use crate::protocol::response::FromBytes;
use std::io::Cursor;
use crate::protocol::kafka_error_codes::check_errors;

/// FindCoordinator Request (Version: 2) => key key_type
/// key => STRING
/// key_type => INT8
#[derive(Debug)]
pub struct FindCoordinatorRequest {
    key: KafkaString,
    key_type: i8,
}

impl FindCoordinatorRequest {
    pub fn new() -> Self {
        Self {
            key: KafkaString("".to_string()),
            key_type: 0
        }
    }
}

impl ToBytes for FindCoordinatorRequest {
    fn get_in_bytes(&self) -> Vec<u8> {
        let mut buffer = Vec::new();
        self.key.write_to_buffer(&mut buffer);
        self.key_type.write_to_buffer(&mut buffer);
        buffer
    }
}
/// FindCoordinator Response (Version: 3) => throttle_time_ms error_code error_message node_id host port TAG_BUFFER
///   throttle_time_ms => INT32
///   error_code => INT16
///   error_message => COMPACT_NULLABLE_STRING
///   node_id => INT32
///   host => COMPACT_STRING
///   port => INT32
#[derive(Debug)]
pub struct FindCoordinatorResponse {
    throttle_time_ms: i32,
    error_code: i16,
    error_message: KafkaString,
    node_id: i32,
    host: KafkaString,
    port: i32,
}

impl FromBytes for FindCoordinatorResponse {
    fn get_from_bytes(buffer: &mut Cursor<Vec<u8>>) -> Self {
        let response = Self {
            throttle_time_ms: i32::read_from_buffer(buffer),
            error_code: i16::read_from_buffer(buffer),
            error_message: KafkaString::read_from_buffer(buffer),
            node_id: i32::read_from_buffer(buffer),
            host: KafkaString::read_from_buffer(buffer),
            port: i32::read_from_buffer(buffer),
        };
        check_errors(response.error_code);
        response
    }
}