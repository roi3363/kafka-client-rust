use std::io::Cursor;

use crate::protocol::primitives::{KafkaPrimitive, KafkaString, KafkaNullableString, VarInt};
use crate::protocol::record::{Record, RecordBatch};
use crate::protocol::request::ToBytes;
use crate::protocol::response::FromBytes;


pub struct ProduceRequest {
    transactional_id: KafkaNullableString,
    acks: i16,
    timeout: i32,
    topic_data: Vec<ProduceTopicDataRequest>,
}

struct ProduceTopicDataRequest {
    pub topic: KafkaString,
    pub data: Vec<ProduceDataRequest>,
}

struct ProduceDataRequest {
    partition: i32,
    record_batches: Vec<RecordBatch>,
}

impl ProduceRequest {
    pub fn new(topic: String, key: Vec<u8>, value: Vec<u8>) -> Self {
        let record = Record::new(key, value);
        let produce_request = ProduceDataRequest {
            partition: 0,
            record_batches: vec![RecordBatch::new(vec![record])],
        };
        Self {
            transactional_id: KafkaNullableString(Some("kjh".to_string())),
            acks: 0,
            timeout: 5000,
            topic_data: vec![ProduceTopicDataRequest {
                topic: KafkaString(topic),
                data: vec![produce_request]
            }]
        }
    }
}

/// Produce Request (Version: 6) => transactional_id acks timeout [topic_data]
///   transactional_id => NULLABLE_STRING
///   acks => INT16
///   timeout => INT32
///   topic_data => topic [data]
///     topic => STRING
///     data => partition record_set
///       partition => INT32
///       record_set => RECORDS
impl ToBytes for ProduceRequest {
    fn get_in_bytes(&self) -> Vec<u8> {
        let mut buffer = Vec::new();
        self.transactional_id.write_to_buffer(&mut buffer);
        self.acks.write_to_buffer(&mut buffer);
        self.timeout.write_to_buffer(&mut buffer);
        (self.topic_data.len() as i32).write_to_buffer(&mut buffer);
        for topic_data in self.topic_data.iter() {
            topic_data.topic.write_to_buffer(&mut buffer);
            (topic_data.data.len() as i32).write_to_buffer(&mut buffer);
            for data in &topic_data.data {
                data.partition.write_to_buffer(&mut buffer);
                for record_batch in data.record_batches.iter() {
                    let record = record_batch.get_in_bytes();
                    (record.len() as i32).write_to_buffer(&mut buffer);
                    record.write_to_buffer(&mut buffer);
                }
            }
        }
        buffer
    }
}

/// Produce Response (Version: 6) => [responses] throttle_time_ms
///   responses => topic [partition_responses]
///     topic => STRING
///     partition_responses => partition error_code base_offset log_append_time log_start_offset
///       partition => INT32
///       error_code => INT16
///       base_offset => INT64
///       log_append_time => INT64
///       log_start_offset => INT64
///   throttle_time_ms => INT32
#[derive(Debug)]
pub struct ProduceResponse {
    responses: Vec<TopicResponse>,
    throttle_time_ms: i32,
}
#[derive(Debug)]
pub struct TopicResponse {
    topic: KafkaString,
    partition_responses: Vec<PartitionResponse>,
}
#[derive(Debug)]
struct PartitionResponse {
    partition: i32,
    error_code: i16,
    base_offset: i64,
    log_append_time: i64,
    log_start_offset: i64,
}

impl FromBytes for ProduceResponse {
    fn get_from_bytes(buffer: &mut Cursor<Vec<u8>>) -> Self {
        let mut response = Self {
            responses: vec![],
            throttle_time_ms: 0
        };

        let responses_len = i32::read_from_buffer(buffer);
        for _ in 0..responses_len {
            let mut topic_response = TopicResponse {
                topic: KafkaString::read_from_buffer(buffer),
                partition_responses: vec![]
            };

            let partition_responses_len = i32::read_from_buffer(buffer);
            for _ in 0..partition_responses_len {
                let partition_response = PartitionResponse {
                    partition: i32::read_from_buffer(buffer),
                    error_code: i16::read_from_buffer(buffer),
                    base_offset: i64::read_from_buffer(buffer),
                    log_append_time: i64::read_from_buffer(buffer),
                    log_start_offset: i64::read_from_buffer(buffer),
                };
                topic_response.partition_responses.push(partition_response);
            }
            response.responses.push(topic_response);
        }

        response.throttle_time_ms = i32::read_from_buffer(buffer);
        response
    }
}