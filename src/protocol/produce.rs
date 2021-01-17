use std::io::Cursor;

use crate::protocol::primitives::{KafkaPrimitive, KafkaString, KafkaNullableString};
use crate::protocol::record::{Record, RecordBatch};
use crate::protocol::request::ToBytes;
use crate::protocol::response::FromBytes;

/// Produce Request (Version: 6) => transactional_id acks timeout [topic_data]
///   transactional_id => NULLABLE_STRING
///   acks => INT16
///   timeout => INT32
///   topic_data => topic [data]
///     topic => STRING
///     data => partition record_set
///       partition => INT32
///       record_set => RECORDS
pub struct ProduceRequest {
    transactional_id: KafkaNullableString,
    acks: i16,
    timeout: i32,
    topics: Vec<TopicRequest>,
}

struct TopicRequest {
    partition: i32,
    record_batches: Vec<RecordBatch>,
}

impl ProduceRequest {
    pub fn new() -> Self {
        Self {
            transactional_id: KafkaNullableString(None),
            acks: 0,
            timeout: 0,
            topics: vec![],
        }
    }
}

impl ToBytes for ProduceRequest {
    fn get_in_bytes(&self) -> Vec<u8> {
        let mut buffer = Vec::new();
        0_i32.write_to_buffer(&mut buffer); // 4 first bytes reserved for the size of message.
        self.transactional_id.write_to_buffer(&mut buffer);
        self.acks.write_to_buffer(&mut buffer);
        self.timeout.write_to_buffer(&mut buffer);

        (self.topics.len() as i32).write_to_buffer(&mut buffer);

        for topic in self.topics.iter() {
            topic.partition.write_to_buffer(&mut buffer);
            (topic.record_batches.len() as i32).write_to_buffer(&mut buffer);

            for record_batch in topic.record_batches.iter() {
                record_batch.base_offset.write_to_buffer(&mut buffer);
                record_batch.batch_length.write_to_buffer(&mut buffer);
                record_batch.partition_leader_epoch.write_to_buffer(&mut buffer);
                record_batch.magic.write_to_buffer(&mut buffer);
                record_batch.crc.write_to_buffer(&mut buffer);
                record_batch.attributes.write_to_buffer(&mut buffer);
                record_batch.last_offset_delta.write_to_buffer(&mut buffer);
                record_batch.first_timestamp.write_to_buffer(&mut buffer);
                record_batch.max_timestamp.write_to_buffer(&mut buffer);
                record_batch.producer_id.write_to_buffer(&mut buffer);
                record_batch.producer_epoch.write_to_buffer(&mut buffer);
                record_batch.base_sequence.write_to_buffer(&mut buffer);

                (record_batch.records.len() as i32).write_to_buffer(&mut buffer);

                for record in record_batch.records.iter() {
                    record.length.write_to_buffer(&mut buffer);
                    record.attributes.write_to_buffer(&mut buffer);
                    record.timestamp_delta.write_to_buffer(&mut buffer);
                    record.offset_delta.write_to_buffer(&mut buffer);
                    record.key_length.write_to_buffer(&mut buffer);
                    record.key.write_to_buffer(&mut buffer);
                    record.value_length.write_to_buffer(&mut buffer);
                    record.value.write_to_buffer(&mut buffer);

                    (record.headers.len() as i32).write_to_buffer(&mut buffer);

                    for header in record.headers.iter() {
                        header.header_key_length.write_to_buffer(&mut buffer);
                        header.header_key.write_to_buffer(&mut buffer);
                        header.header_value_length.write_to_buffer(&mut buffer);
                        header.header_value.write_to_buffer(&mut buffer);
                    }
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
pub struct ProduceResponse {
    responses: Vec<TopicResponse>,
    throttle_time_ms: i32,
}

pub struct TopicResponse {
    topic: KafkaString,
    partition_responses: Vec<PartitionResponse>,
}

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