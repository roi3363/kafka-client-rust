use crate::protocol::request::ToBytes;
use crate::protocol::primitives::{KafkaPrimitive, KafkaString};
use crate::protocol::response::FromBytes;
use std::io::Cursor;
use crate::protocol::kafka_error_codes::{KAFKA_ERRORS, check_errors};


pub struct CommitOffsetRequest {
    group_id: KafkaString,
    generation_id: i32,
    member_id: KafkaString,
    topics: Vec<TopicRequest>,
}

struct TopicRequest {
    name: KafkaString,
    partitions: Vec<PartitionRequest>,
}

struct PartitionRequest {
    partition_index: i32,
    committed_offset: i64,
    committed_leader_epoch: i32,
    committed_metadata: KafkaString,
}

impl CommitOffsetRequest {
    pub fn new(group_id: String) -> Self {
        let partition = PartitionRequest {
            partition_index: 0,
            committed_offset: 10,
            committed_leader_epoch: 0,
            committed_metadata: KafkaString("".to_string())
        };
        let topic = TopicRequest {
            name: KafkaString("test".to_string()),
            partitions: vec![partition]
        };
        Self {
            group_id: KafkaString("roi".to_string()),
            generation_id: 1,
            member_id: KafkaString("roi".to_string()),
            topics: vec![topic],
        }
    }
}

/// OffsetCommit Request (Version: 6) => group_id generation_id member_id [topics]
///   group_id => STRING
///   generation_id => INT32
///   member_id => STRING
///   topics => name [partitions]
///     name => STRING
///     partitions => partition_index committed_offset committed_leader_epoch committed_metadata
///       partition_index => INT32
///       committed_offset => INT64
///       committed_leader_epoch => INT32
///       committed_metadata => NULLABLE_STRING
///
impl ToBytes for CommitOffsetRequest {
    fn get_in_bytes(&self) -> Vec<u8> {
        let mut buffer = Vec::new();
        self.group_id.write_to_buffer(&mut buffer);
        self.generation_id.write_to_buffer(&mut buffer);
        self.member_id.write_to_buffer(&mut buffer);

        let topics_len = self.topics.len() as i32;
        topics_len.write_to_buffer(&mut buffer);
        for topic in self.topics.iter() {
            topic.name.write_to_buffer(&mut buffer);

            let partitions_len = topic.partitions.len() as i32;
            partitions_len.write_to_buffer(&mut buffer);
            for partition in topic.partitions.iter() {
                partition.partition_index.write_to_buffer(&mut buffer);
                partition.committed_offset.write_to_buffer(&mut buffer);
                partition.committed_leader_epoch.write_to_buffer(&mut buffer);
                partition.committed_metadata.write_to_buffer(&mut buffer);
            }
        }
        buffer

    }
}

/// OffsetCommit Response (Version: 6) => throttle_time_ms [topics]
///   throttle_time_ms => INT32
///   topics => name [partitions]
///     name => STRING
///     partitions => partition_index error_code
///       partition_index => INT32
///       error_code => INT16
#[derive(Debug)]
pub struct CommitOffsetResponse {
    throttle_time_ms: i32,
    topics: Vec<TopicResponse>,
}
#[derive(Debug)]
struct TopicResponse {
    name: KafkaString,
    partitions: Vec<PartitionResponse>,
}

#[derive(Debug)]
struct PartitionResponse {
    partition_index: i32,
    error_code: i16,
}

impl FromBytes for CommitOffsetResponse {
    fn get_from_bytes(buffer: &mut Cursor<Vec<u8>>) -> Self {
        let mut response = Self {
            throttle_time_ms: i32::read_from_buffer(buffer),
            topics: vec![]
        };
        let topic_len = i32::read_from_buffer(buffer);
        for _ in 0..topic_len {
            let mut topic = TopicResponse {
                name: KafkaString::read_from_buffer(buffer),
                partitions: vec![]
            };
            let partitions_length = i32::read_from_buffer(buffer);
            for _ in 0..partitions_length {
                let partition = PartitionResponse {
                    partition_index: i32::read_from_buffer(buffer),
                    error_code: i16::read_from_buffer(buffer),
                };
                if partition.error_code != 0 {
                    check_errors(partition.error_code);
                }
                topic.partitions.push(partition);
            }
            response.topics.push(topic);
        }
        response
    }
}