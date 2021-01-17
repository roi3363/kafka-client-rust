use crate::protocol::request::ToBytes;
use crate::protocol::response::FromBytes;
use std::io::Cursor;
use crate::protocol::primitives::{KafkaString, KafkaPrimitive};

/// ListOffsets Request (Version: 3) => replica_id isolation_level [topics]
///   replica_id => INT32
///   isolation_level => INT8
///   topics => name [partitions]
///     name => STRING
///     partitions => partition_index timestamp
///       partition_index => INT32
///       timestamp => INT64

pub struct ListOffsetsRequest {
    replica_id: i32,
    isolation_level: i8,
    topics: Vec<TopicRequest>,
}

struct TopicRequest {
    name: KafkaString,
    partitions: Vec<PartitionRequest>
}

struct PartitionRequest {
    partition_index: i32,
    timestamp: i64,
}

impl ListOffsetsRequest {
    pub fn new(topics: &Vec<&str>) -> Self {
        Self {
            replica_id: -1,
            isolation_level: 0,
            topics: vec![]
        }
    }
}


impl ToBytes for ListOffsetsRequest {
    fn get_in_bytes(&self) -> Vec<u8> {
        let mut buffer = Vec::new();
        self.replica_id.write_to_buffer(&mut buffer);
        self.isolation_level.write_to_buffer(&mut buffer);


        let topics_len = self.topics.len() as i32;
        topics_len.write_to_buffer(&mut buffer);
        for topic in self.topics.iter() {
            topic.name.write_to_buffer(&mut buffer);

            let partitions_len = topic.partitions.len() as i32;
            partitions_len.write_to_buffer(&mut buffer);
            for partition in topic.partitions.iter() {
                partition.partition_index.write_to_buffer(&mut buffer);
                partition.timestamp.write_to_buffer(&mut buffer);
            }
        }
        buffer
    }
}


/// ListOffsets Response (Version: 3) => throttle_time_ms [topics]
///  throttle_time_ms => INT32
///  topics => name [partitions]
///    name => STRING
///    partitions => partition_index error_code timestamp offset
///      partition_index => INT32
///      error_code => INT16
///      timestamp => INT64
///      offset => INT64
///
///
#[derive(Debug)]
pub struct ListOffsetsResponse {
    throttle_time_ms: i32,
    topics: Vec<TopicResponse>,

}

#[derive(Debug)]
struct TopicResponse {
    name: KafkaString,
    partitions: Vec<PartitionResponse>
}

#[derive(Debug)]
struct PartitionResponse {
    partition_index: i32,
    timestamp: i64,
    offset: i64,
}


impl FromBytes for ListOffsetsResponse {
    fn get_from_bytes(buffer: &mut Cursor<Vec<u8>>) -> Self {
        let mut response = Self {
            throttle_time_ms: i32::read_from_buffer(buffer),
            topics: vec![]
        };
        let topics_len = i32::read_from_buffer(buffer);
        for _ in 0..topics_len {
            let mut topic = TopicResponse {
                name: KafkaString::read_from_buffer(buffer),
                partitions: vec![],
            };

            let partitions_len = i32::read_from_buffer(buffer);
            for _ in 0..partitions_len {
                let partition = PartitionResponse {
                    partition_index: i32::read_from_buffer(buffer),
                    timestamp: i64::read_from_buffer(buffer),
                    offset: i64::read_from_buffer(buffer),
                };
                topic.partitions.push(partition);
            }
            response.topics.push(topic)
        }
        response
    }
}