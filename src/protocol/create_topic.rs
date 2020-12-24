use crate::utils::buffer_writer::{BufferWriter};
use crate::protocol::top_level::{ToBytes};
use crate::protocol::header::RequestHeader;
use crate::protocol::primitives::DataTypes;
use crate::utils::buffer_reader::BufferReader;

const API_KEY: i16 = 19;
const API_VERSION: i16 = 6;
const CORRELATION_ID: i32 = 112;
const CLIENT_ID: &str = "roi";
const TIMEOUT: i32 = 5000;
const TOPICS_LENGTH: i32 = 1; // Always creates one topic at a time

/// CreateTopics Request (Version: 6) => [topics] timeout_ms validate_only TAG_BUFFER
///   topics => name num_partitions replication_factor [assignments] [configs] TAG_BUFFER
///     name => COMPACT_STRING
///     num_partitions => INT32
///     replication_factor => INT16
///     assignments => partition_index [broker_ids] TAG_BUFFER
///       partition_index => INT32
///       broker_ids => INT32
///     configs => name value TAG_BUFFER
///       name => COMPACT_STRING
///       value => COMPACT_NULLABLE_STRING
///   timeout_ms => INT32
///  validate_only => BOOLEAN
#[derive(Debug)]
pub struct CreateTopicRequest {
    pub topic: Topic,
    pub timeout_ms: i32,
    pub validate_only: bool,
}

#[derive(Debug)]
pub struct Topic {
    name: String,
    num_partitions: i32,
    replication_factor: i16,
    assignments: Vec<Assignments>,
    configs: Vec<Config>,
}

#[derive(Debug)]
pub struct Assignments {
    partition_index: i32,
    broker_ids: Vec<i32>
}

#[derive(Debug)]
pub struct Config {
    name: String,
    value: String
}

impl CreateTopicRequest {
    pub fn new(topic: &str, num_partitions: i32, replication_factor: i16) -> Self {
        let topic = Topic::new(topic.to_string(), num_partitions, replication_factor);
        Self {
            topic,
            timeout_ms: TIMEOUT,
            validate_only: false,
        }
    }
}

impl ToBytes for CreateTopicRequest {
    fn get_in_bytes(&self) -> Vec<u8> {
        let mut buffer_writer = BufferWriter::new();

        buffer_writer.write_int32(TOPICS_LENGTH);
        buffer_writer.write_str(&self.topic.name.as_str());
        buffer_writer.write_int32(self.topic.num_partitions);
        buffer_writer.write_int16(self.topic.replication_factor);
        buffer_writer.write_int32(self.topic.assignments.len() as i32);
        buffer_writer.write_int32(self.topic.configs.len() as i32);
        buffer_writer.write_int32(TIMEOUT);

        buffer_writer.buffer
    }

    fn header(&self) -> RequestHeader {
        RequestHeader::new(API_KEY, API_VERSION, CORRELATION_ID, CLIENT_ID.to_string())
    }
}

impl Topic {
    pub fn new(name: String, num_partitions: i32, replication_factor: i16) -> Self {
        let assignments = vec![Assignments{ partition_index: 0, broker_ids: vec![] }];
        let configs = vec![Config { name: "".to_string(), value: "".to_string() }];
        Self {
            name,
            num_partitions,
            replication_factor,
            assignments,
            configs,
        }
    }
}