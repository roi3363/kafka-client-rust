
use serde::export::Vec;
use crate::protocol::primitives::{KafkaPrimitive};
use crate::protocol::request::{ToBytes};
use crate::protocol::response::FromBytes;
use std::io::Cursor;


const TIMEOUT: i32 = 5000;
const TOPICS_LENGTH: i32 = 1; // Always creates one topic at a time


/// CreateTopics Request (Version: 3) => [topics] timeout_ms validate_only
///   topics => name num_partitions replication_factor [assignments] [configs]
///     name => STRING
///     num_partitions => INT32
///     replication_factor => INT16
///     assignments => partition_index [broker_ids]
///       partition_index => INT32
///       broker_ids => INT32
///     configs => name value
///       name => STRING
///       value => NULLABLE_STRING
///   timeout_ms => INT32
///   validate_only => BOOLEAN
#[derive(Debug)]
pub struct CreateTopicRequest {
    topic: TopicRequest,
    timeout_ms: i32,
    validate_only: bool,
}

#[derive(Debug)]
struct TopicRequest {
    name: String,
    num_partitions: i32,
    replication_factor: i16,
    assignments: Vec<Assignment>,
    configs: Vec<Config>,
}



impl CreateTopicRequest {
    pub fn new(topic: &str, num_partitions: i32, replication_factor: i16) -> Self {
        let topic = TopicRequest::new(topic.to_string(), num_partitions, replication_factor);
        Self {
            topic,
            timeout_ms: TIMEOUT,
            validate_only: false,
        }
    }
}

impl ToBytes for CreateTopicRequest {
    fn get_in_bytes(&self) -> Vec<u8> {
        let mut buffer = Vec::new();
        let assignments_len = self.topic.assignments.len() as i32;
        let configs_len = self.topic.configs.len() as i32;

        TOPICS_LENGTH.write_to_buffer(&mut buffer);
        self.topic.name.as_str().write_to_buffer(&mut buffer);
        self.topic.num_partitions.write_to_buffer(&mut buffer);
        self.topic.replication_factor.write_to_buffer(&mut buffer);

        assignments_len.write_to_buffer(&mut buffer);
        for assignment in  self.topic.assignments.iter() {
            assignment.partition_index.write_to_buffer(&mut buffer);
            assignment.broker_ids.clone().write_to_buffer(&mut buffer);
        }
        configs_len.write_to_buffer(&mut buffer);
        for config in  self.topic.configs.iter() {
            config.name.as_str().write_to_buffer(&mut buffer);
            config.value.as_str().write_to_buffer(&mut buffer);
        }
        TIMEOUT.write_to_buffer(&mut buffer);
        self.validate_only.write_to_buffer(&mut buffer);
        buffer
    }
}



#[derive(Debug)]
struct Assignment {
    partition_index: i32,
    broker_ids: Vec<i32>,
}

#[derive(Debug)]
struct Config {
    name: String,
    value: String
}

impl TopicRequest {
    pub fn new(name: String, num_partitions: i32, replication_factor: i16) -> Self {
        let assignments = vec![];
        let configs = vec![];
        Self {
            name,
            num_partitions,
            replication_factor,
            assignments,
            configs,
        }
    }
}
/// CreateTopics Response (Version: 3) => throttle_time_ms [topics]
///   throttle_time_ms => INT32
///   topics => name error_code error_message
///     name => STRING
///     error_code => INT16
///     error_message => NULLABLE_STRING

#[derive(Debug)]
pub struct CreateTopicResponse {
    throttle_time_ms: i32,
    topics: Vec<TopicRequest>,
}

struct TopicResponse {
    name: String,
    error_code: i16,
    error_message: String,
}

impl CreateTopicResponse {
    fn new() -> Self {
        Self {
            throttle_time_ms: 0,
            topics: vec![],
        }
    }
}

impl FromBytes for CreateTopicResponse {
    fn get_from_bytes(buffer: &mut Cursor<Vec<u8>>) -> Self {
        let mut response = Self {
            throttle_time_ms: 0.read_from_buffer(buffer),
            topics: vec![]
        };
        let topics_length = (response.topics.len() as i32).read_from_buffer(buffer);
        let mut topics: Vec<TopicResponse> = Vec::with_capacity(topics_length as usize);
        for _ in 0..topics_length {
            let mut topic = TopicResponse {
                name: "".to_string().read_from_buffer(buffer),
                error_code: 0.read_from_buffer(buffer),
                error_message: "".to_string().read_from_buffer(buffer),
            };
            topics.push(topic);
        }
        response
    }
}