use crate::protocol::request::ToBytes;
use crate::protocol::primitives::KafkaPrimitive;

use serde::export::Vec;

use std::io::{Cursor};
use crate::protocol::response::FromBytes;



pub struct MetadataRequest<'a> {
    topics: Vec<&'a str>,
    allow_auto_topic_creation: bool,
}

impl <'a> MetadataRequest<'a> {
    pub fn new(topics: Vec<&'a str>) -> Self {
        Self {
            topics,
            allow_auto_topic_creation: false
        }
    }
}

impl <'a> ToBytes for MetadataRequest<'a> {
    fn get_in_bytes(&self) -> Vec<u8> {
        let mut buffer = Vec::new();
        self.topics.as_slice().write_to_buffer(&mut buffer);
        self.allow_auto_topic_creation.write_to_buffer(&mut buffer);
        buffer
    }
}
/// Metadata Response (Version: 6) => throttle_time_ms [brokers] cluster_id controller_id [topics]
///   throttle_time_ms => INT32
///   brokers => node_id host port rack
///     node_id => INT32
///     host => STRING
///     port => INT32
///     rack => NULLABLE_STRING
///   cluster_id => NULLABLE_STRING
///   controller_id => INT32
///   topics => error_code name is_internal [partitions]
///     error_code => INT16
///     name => STRING
///     is_internal => BOOLEAN
///     partitions => error_code partition_index leader_id [replica_nodes] [isr_nodes] [offline_replicas]
///       error_code => INT16
///       partition_index => INT32
///       leader_id => INT32
///       replica_nodes => INT32
///       isr_nodes => INT32
///       offline_replicas => INT32
#[derive(Debug)]
pub struct MetadataResponse {
    throttle_time_ms: i32,
    brokers: Vec<Broker>,
    cluster_id: String,
    controller_id: i32,
    topics: Vec<Topic>
}

#[derive(Debug)]
struct Broker {
    node_id: i32,
    host: String,
    port: i32,
    rack: String,
}

#[derive(Debug)]
struct Topic {
    error_code: i16,
    name: String,
    is_internal: bool,
    partitions: Vec<Partition>
}

#[derive(Debug)]
struct Partition {
    error_code: i16,
    partition_index: i32,
    leader_id: i32,
    replica_nodes: Vec<i32>,
    isr_nodes: Vec<i32>,
    offline_replicas: Vec<i32>,
}

impl FromBytes for MetadataResponse {
    fn get_from_bytes(buffer: &mut Cursor<Vec<u8>>) -> Self {
        let mut response = Self {
            throttle_time_ms: 0.read_from_buffer(buffer),
            brokers: vec![],
            cluster_id: "".to_string(),
            controller_id: 0,
            topics: vec![]
        };
        let brokers_length = (response.brokers.len() as i32).read_from_buffer(buffer);
        for _ in 0..brokers_length {
            let mut broker = Broker {
                node_id: 0.read_from_buffer(buffer),
                host: "".to_string().read_from_buffer(buffer),
                port: 0.read_from_buffer(buffer),
                rack: "".to_string().read_from_buffer(buffer),
            };
            response.brokers.push(broker);
        }
        response.cluster_id = response.cluster_id.read_from_buffer(buffer);
        response.controller_id = response.controller_id.read_from_buffer(buffer);

        let topics_length = (response.topics.len() as i32).read_from_buffer(buffer);
        let mut topics: Vec<Topic> = Vec::with_capacity(topics_length as usize);
        for _ in 0..topics_length {
            let mut topic = Topic {
                error_code: 0.read_from_buffer(buffer),
                name: "".to_string().read_from_buffer(buffer),
                is_internal: false.read_from_buffer(buffer),
                partitions: vec![]
            };

            let partitions_length = (topic.partitions.len() as i32).read_from_buffer(buffer);
            for _ in 0..partitions_length {
                let mut partition = Partition {
                    error_code: 0.read_from_buffer(buffer),
                    partition_index: 0.read_from_buffer(buffer),
                    leader_id: 0.read_from_buffer(buffer),
                    replica_nodes: vec![].read_from_buffer(buffer),
                    isr_nodes: vec![].read_from_buffer(buffer),
                    offline_replicas: vec![].read_from_buffer(buffer),
                };
                topic.partitions.push(partition);
            }
            topics.push(topic);
        }
        response.topics = topics;
        response
    }
}



impl MetadataResponse {
    fn new() -> Self {
        Self {
            throttle_time_ms: 0,
            brokers: vec![],
            cluster_id: "".to_string(),
            controller_id: 0,
            topics: vec![]
        }
    }
}

