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
        let mut response = Self::new();
        response.throttle_time_ms = response.throttle_time_ms.read_from_buffer(buffer);
        let brokers_length = (response.brokers.len() as i32).read_from_buffer(buffer);
        let mut brokers: Vec<Broker> = Vec::with_capacity(brokers_length as usize);
        for _ in 0..brokers_length {
            let mut broker = Broker {
                node_id: 0,
                host: "".to_string(),
                port: 0,
                rack: "".to_string()
            };
            broker.node_id = broker.node_id.read_from_buffer(buffer);
            broker.host = broker.host.read_from_buffer(buffer);
            broker.port = broker.port.read_from_buffer(buffer);
            broker.rack = broker.rack.read_from_buffer(buffer);
            brokers.push(broker);
        }
        response.brokers = brokers;
        response.cluster_id = response.cluster_id.read_from_buffer(buffer);
        response.controller_id = response.controller_id.read_from_buffer(buffer);

        let topics_length = (response.topics.len() as i32).read_from_buffer(buffer);
        let mut topics: Vec<Topic> = Vec::with_capacity(topics_length as usize);
        for _ in 0..topics_length {
            let mut topic = Topic {
                error_code: 0,
                name: "".to_string(),
                is_internal: false,
                partitions: vec![]
            };
            topic.error_code = topic.error_code.read_from_buffer(buffer);
            topic.name = topic.name.read_from_buffer(buffer);
            topic.is_internal = topic.is_internal.read_from_buffer(buffer);

            let partitions_length = (topic.partitions.len() as i32).read_from_buffer(buffer);
            let mut partitions: Vec<Partition> = Vec::with_capacity(partitions_length as usize);
            for _ in 0..partitions_length {
                let mut partition = Partition {
                    error_code: 0,
                    partition_index: 0,
                    leader_id: 0,
                    replica_nodes: vec![],
                    isr_nodes: vec![],
                    offline_replicas: vec![]
                };
                partition.error_code = partition.error_code.read_from_buffer(buffer);
                partition.partition_index = partition.partition_index.read_from_buffer(buffer);
                partition.leader_id = partition.leader_id.read_from_buffer(buffer);
                partition.replica_nodes = partition.replica_nodes.read_from_buffer(buffer);
                
                partition.isr_nodes = partition.isr_nodes.read_from_buffer(buffer);
                partition.offline_replicas = partition.offline_replicas.read_from_buffer(buffer);
                partitions.push(partition);
            }
            topic.partitions = partitions;
            topics.push(topic);
        }
        response.topics = topics;
        println!("{:#?}", response);
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

