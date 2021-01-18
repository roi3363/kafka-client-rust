use crate::protocol::request::ToBytes;
use crate::protocol::primitives::{KafkaPrimitive, KafkaString, KafkaArray};
use std::io::{Cursor};
use crate::protocol::response::FromBytes;
use crate::protocol::kafka_error_codes::check_errors;
use byteorder::{WriteBytesExt, BE};


pub struct MetadataRequest {
    topics: KafkaArray<String>,
    allow_auto_topic_creation: bool,
}

impl MetadataRequest {
    pub fn new(topics: &Vec<String>) -> Self {
        Self {
            topics: KafkaArray(topics.clone()),
            allow_auto_topic_creation: false
        }
    }
}

impl ToBytes for MetadataRequest {
    fn get_in_bytes(&self) -> Vec<u8> {
        let topics_len = self.topics.0.len() as i32;
        let mut buffer = Vec::with_capacity(topics_len as usize);
        self.topics.write_to_buffer(&mut buffer);
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
    pub throttle_time_ms: i32,
    pub brokers: Vec<BrokerMetadataResponse>,
    pub cluster_id: String,
    pub controller_id: i32,
    pub topics: Vec<TopicMetadataResponse>,
}

#[derive(Debug)]
pub struct BrokerMetadataResponse {
    pub node_id: i32,
    pub host: String,
    pub port: i32,
    rack: String,
}

#[derive(Debug)]
pub struct TopicMetadataResponse {
    pub error_code: i16,
    pub name: String,
    is_internal: bool,
    pub partitions: Vec<PartitionMetadataResponse>,
}

#[derive(Debug)]
pub struct PartitionMetadataResponse {
    error_code: i16,
    pub partition_index: i32,
    pub leader_id: i32,
    replica_nodes: Vec<i32>,
    isr_nodes: Vec<i32>,
    offline_replicas: Vec<i32>,
}

impl FromBytes for MetadataResponse {
    fn get_from_bytes(buffer: &mut Cursor<Vec<u8>>) -> Self {
        let mut response = Self {
            throttle_time_ms: i32::read_from_buffer(buffer),
            brokers: vec![],
            cluster_id: "".to_string(),
            controller_id: 0,
            topics: vec![]
        };
        let brokers_length = i32::read_from_buffer(buffer);
        for _ in 0..brokers_length {
            let broker = BrokerMetadataResponse {
                node_id: i32::read_from_buffer(buffer),
                host: KafkaString::read_from_buffer(buffer).0,
                port: i32::read_from_buffer(buffer),
                rack: KafkaString::read_from_buffer(buffer).0,
            };
            response.brokers.push(broker);
        }
        response.cluster_id =  KafkaString::read_from_buffer(buffer).0;
        response.controller_id = i32::read_from_buffer(buffer);

        let topics_length = i32::read_from_buffer(buffer);
        for _ in 0..topics_length {
            let mut topic = TopicMetadataResponse {
                error_code: i16::read_from_buffer(buffer),
                name: KafkaString::read_from_buffer(buffer).0,
                is_internal: bool::read_from_buffer(buffer),
                partitions: vec![]
            };
            check_errors(topic.error_code);

            let partitions_length = i32::read_from_buffer(buffer);
            for _ in 0..partitions_length {
                let partition = PartitionMetadataResponse {
                    error_code:  i16::read_from_buffer(buffer),
                    partition_index:  i32::read_from_buffer(buffer),
                    leader_id:  i32::read_from_buffer(buffer),
                    replica_nodes: KafkaArray::read_from_buffer(buffer).0,
                    isr_nodes: KafkaArray::read_from_buffer(buffer).0,
                    offline_replicas: KafkaArray::read_from_buffer(buffer).0,
                };

                topic.partitions.push(partition);
            }
            response.topics.push(topic);
        }
        response
    }
}
