use crate::protocol::top_level::FromBytes;
use std::net::TcpStream;
use std::io::BufReader;
use std::fs::read;
use crate::utils::buffer_reader::BufferReader;

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

pub struct MetadataResponse {
    throttle_time_ms: i32,
    brokers: Vec<Broker>,
    cluster_id: i32,
    controller_id: String,
    topics: Vec<Topic>,
}

struct Broker {
    error_code: i16,
    name: String,
    is_internal: bool,
}

struct Topic {
    node_id: i32,
    host: String,
    port: i32,
    rack: String,
}

struct Partition {
    error_code: i16,
    partition_index: i32,
    leader_id: i32,
    replica_nodes: Vec<i32>,
    isr_nodes: Vec<i32>,
    offline_replicas: Vec<i32>,
}

impl MetadataResponse {
    pub fn new(topics: Vec<String>) -> Self {
        Self {
            throttle_time_ms: 0,
            brokers: vec![],
            cluster_id: 0,
            controller_id: "".to_string(),
            topics: vec![]
        }
    }
}

impl FromBytes for MetadataResponse {
    fn get_from_bytes(stream: &mut TcpStream) -> Vec<u8> {
        let mut reader = BufReader::new(stream);
        BufferReader::get_response_size2(&mut reader);
        reader
    }
}