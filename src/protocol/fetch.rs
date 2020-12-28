use crate::protocol::request::ToBytes;
use crate::protocol::primitives::KafkaPrimitive;

#[derive(Debug)]
pub struct FetchRequest {
    replica_id: i32,
    max_wait_ms: i32,
    min_bytes: i32,
    max_bytes: i32,
    isolation_level: i8,
    session_id: i32,
    session_epoch: i32,
    topics: Vec<Topic>,
    forgotten_topics_data: Vec<ForgottenTopicsData>,
}

#[derive(Debug)]
pub struct Topic {
    topic: String,
    partitions: Vec<Partition>,
}

#[derive(Debug)]
pub struct ForgottenTopicsData {
    topic: String,
    partitions: Vec<i32>,
}

#[derive(Debug)]
pub struct Partition {
    partition: i32,
    fetch_offset: i64,
    log_start_offset: i64,
    partition_max_bytes: i32,
}

impl FetchRequest {
    pub fn new(topics: Vec<&str>) -> Self {
        let partition = Partition {
            partition: 0,
            fetch_offset: 0,
            log_start_offset: 0,
            partition_max_bytes: 0
        };
        let topic = Topic {
            topic: topics.get(0).unwrap().to_string(),
            partitions: vec![partition]
        };
        Self {
            replica_id: 0,
            max_wait_ms: 5000,
            min_bytes: i32::min_value(),
            max_bytes: i32::max_value(),
            isolation_level: 0,
            session_id: 0,
            session_epoch: 0,
            topics: vec![topic],
            forgotten_topics_data: vec![]
        }
    }
}
/// Fetch Request (Version: 8) => replica_id max_wait_ms min_bytes max_bytes isolation_level session_id session_epoch [topics] [ForgottenTopicsData]
///   replica_id => INT32
///   max_wait_ms => INT32
///   min_bytes => INT32
///   max_bytes => INT32
///   isolation_level => INT8
///   session_id => INT32
///   session_epoch => INT32
///   topics => topic [partitions]
///     topic => STRING
///     partitions => partition fetch_offset log_start_offset partition_max_bytes
///       partition => INT32
///       fetch_offset => INT64
///       log_start_offset => INT64
///       partition_max_bytes => INT32
///   ForgottenTopicsData => topic [partitions]
///     topic => STRING
///     partitions => INT32
impl ToBytes for FetchRequest {
    fn get_in_bytes(&self) -> Vec<u8> {
        let mut buffer = Vec::new();
        self.replica_id.write_to_buffer(&mut buffer);
        self.max_wait_ms.write_to_buffer(&mut buffer);
        self.min_bytes.write_to_buffer(&mut buffer);
        self.max_bytes.write_to_buffer(&mut buffer);
        self.isolation_level.write_to_buffer(&mut buffer);
        self.session_id.write_to_buffer(&mut buffer);
        self.session_epoch.write_to_buffer(&mut buffer);

        let topics_len = self.topics.len() as i32;
        topics_len.write_to_buffer(&mut buffer);
        for topic in self.topics.iter() {
            topic.topic.as_str().write_to_buffer(&mut buffer);

            let partitions_len = topic.partitions.len() as i32;
            partitions_len.write_to_buffer(&mut buffer);
            for partition in topic.partitions.iter() {
                partition.partition.write_to_buffer(&mut buffer);
                partition.fetch_offset.write_to_buffer(&mut buffer);
                partition.log_start_offset.write_to_buffer(&mut buffer);
                partition.partition_max_bytes.write_to_buffer(&mut buffer);
            }
        }

        let forgotten_topics_len = self.forgotten_topics_data.len() as i32;
        forgotten_topics_len.write_to_buffer(&mut buffer);
        for forgotten_topic in self.forgotten_topics_data.iter() {
            forgotten_topic.topic.as_str().write_to_buffer(&mut buffer);
            let partitions_len = forgotten_topic.partitions.len() as i32;
            partitions_len.write_to_buffer(&mut buffer);
            for partition in forgotten_topic.partitions.iter() {
                partition.write_to_buffer(&mut buffer);
            }
        }
        buffer
    }
}
