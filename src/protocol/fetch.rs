use crate::utils::buffer_writer::{BufferWriter};
use crate::protocol::top_level::ToBytes;
use crate::protocol::header::RequestHeader;

const API_KEY: i16 = 1;
const API_VERSION: i16 = 8;
const CORRELATION_ID: i32 = 112;
const CLIENT_ID: &str = "roi";
const TIMEOUT: i32 = 5000;
const REPLICA_ID: i32 = 0;
const MAX_WAIT_TIME: i32 = 4;
const MIN_BYTES: i32 = 100;
const MAX_BYTES: i32 = 10000000;
const ARRAY_LENGTH_TOPICS: i32 = 1;
const ARRAY_LENGTH_PARTITIONS: i32 = 3;
const PARTITION: i32 = 0;
const FETCH_OFFSET: i64 = 1;
const PARTITION_MAX_BYTES: i32 = 1000000;




#[derive(Debug)]
pub struct FetchRequest {
    replica_id: i32,
    max_wait_time: i32,
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
    name: String,
    partitions: Partition,
}

#[derive(Debug)]
pub struct ForgottenTopicsData {
    name: String,
    partitions: Vec<i32>,
}

#[derive(Debug)]
pub struct Partition {
    partition: i32,
    fetch_offset: i64,
    partition_max_bytes: i32,
}

impl FetchRequest {
    pub fn new(topics: Vec<&str>) -> Self {
        Self {
            replica_id: 1,
            max_wait_time: 1000,
            min_bytes: 1,
            max_bytes: 0,
            isolation_level: 0,
            session_id: 0,
            session_epoch: 0,
            topics: vec![],
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
        let mut buffer_writer = BufferWriter::new();
        buffer_writer.write_int32(self.replica_id);
        buffer_writer.write_int32(self.max_wait_time);
        buffer_writer.write_int32(self.min_bytes);
        buffer_writer.write_int32(self.max_bytes);
        buffer_writer.write_int8(self.isolation_level);
        buffer_writer.write_int32(self.session_id);
        buffer_writer.write_int32(self.session_epoch);

        let topics_len = self.topics.len() as i32;
        buffer_writer.write_int32(topics_len);
        // for topic in self.topics.iter() {
        //     buffer_writer.write_str(topic.)
        // }

        buffer_writer.buffer
    }

    fn header(&self) -> RequestHeader {
        RequestHeader::new(API_KEY, API_VERSION, CORRELATION_ID, CLIENT_ID.to_string())
    }
}
