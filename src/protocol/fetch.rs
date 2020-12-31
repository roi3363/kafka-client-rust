use crate::protocol::request::ToBytes;
use crate::protocol::primitives::{KafkaPrimitive};
use crate::protocol::response::FromBytes;
use std::io::{Cursor, Seek, SeekFrom, Read};
use std::convert::TryInto;
use byteorder::{ReadBytesExt, BE, BigEndian};
use std::str::{from_utf8, Utf8Error};
use crc::crc32;
use crate::utils::buffer_utils::BufferUtils;
use std::process::exit;

#[derive(Debug)]
pub struct FetchRequest {
    replica_id: i32,
    max_wait_ms: i32,
    min_bytes: i32,
    max_bytes: i32,
    isolation_level: i8,
    session_id: i32,
    session_epoch: i32,
    topics: Vec<TopicRequest>,
    forgotten_topics_data: Vec<ForgottenTopics>,
}

#[derive(Debug)]
struct TopicRequest {
    topic: String,
    partitions: Vec<Partition>,
}

#[derive(Debug)]
struct Partition {
    partition: i32,
    fetch_offset: i64,
    log_start_offset: i64,
    partition_max_bytes: i32,
}

#[derive(Debug)]
struct ForgottenTopics {
    topic: String,
    partitions: Vec<i32>,
}

impl FetchRequest {
    pub fn new(topics: Vec<&str>) -> Self {
        let partition = Partition {
            partition: 0,
            fetch_offset: 27,
            log_start_offset: 0,
            partition_max_bytes: 20000,
        };
        let topic = TopicRequest {
            topic: topics.get(0).unwrap().to_string(),
            partitions: vec![partition],
        };
        Self {
            replica_id: -1,
            max_wait_ms: 5000,
            min_bytes: 10,
            max_bytes: 1000000,
            isolation_level: 0,
            session_id: 0,
            session_epoch: 0,
            topics: vec![topic],
            forgotten_topics_data: vec![],
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


#[derive(Debug)]
pub struct FetchResponse {
    throttle_time_ms: i32,
    error_code: i16,
    session_id: i32,
    responses: Vec<TopicResponse>,
}

#[derive(Debug)]
struct TopicResponse {
    topic: String,
    partition_responses: Vec<PartitionResponse>,
}

#[derive(Debug)]
struct PartitionResponse {
    partition: i32,
    error_code: i16,
    high_watermark: i64,
    last_stable_offset: i64,
    log_start_offset: i64,
    aborted_transactions: Vec<AbortedTransactions>,
    record_set: Vec<RecordBatch>,
}

#[derive(Debug)]
struct AbortedTransactions {
    producer_id: i64,
    first_offset: i64,
}

#[derive(Debug)]
struct RecordBatch {
    base_offset: i64,
    batch_length: i32,
    partition_leader_epoch: i32,
    magic: i8,
    crc: i32,
    attributes: i16,
    last_offset_delta: i32,
    first_timestamp: i64,
    max_timestamp: i64,
    producer_id: i64,
    producer_epoch: i16,
    base_sequence: i32,
    records: Vec<Record>,
}

#[derive(Debug)]
struct Record {
    length: i32,
    attributes: i8,
    timestamp_delta: i32,
    offset_delta: i32,
    key_length: i32,
    key: String,
    value_len: i32,
    value: String,
    headers: Vec<RecordHeader>,
}


#[derive(Debug)]
struct RecordHeader {
    header_key_length: i32,
    header_key: String,
    header_value_length: i32,
    header_value: String,
}


impl FetchResponse {
    pub fn new() -> Self {
        Self {
            throttle_time_ms: 0,
            error_code: 0,
            session_id: 0,
            responses: vec![],
        }
    }
}

/// Fetch Response (Version: 8) => throttle_time_ms error_code session_id [responses]
///   throttle_time_ms => INT32
///   error_code => INT16
///   session_id => INT32
///   responses => topic [partition_responses]
///     topic => STRING
///     partition_responses => partition error_code high_watermark last_stable_offset log_start_offset [aborted_transactions] record_set
///       partition => INT32
///       error_code => INT16
///       high_watermark => INT64
///       last_stable_offset => INT64
///       log_start_offset => INT64
///       aborted_transactions => producer_id first_offset
///         producer_id => INT64
///         first_offset => INT64
///       record_set => RECORDS
impl FromBytes for FetchResponse {
    fn get_from_bytes(buffer: &mut Cursor<Vec<u8>>) -> Self {
        let mut response = Self {
            throttle_time_ms: 0.read_from_buffer(buffer),
            error_code: 0.read_from_buffer(buffer),
            session_id: 0.read_from_buffer(buffer),
            responses: vec![],
        };

        let topic_responses_len = 0_i32.read_from_buffer(buffer);
        for _ in 0..topic_responses_len {
            let mut topic_response = TopicResponse {
                topic: "".to_string().read_from_buffer(buffer),
                partition_responses: vec![],
            };

            let partition_response_len = 0_i32.read_from_buffer(buffer);
            for _ in 0..partition_response_len {
                let mut partition_response = PartitionResponse {
                    partition: 0.read_from_buffer(buffer),
                    error_code: 0.read_from_buffer(buffer),
                    high_watermark: 0.read_from_buffer(buffer),
                    last_stable_offset: 0.read_from_buffer(buffer),
                    log_start_offset: 0.read_from_buffer(buffer),
                    aborted_transactions: vec![],
                    record_set: vec![],
                };

                let aborted_txn_len = 0_i32.read_from_buffer(buffer);
                for _ in 0..aborted_txn_len {
                    let aborted_txn = AbortedTransactions {
                        producer_id: 0.read_from_buffer(buffer),
                        first_offset: 0.read_from_buffer(buffer),
                    };
                    partition_response.aborted_transactions.push(aborted_txn);
                }
                let records_bytes_len = 0i32.read_from_buffer(buffer);
                let end = buffer.position() as i32 + records_bytes_len;
                while (buffer.position() as i32) < end {
                    let record = read_record(buffer);
                    partition_response.record_set.push(record);
                }
                topic_response.partition_responses.push(partition_response);
            }
            response.responses.push(topic_response);
        }
        response
    }
}


fn read_record(buffer: &mut Cursor<Vec<u8>>) -> RecordBatch {
    let mut batch = RecordBatch {
        base_offset: 0.read_from_buffer(buffer),
        batch_length: 0.read_from_buffer(buffer),
        partition_leader_epoch: 0.read_from_buffer(buffer),
        magic: 0.read_from_buffer(buffer),
        crc: 0.read_from_buffer(buffer),
        attributes: 0.read_from_buffer(buffer),
        last_offset_delta: 0.read_from_buffer(buffer),
        first_timestamp: 0.read_from_buffer(buffer),
        max_timestamp: 0.read_from_buffer(buffer),
        producer_id: 0.read_from_buffer(buffer),
        producer_epoch: 0.read_from_buffer(buffer),
        base_sequence: 0.read_from_buffer(buffer),
        records: vec![],
    };
    let _records_length = 0_i32.read_from_buffer(buffer);

    let length = BufferUtils::read_varint(buffer);
    let attributes = 0_i8.read_from_buffer(buffer);
    let timestamp_delta = BufferUtils::read_varint(buffer);
    let offset_delta = BufferUtils::read_varint(buffer);
    let key_length = BufferUtils::read_varint(buffer);
    let key = BufferUtils::read_string(key_length, buffer);
    let value_len = BufferUtils::read_varint(buffer);
    let value = BufferUtils::read_string(value_len, buffer);
    let mut record = Record {
        length,
        attributes,
        timestamp_delta,
        offset_delta,
        key_length,
        key,
        value_len,
        value,
        headers: vec![],
    };

    let _header_len = BufferUtils::read_varint(buffer);
    let header_key_length = BufferUtils::read_varint(buffer);
    let header_key = BufferUtils::read_string(header_key_length, buffer);
    let header_value_length = BufferUtils::read_varint(buffer);
    let header_value = BufferUtils::read_string(header_value_length, buffer);
    let header = RecordHeader {
        header_key_length,
        header_key,
        header_value_length,
        header_value,
    };
    record.headers.push(header);
    batch.records.push(record);
    batch
}