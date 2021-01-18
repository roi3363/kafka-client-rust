use std::collections::HashMap;
use std::io::Cursor;
use std::str::{from_utf8, Utf8Error};

use crate::clients::kafka_client::PartitionMetadata;
use crate::protocol::api_keys::ApiKeys::Metadata;
use crate::protocol::kafka_error_codes::check_errors;
use crate::protocol::metadata::MetadataResponse;
use crate::protocol::primitives::{KafkaPrimitive, KafkaString, VarInt, VarString};
use crate::protocol::record::{Record, RecordBatch, RecordHeader};
use crate::protocol::request::ToBytes;
use crate::protocol::response::FromBytes;

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
    topic: KafkaString,
    partitions: Vec<PartitionFetchRequest>,
}

#[derive(Debug)]
struct PartitionFetchRequest {
    partition: i32,
    fetch_offset: i64,
    log_start_offset: i64,
    partition_max_bytes: i32,
}

#[derive(Debug)]
struct ForgottenTopics {
    topic: KafkaString,
    partitions: Vec<i32>,
}



impl FetchRequest {
    pub fn new(partitions_by_topics: HashMap<String, &Vec<i32>>) -> Self {
        let mut topic_requests = Vec::new();
        for (topic, partitions) in partitions_by_topics {
            let mut topic_request = TopicRequest {
                topic: KafkaString(topic),
                partitions: Vec::new(),
            };
            for &partition in partitions {
                let partition_request = PartitionFetchRequest {
                    partition,
                    fetch_offset: 0,
                    log_start_offset: 0,
                    partition_max_bytes: 10000
                };
                topic_request.partitions.push(partition_request);
            }
            topic_requests.push(topic_request);
        }
        Self {
            replica_id: -1,
            max_wait_ms: 5000,
            min_bytes: 100000,
            max_bytes: 1000000,
            isolation_level: 0,
            session_id: 0,
            session_epoch: 0,
            topics: topic_requests,
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
            topic.topic.write_to_buffer(&mut buffer);

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
            forgotten_topic.topic.write_to_buffer(&mut buffer);
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
    pub responses: Vec<TopicResponse>,
}

#[derive(Debug)]
pub struct TopicResponse {
    topic: KafkaString,
    pub partition_responses: Vec<PartitionResponse>,
}

#[derive(Debug)]
pub struct PartitionResponse {
    partition: i32,
    error_code: i16,
    high_watermark: i64,
    last_stable_offset: i64,
    log_start_offset: i64,
    aborted_transactions: Vec<AbortedTransactions>,
    records: Vec<RecordBatch>,
}

#[derive(Debug)]
struct AbortedTransactions {
    producer_id: i64,
    first_offset: i64,
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

    fn read_record(buffer: &mut Cursor<Vec<u8>>) -> RecordBatch {
        let mut batch = RecordBatch {
            base_offset: i64::read_from_buffer(buffer),
            batch_length: i32::read_from_buffer(buffer),
            partition_leader_epoch: i32::read_from_buffer(buffer),
            magic: i8::read_from_buffer(buffer),
            crc: i32::read_from_buffer(buffer),
            attributes: i16::read_from_buffer(buffer),
            last_offset_delta: i32::read_from_buffer(buffer),
            first_timestamp: i64::read_from_buffer(buffer),
            max_timestamp: i64::read_from_buffer(buffer),
            producer_id: i64::read_from_buffer(buffer),
            producer_epoch: i16::read_from_buffer(buffer),
            base_sequence: i32::read_from_buffer(buffer),
            records: vec![],
        };
        let _records_length = i32::read_from_buffer(buffer);

        let length = VarInt::read_from_buffer(buffer);
        let attributes = i8::read_from_buffer(buffer);
        let timestamp_delta = VarInt::read_from_buffer(buffer);
        let offset_delta = VarInt::read_from_buffer(buffer);
        let key = VarString::read_from_buffer(buffer);
        let key_length =  key.0.len() as i32;
        let value = VarString::read_from_buffer(buffer);
        let value_length = value.0.len() as i32;
        let mut record = Record {
            length: length.0,
            attributes,
            timestamp_delta: timestamp_delta.0,
            offset_delta: offset_delta.0,
            key_length,
            key,
            value_length,
            value,
            headers: vec![],
        };

        let _header_len = VarInt::read_from_buffer(buffer);
        let header_key = VarString::read_from_buffer(buffer);
        let header_key_length = header_key.0.len() as i32;
        let header_value = VarString::read_from_buffer(buffer);
        let header_value_length = header_value.0.len() as i32;
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
            throttle_time_ms: i32::read_from_buffer(buffer),
            error_code: i16::read_from_buffer(buffer),
            session_id: i32::read_from_buffer(buffer),
            responses: vec![],
        };
        check_errors(response.error_code);

        let topics_len = i32::read_from_buffer(buffer);
        for _ in 0..topics_len {
            let mut topic = TopicResponse {
                topic: KafkaString::read_from_buffer(buffer),
                partition_responses: vec![],
            };

            let partitions_len = i32::read_from_buffer(buffer);
            for _ in 0..partitions_len {
                let mut partition = PartitionResponse {
                    partition: i32::read_from_buffer(buffer),
                    error_code: i16::read_from_buffer(buffer),
                    high_watermark: i64::read_from_buffer(buffer),
                    last_stable_offset: i64::read_from_buffer(buffer),
                    log_start_offset: i64::read_from_buffer(buffer),
                    aborted_transactions: vec![],
                    records: vec![],
                };
                check_errors(partition.error_code);

                let aborted_txn_len = i32::read_from_buffer(buffer);
                for _ in 0..aborted_txn_len {
                    let aborted_txn = AbortedTransactions {
                        producer_id: i64::read_from_buffer(buffer),
                        first_offset: i64::read_from_buffer(buffer),
                    };
                    partition.aborted_transactions.push(aborted_txn);
                }
                let records_bytes_len = i32::read_from_buffer(buffer);
                let end = buffer.position() as i32 + records_bytes_len;
                while (buffer.position() as i32) < end {
                    let record = Self::read_record(buffer);
                    partition.records.push(record);
                }
                topic.partition_responses.push(partition);
            }
            response.responses.push(topic);
        }
        response
    }
}


