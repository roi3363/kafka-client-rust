use crate::protocol::primitives::{KafkaPrimitive, VarInt, KafkaValue};
use crate::protocol::request::ToBytes;
use crate::protocol::response::FromBytes;
use std::io::Cursor;
use serde::{Serialize, Deserialize};
///
///  baseOffset: int64
///  batchLength: int32
///  partitionLeaderEpoch: int32
///  magic: int8 (current magic value is 2)
///  crc: int32
///  attributes: int16
///  	bit 0~2:
///  		0: no compression
///  		1: gzip
///  		2: snappy
///  		3: lz4
///  		4: zstd
///  	bit 3: timestampType
///  	bit 4: isTransactional (0 means not transactional)
///  	bit 5: isControlBatch (0 means not a control batch)
///  	bit 6~15: unused
///  lastOffsetDelta: int32
///  firstTimestamp: int64
///  maxTimestamp: int64
///  producerId: int64
///  producerEpoch: int16
///  baseSequence: int32
///  records: [Record]
#[derive(Debug, Serialize)]
pub struct RecordBatch {
    pub base_offset: i64,
    pub batch_length: i32,
    pub partition_leader_epoch: i32,
    pub magic: i8,
    pub crc: i32,
    pub attributes: i16,
    pub last_offset_delta: i32,
    pub first_timestamp: i64,
    pub max_timestamp: i64,
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub base_sequence: i32,
    pub records: Vec<Record>,
}

impl RecordBatch {
    pub fn new(records: Vec<Record>) -> Self {
        let mut batch = Self {
            base_offset: 1,
            batch_length: 0,
            partition_leader_epoch: -1,
            magic: 2,
            crc: 0,
            attributes: 0,
            last_offset_delta: 0,
            first_timestamp: 0,
            max_timestamp: -1,
            producer_id: -1,
            producer_epoch: 1,
            base_sequence: -1,
            records
        };
        // batch.batch_length = bincode::serialize(&batch).unwrap().len() as i32;
        println!("{:#?}", bincode::serialize(&batch).unwrap().len() as i32);
        batch.batch_length = 100 as i32;
        batch
    }
}

/// length: varint
/// 	attributes: int8
/// 		bit 0~7: unused
/// 	timestampDelta: varint
/// 	offsetDelta: varint
/// 	keyLength: varint
/// 	key: byte[]
/// 	valueLen: varint
/// 	value: byte[]
/// 	Headers => [Header]
#[derive(Debug, Serialize)]
pub struct Record {
    pub length: VarInt,
    pub attributes: i8,
    pub timestamp_delta: VarInt,
    pub offset_delta: VarInt,
    pub key_length: VarInt,
    pub key: KafkaValue,
    pub value_length: VarInt,
    pub value: KafkaValue,
    pub headers: Vec<RecordHeader>,
}

impl Record {
    pub fn new(key: Vec<u8>, value: Vec<u8>) -> Self {
        let mut record = Self {
            length: VarInt(10),
            attributes: 0,
            timestamp_delta: VarInt(0),
            offset_delta: VarInt(1),
            key_length: VarInt(key.len() as i32),
            key: KafkaValue(key),
            value_length: VarInt(value.len() as i32),
            value: KafkaValue(value),
            headers: RecordHeader::new()
        };
        // record.length = VarInt(bincode::serialize(&record).unwrap().len() as i32);
        // record.length = VarInt(10 as i32);
        record
    }
}

/// headerKeyLength: varint
/// 		headerKey: String
/// 		headerValueLength: varint
/// 		Value: byte[]
#[derive(Debug, Serialize)]
pub struct RecordHeader {
    pub header_key_length: VarInt,
    pub header_key: KafkaValue,
    pub header_value_length: VarInt,
    pub header_value: KafkaValue,
}

impl RecordHeader {
    pub fn new() -> Vec<RecordHeader> {
        let header_key = KafkaValue("app".as_bytes().to_vec());
        let header_value = KafkaValue("rust".as_bytes().to_vec());
        let header = Self {
            header_key_length: VarInt(header_key.0.len() as i32),
            header_key,
            header_value_length: VarInt(header_value.0.len() as i32),
            header_value,
        };
        vec![header]
    }
}

impl ToBytes for RecordBatch {
    fn get_in_bytes(&self) -> Vec<u8> {
        let mut buffer = Vec::new();
        self.base_offset.write_to_buffer(&mut buffer);
        self.batch_length.write_to_buffer(&mut buffer);
        self.partition_leader_epoch.write_to_buffer(&mut buffer);
        self.magic.write_to_buffer(&mut buffer);
        self.crc.write_to_buffer(&mut buffer);
        self.attributes.write_to_buffer(&mut buffer);
        self.last_offset_delta.write_to_buffer(&mut buffer);
        self.first_timestamp.write_to_buffer(&mut buffer);
        self.max_timestamp.write_to_buffer(&mut buffer);
        self.producer_id.write_to_buffer(&mut buffer);
        self.producer_epoch.write_to_buffer(&mut buffer);
        self.base_sequence.write_to_buffer(&mut buffer);

        let records_len = VarInt(self.records.len() as i32);
        // let records_len = self.records.len() as i32;
        records_len.write_to_buffer(&mut buffer);
        let mut records_buffer = Vec::new();
        for record in self.records.iter() {
            record.length.write_to_buffer(&mut records_buffer);
            record.attributes.write_to_buffer(&mut records_buffer);
            record.timestamp_delta.write_to_buffer(&mut records_buffer);
            record.offset_delta.write_to_buffer(&mut records_buffer);
            record.key_length.write_to_buffer(&mut records_buffer);
            record.key.write_to_buffer(&mut records_buffer);
            record.value_length.write_to_buffer(&mut records_buffer);
            record.value.write_to_buffer(&mut records_buffer);

            let headers_len = record.headers.len() as i32;
            headers_len.write_to_buffer(&mut records_buffer);
            for header in record.headers.iter() {
                header.header_key_length.write_to_buffer(&mut records_buffer);
                header.header_key.write_to_buffer(&mut records_buffer);
                header.header_value_length.write_to_buffer(&mut records_buffer);
                header.header_value.write_to_buffer(&mut records_buffer);
            }
        }
        (records_buffer.len() as i32).write_to_buffer(&mut buffer);
        records_buffer.write_to_buffer(&mut buffer);
        buffer
    }
}

impl FromBytes for RecordBatch {
    fn get_from_bytes(buffer: &mut Cursor<Vec<u8>>) -> Self {
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
        let key = KafkaValue::read_from_buffer(buffer);
        let key_length = VarInt(key.0.len() as i32);
        let value = KafkaValue::read_from_buffer(buffer);
        let value_length = VarInt(value.0.len() as i32);
        let mut record = Record {
            length: VarInt(length.0),
            attributes,
            timestamp_delta: VarInt(timestamp_delta.0),
            offset_delta: VarInt(offset_delta.0),
            key_length,
            key,
            value_length,
            value,
            headers: vec![],
        };

        let header_len = VarInt::read_from_buffer(buffer);
        if header_len.0 != 0 {
            let header_key = KafkaValue::read_from_buffer(buffer);
            let header_key_length = VarInt(header_key.0.len() as i32);
            let header_value = KafkaValue::read_from_buffer(buffer);
            let header_value_length = VarInt(header_value.0.len() as i32);
            let header = RecordHeader {
                header_key_length,
                header_key,
                header_value_length,
                header_value,
            };
            record.headers.push(header);
        }
        batch.records.push(record);
        println!("{:#?}", batch);
        batch
    }
}


