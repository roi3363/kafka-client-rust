use crate::protocol::primitives::VarString;

#[derive(Debug)]
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

#[derive(Debug)]
pub struct Record {
    pub length: i32,
    pub attributes: i8,
    pub timestamp_delta: i32,
    pub offset_delta: i32,
    pub key_length: i32,
    pub key: VarString,
    pub value_length: i32,
    pub value: VarString,
    pub headers: Vec<RecordHeader>,
}


#[derive(Debug)]
pub struct RecordHeader {
    pub header_key_length: i32,
    pub header_key: VarString,
    pub header_value_length: i32,
    pub header_value: VarString,
}



