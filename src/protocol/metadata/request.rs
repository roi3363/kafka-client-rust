use crate::utils::buffer_writer::{BufferWriter};
use crate::protocol::header::RequestHeader;
use crate::protocol::top_level::ToBytes;

const API_KEY: i16 = 3;
const API_VERSION: i16 = 6;
const CORRELATION_ID: i32 = 112;
const CLIENT_ID: &str = "roi";


/// Metadata Request (Version: 6) => [topics] allow_auto_topic_creation
///   topics => name
///     name => STRING
///   allow_auto_topic_creation => BOOLEAN
pub struct MetadataRequest {
    pub topics: Vec<String>,
    pub allow_auto_topic_creation: bool,
}

impl MetadataRequest {
    pub fn new(topics: Vec<String>) -> Self {
        Self { topics, allow_auto_topic_creation: true }
    }
}

impl ToBytes for MetadataRequest {
    fn get_in_bytes(&self) -> Vec<u8> {
        let mut buffer_writer = BufferWriter::new();
        buffer_writer.write_int32(self.topics.len() as i32);
        for topic in self.topics.iter() {
            buffer_writer.write_str(topic.as_str());
        }
        buffer_writer.write_bool(self.allow_auto_topic_creation);
        buffer_writer.buffer
    }

    fn header(&self) -> RequestHeader {
        RequestHeader::new(API_KEY, API_VERSION, CORRELATION_ID, CLIENT_ID.to_string())
    }
}

