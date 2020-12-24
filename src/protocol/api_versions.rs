use crate::protocol::top_level::ToBytes;
use crate::protocol::header::RequestHeader;

const API_KEY: i16 = 18;
const API_VERSION: i16 = 0;
const CORRELATION_ID: i32 = 112;
const CLIENT_ID: &str = "roi";

#[derive(Debug, Clone)]
pub struct ApiVersionsRequest {
    pub header: RequestHeader,
}

impl ApiVersionsRequest {
    pub fn new() -> Self {
        let header = RequestHeader::new(API_KEY, API_VERSION, CORRELATION_ID, CLIENT_ID.to_string());
        Self { header }
    }
}

impl ToBytes for ApiVersionsRequest {
    fn get_in_bytes(&self) -> Vec<u8> {
        Vec::new()
    }

    fn header(&self) -> RequestHeader {
        RequestHeader::new(API_KEY, API_VERSION, CORRELATION_ID, CLIENT_ID.to_string())
    }
}