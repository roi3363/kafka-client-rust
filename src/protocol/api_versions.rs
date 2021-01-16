use std::io::{Cursor, Write};
use crate::protocol::request::{ToBytes, Request};
use crate::protocol::response::{FromBytes, Response};
use crate::protocol::primitives::{KafkaPrimitive, KafkaString};
use std::collections::HashMap;
use std::net::TcpStream;
use crate::protocol::api_keys::ApiKeys;
use crate::protocol::header::RequestHeader;
use crate::clients::kafka_client::KafkaClient;

const API_VERSIONS_VERSION: i16 = 1;
const CLIENT_ID: &str = "api-versions-fetcher";

#[derive(Debug, Copy, Clone)]
pub struct ApiVersion {
    pub api_key: i16,
    pub min_version: i16,
    pub max_version: i16,
}


pub fn api_versions(stream: &mut TcpStream, correlation_id: i32) -> HashMap<i16, ApiVersion> {
    let api_versions_key = ApiKeys::ApiVersions as i16;
    let header = RequestHeader::new(api_versions_key, API_VERSIONS_VERSION,
                                    correlation_id, CLIENT_ID.to_string());

    let body = ApiVersionsRequest {};
    let request = Request::new(header, body);
    stream.write(request.buffer.as_slice()).unwrap();
    let response = Response::<ApiVersionsResponse>::build(stream);
    let mut api_keys: HashMap<i16, ApiVersion> = HashMap::new();
    for api_version in response.body.api_versions {
        api_keys.insert(api_version.api_key, api_version);
    }
    api_keys
}

// -------- Request --------

#[derive(Debug, Clone)]
pub struct ApiVersionsRequest {}

impl ToBytes for ApiVersionsRequest {
    fn get_in_bytes(&self) -> Vec<u8> {
        Vec::new()
    }
}

// -------- Response -------

#[derive(Debug, Clone)]
pub struct ApiVersionsResponse {
    pub error_code: i16,
    pub api_versions: Vec<ApiVersion>,
    pub throttle_time_ms: i32,
}


impl ApiVersionsResponse {
    fn new() -> Self {
        Self {
            error_code: 0,
            api_versions: vec![],
            throttle_time_ms: 0,
        }
    }
}

/// ApiVersions Response (Version: 2) => error_code [api_keys] throttle_time_ms
///   error_code => INT16
///   api_keys => api_key min_version max_version
///     api_key => INT16
///     min_version => INT16
///     max_version => INT16
///   throttle_time_ms => INT32

impl FromBytes for ApiVersionsResponse {
    fn get_from_bytes(buffer: &mut Cursor<Vec<u8>>) -> Self {
        let mut response = Self::new();
        response.error_code = i16::read_from_buffer(buffer);

        let api_keys_len = i32::read_from_buffer(buffer);
        let mut api_keys: Vec<ApiVersion> = Vec::with_capacity(api_keys_len as usize);
        for _ in 0..api_keys_len {
            let mut api_key = ApiVersion {
                api_key: 0,
                min_version: 0,
                max_version: 0,
            };
            api_key.api_key = i16::read_from_buffer(buffer);
            api_key.min_version = i16::read_from_buffer(buffer);
            api_key.max_version = i16::read_from_buffer(buffer);
            api_keys.push(api_key);
        }
        response.api_versions = api_keys;
        response.throttle_time_ms = i32::read_from_buffer(buffer);
        response
    }
}

