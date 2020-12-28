use std::io::{Cursor};
use crate::protocol::request::{ToBytes};
use crate::protocol::response::{FromBytes};
use crate::protocol::primitives::KafkaPrimitive;

// -------- Request --------

#[derive(Debug, Clone)]
pub struct ApiVersionsRequest {}

impl ApiVersionsRequest {
    pub fn new() -> Self {
        Self { }
    }
}

impl ToBytes for ApiVersionsRequest {
    fn get_in_bytes(&self) -> Vec<u8> {
        Vec::new()
    }
}

// -------- Response -------

#[derive(Debug, Clone)]
pub struct ApiVersionsResponse {
    pub error_code: i16,
    pub api_keys: Vec<ApiKey>,
    pub throttle_time_ms: i32,
}

#[derive(Debug, Copy, Clone)]
pub struct ApiKey {
    pub key: i16,
    pub min_version: i16,
    pub max_version: i16,
}

impl ApiVersionsResponse {
    fn new() -> Self {
        Self {
            error_code: 0,
            api_keys: vec![],
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
        response.error_code = response.error_code.read_from_buffer(buffer);

        let api_keys_len = (response.api_keys.len() as i32).read_from_buffer(buffer);
        let mut api_keys: Vec<ApiKey> = Vec::with_capacity(api_keys_len as usize);
        for _ in 0..api_keys_len {
            let mut api_key = ApiKey {
                key: 0,
                min_version: 0,
                max_version: 0
            };
            api_key.key =  api_key.key.read_from_buffer(buffer);
            api_key.min_version =  api_key.min_version.read_from_buffer(buffer);
            api_key.max_version =  api_key.max_version.read_from_buffer(buffer);
            api_keys.push(api_key);
        }
        response.api_keys = api_keys;
        response.throttle_time_ms = response.throttle_time_ms.read_from_buffer(buffer);
        response
    }
}

