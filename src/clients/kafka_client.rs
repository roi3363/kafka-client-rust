use std::net::TcpStream;

use crate::protocol::header::RequestHeader;
use crate::protocol::request::Request;
use std::collections::HashMap;
use crate::protocol::api_versions::{ApiKey, ApiVersionsRequest, ApiVersionsResponse};
use std::io::Write;
use crate::protocol::api_keys::ApiKeys;
use crate::protocol::response::Response;



pub trait KafkaClient {

    fn stream(&mut self) -> &mut TcpStream;
    fn client_id(&self) -> &str;

    fn send_request(&mut self, buffer: Vec<u8>) {
        match self.stream().write(buffer.as_slice()) {
            Ok(_) => {}
            Err(_) => {}
        };
    }

    fn api_versions(&mut self) -> HashMap<i16, ApiKey> {
        let api_versions_key: i16 = ApiKeys::ApiVersions as i16;
        let api_versions_version: i16 = 1 as i16;
        let correlation_id: i32 = 19;
        let header = RequestHeader::new(api_versions_key, api_versions_version, correlation_id, self.client_id().to_string());
        let body = ApiVersionsRequest::new();
        let request = Request::new(header, body);
        self.stream().write(request.buffer.as_slice()).unwrap();
        let response = Response::<ApiVersionsResponse>::new(self.stream());
        let mut api_keys: HashMap<i16, ApiKey> = HashMap::new();
        for api_key in response.body.api_keys {
            api_keys.insert(api_key.key, api_key);
        }
        api_keys
    }
}
