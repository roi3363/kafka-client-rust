use std::net::TcpStream;
use std::io::{Write};

use crate::protocol::api_versions::{ApiVersionsRequest, ApiVersionsResponse, ApiKey};
use crate::protocol::response::Response;

use std::collections::HashMap;
use crate::clients::connection::connect_to_server;
use crate::protocol::header::RequestHeader;
use crate::protocol::request::Request;
use crate::protocol::create_topic::{CreateTopicRequest, CreateTopicResponse};

use crate::protocol::api_keys::ApiKeys;

use crate::protocol::metadata::{MetadataRequest, MetadataResponse};
use crate::clients::kafka_client::KafkaClient;
use crate::protocol::fetch::FetchRequest;

const CORRELATION_ID: i32 = 19;
const CLIENT_ID: i32 = 1;


pub struct AdminClient {
    pub host: String,
    pub stream: TcpStream,
    pub api_versions: HashMap<i16, ApiKey>,
    pub client_id: String,
}

impl KafkaClient for AdminClient {

    fn stream(&mut self) -> &mut TcpStream {
        &mut self.stream
    }

    fn client_id(&self) -> &str {
        &self.client_id
    }
}

impl AdminClient {
    pub fn new(host: String) -> Self {
        let mut stream = connect_to_server(host.as_str());
        let api_versions = Self::api_versions(&mut stream);
        Self { host, stream, api_versions, client_id: CLIENT_ID.to_string() }
    }

    pub fn create_topic(&mut self, name: &str, num_partitions: i32, replication_factor: i16) {
        let api_key =  self.api_versions.get(&(ApiKeys::CreateTopics as i16)).unwrap();
        let header = RequestHeader::new(api_key.key, api_key.max_version, CORRELATION_ID, self.client_id.clone());
        let body = CreateTopicRequest::new(name, num_partitions, replication_factor);
        let request = Request::new(header, body);
        self.send_request(request.buffer);
        let response = Response::<CreateTopicResponse>::new(self.stream());
    }

    pub fn send_request(&mut self, buffer: Vec<u8>) {
        match self.stream.write(buffer.as_slice()) {
            Ok(_) => {}
            Err(_) => {}
        };
    }

    fn api_versions(stream: &mut TcpStream) -> HashMap<i16, ApiKey> {
        const API_VERSIONS_KEY: i16 = ApiKeys::ApiVersions as i16;
        const VERSION: i16 = 1;
        let header = RequestHeader::new(API_VERSIONS_KEY, VERSION, CORRELATION_ID, CLIENT_ID.to_string());
        let body = ApiVersionsRequest::new();
        let request = Request::new(header, body);
        match stream.write(request.buffer.as_slice()) {
            Ok(_) => {}
            Err(_) => {}
        };

        let response = Response::<ApiVersionsResponse>::new(stream);
        let mut api_keys: HashMap<i16, ApiKey> = HashMap::new();
        for api_key in response.body.api_keys {
            api_keys.insert(api_key.key, api_key);
        }
        api_keys
    }

    pub fn fetch_metadata(&mut self, topics: Vec<&str>) {
        let api_key =  self.api_versions.get(&(ApiKeys::Metadata as i16)).unwrap();
        let header = RequestHeader::new(api_key.key, api_key.max_version, CORRELATION_ID, self.client_id.clone());
        let body = MetadataRequest::new(topics);
        let request = Request::new(header, body);
        self.send_request(request.buffer);
        let response = Response::<MetadataResponse>::new(self.stream());
    }

    pub fn fetch(&mut self, topics: Vec<&str>) {
        let api_key =  self.api_versions.get(&(ApiKeys::Fetch as i16)).unwrap();
        let header = RequestHeader::new(api_key.key, api_key.max_version, CORRELATION_ID, self.client_id.clone());
        let body = FetchRequest::new(topics);
        let request = Request::new(header, body);
        self.send_request(request.buffer);
        // let response = Response::<FetchRe>::new(self.stream());
    }
}
