use std::collections::HashMap;
use std::io::Write;
use std::net::TcpStream;
use std::process::exit;

use crate::protocol::api_keys::ApiKeys;
use crate::protocol::api_versions::{api_versions, ApiVersion};
use crate::protocol::header::RequestHeader;
use crate::protocol::metadata::{MetadataRequest, MetadataResponse};
use crate::protocol::request::{Request, ToBytes};
use crate::protocol::response::{FromBytes, Response};
use std::borrow::{BorrowMut, Borrow};
use std::slice::Iter;
use std::iter::Cycle;
use std::vec::IntoIter;


#[derive(Debug)]
pub struct PartitionMetadata {
    pub leader_id: i32,
    pub partitions: Vec<i32>,
}

#[derive(Debug)]
pub struct KafkaClient {
    client_id: String,
    pub api_versions: HashMap<i16, ApiVersion>,
    hosts: Vec<String>,
    pub brokers: HashMap<i32, String>,
    next_broker: Cycle<IntoIter<i32>>,
    pub topics_metadata: HashMap<String, HashMap<i32, Vec<i32>>>,
    correlation_id: i32,
}

impl KafkaClient {
    pub fn new(hosts: &Vec<&str>, client_id: String) -> Self {
        let mut kafka_client = Self {
            client_id,
            api_versions: HashMap::new(),
            brokers: HashMap::new(),
            hosts: hosts.iter().map(|x| x.to_string()).collect(),
            topics_metadata: HashMap::new(),
            correlation_id: 1,
            next_broker: Vec::new().into_iter().cycle(),
        };
        kafka_client.connect();
        let brokers: Vec<i32> = kafka_client.brokers.keys().cloned().collect();
        kafka_client.next_broker = brokers.into_iter().cycle();
        let mut broker = kafka_client.tcp_stream(None);
        kafka_client.api_versions = api_versions(&mut broker, kafka_client.correlation_id());
        kafka_client.update_topics_metadata();
        kafka_client
    }

    pub fn correlation_id(&mut self) -> i32 {
        self.correlation_id += 1;
        self.correlation_id
    }

    pub fn next_broker(&mut self) -> &String {
        println!("{:#?}", self.next_broker.next().unwrap().borrow());
        self.brokers.get(self.next_broker.next().unwrap().borrow()).unwrap()
    }

    pub fn send_request<T: ToBytes, U: FromBytes>(&mut self,
                                                  node_id: Option<i32>,
                                                  api_key: ApiKeys,
                                                  request_body: T,
                                                  api_version_tmp: i16) -> Response<U> {
        let api_version = self.api_versions.get(&(api_key as i16)).unwrap();
        let header = RequestHeader::new(api_version.api_key, api_version_tmp,
                                        self.correlation_id(), self.client_id.clone());
        let request = Request::new(header, request_body);
        let mut stream = self.tcp_stream(node_id);
        stream.write(request.buffer.as_slice()).unwrap();
        let response = Response::<U>::build(&mut stream);
        response
    }

    pub fn send_request2<T: ToBytes, U: FromBytes>(client_id: String,
                                                   api_version: ApiVersion,
                                                   mut stream: TcpStream,
                                                   request_body: T,
                                                   correlation_id: i32,
                                                   api_version_tmp: i16) -> Response<U> {
        let header = RequestHeader::new(api_version.api_key, api_version_tmp,
                                        correlation_id, client_id);
        let request = Request::new(header, request_body);
        stream.write(request.buffer.as_slice()).unwrap();
        let response = Response::<U>::build(&mut stream);
        response
    }

    fn connect(&mut self) {
        let mut connected_brokers = Vec::new();
        for host in &self.hosts {
            match TcpStream::connect(host) {
                Ok(_) => {
                    connected_brokers.push(host);
                }
                Err(e) => {
                    println!("{}", e);
                }
            }
        }

        if connected_brokers.is_empty() {
            eprintln!("Could not connect to any of the hosts provided");
            exit(1);
        }

        let metadata = Self::fetch_initial_metadata(
            Self::tcp_stream2(connected_brokers.get(0).unwrap().to_string()).borrow_mut(), self.correlation_id()
        ).body;

        println!("Client ({}) successfully connected to cluster on hosts:", self.client_id);
        for broker in &metadata.brokers {
            let host = format!("{}:{}", broker.host, broker.port);
            self.brokers.insert(broker.node_id, host);
        }

    }

    pub fn update_topics_metadata(&mut self) {
        let metadata = self.fetch_metadata(Vec::new()).body;
        if metadata.topics.is_empty() {
            panic!("None of the topics could be found in the cluster.")
        }
        for topic in metadata.topics {
            let topic_name = topic.name;
            for partition in topic.partitions {
                self.topics_metadata
                    .entry(topic_name.clone())
                    .or_insert(HashMap::new())
                    .entry(partition.leader_id)
                    .or_insert(Vec::new())
                    .push(partition.partition_index);
            }
        }
    }

    pub fn fetch_metadata(&mut self, topics: Vec<String>) -> Response<MetadataResponse> {
        let body = MetadataRequest::new(&topics);
        let response: Response<MetadataResponse> = self.send_request(None, ApiKeys::Metadata, body, 6);
        response
    }

    pub fn fetch_initial_metadata(stream: &mut TcpStream, correlation_id: i32) -> Response<MetadataResponse> {
        let body = MetadataRequest::new(Vec::new().as_ref());
        let header = RequestHeader::new(ApiKeys::Metadata as i16, 6,
                                        correlation_id, "initial-metadata".to_string());
        let request = Request::new(header, body);
        stream.write(request.buffer.as_slice()).unwrap();
        let response = Response::<MetadataResponse>::build(stream);
        response
    }

    pub fn tcp_stream(&mut self, host: Option<i32>) -> TcpStream {
        if host.is_none() {
            return TcpStream::connect(self.next_broker()).unwrap();
        }
        let broker = self.brokers.get(&host.unwrap()).unwrap().to_owned();
        TcpStream::connect(broker).unwrap()
    }

    pub fn tcp_stream2(host: String) -> TcpStream {
        TcpStream::connect(host).unwrap()
    }
}
