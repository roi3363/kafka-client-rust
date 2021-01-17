use std::collections::HashMap;
use std::io::{Cursor, Write};
use std::iter::Map;
use std::net::TcpStream;
use std::thread;
use std::thread::JoinHandle;

use crate::clients::kafka_client::KafkaClient;
use crate::protocol::api_keys::ApiKeys;
use crate::protocol::api_versions::{api_versions, ApiVersion, ApiVersionsRequest, ApiVersionsResponse};
use crate::protocol::create_topic::{CreateTopicRequest, CreateTopicResponse};
use crate::protocol::fetch::{FetchRequest, FetchResponse};
use crate::protocol::find_coordinator::{FindCoordinatorRequest, FindCoordinatorResponse};
use crate::protocol::header::RequestHeader;
use crate::protocol::join_group::{JoinGroupRequest, JoinGroupResponse};
use crate::protocol::list_offsets::{ListOffsetsRequest, ListOffsetsResponse};
use crate::protocol::metadata::{MetadataRequest, MetadataResponse};
use crate::protocol::offset_commit::{CommitOffsetRequest, CommitOffsetResponse};
use crate::protocol::primitives::{KafkaArray, KafkaString};
use crate::protocol::request::Request;
use crate::protocol::response::Response;
use crate::protocol::produce::{ProduceResponse, ProduceRequest};

const CLIENT_ID: &str = "consumer-client";

#[derive(Debug)]
pub struct ConsumerClient {
    pub kafka_client: KafkaClient,
}

impl ConsumerClient {
    pub fn new(hosts: Vec<&str>) -> Self {
        Self {
            kafka_client: KafkaClient::new(&hosts, CLIENT_ID.to_string()),
        }
    }

    pub fn topics_metadata_in_cache(&self, topics: &Vec<&str>) -> bool {
        self.kafka_client.topics_metadata.keys().any(|x| topics.contains(&x.as_str()))
    }

    pub fn fetch(&mut self, topics: Vec<&str>) -> Vec<Response<FetchResponse>> {
        if !self.topics_metadata_in_cache(&topics) {
            self.kafka_client.update_topics_metadata();
        }

        let mut child_threads = Vec::new();
        for topic in topics {
            let partitions = &self.kafka_client.topics_metadata.get(topic).unwrap().clone();
            for (&node_id, partitions) in partitions {
                if partitions.is_empty() {
                    continue
                }
                let mut partitions_by_topic = HashMap::new();
                partitions_by_topic.insert(topic.to_string(), partitions);
                let body = FetchRequest::new(partitions_by_topic);
                let api_version = self.kafka_client.api_versions.get(&(ApiKeys::Fetch as i16)).unwrap().clone();
                let broker = self.kafka_client.brokers.get(&node_id).unwrap().to_string();
                let correlation_id = self.kafka_client.correlation_id();
                child_threads.push(thread::spawn(move || {
                    let response: Response<FetchResponse> = KafkaClient::send_request2(
                        CLIENT_ID.to_string(),
                        api_version,
                        KafkaClient::tcp_stream2(broker),
                        body,
                        correlation_id,
                        8
                    );
                    response
                }));
            }
        }
        let mut responses = Vec::new();
        child_threads.into_iter().for_each(|x| responses.push(x.join().unwrap()));
        responses
    }


    pub fn commit_offset(&mut self, topics: Vec<&str>) -> Response<CommitOffsetResponse> {
        let body = CommitOffsetRequest::new(topics.get(0).unwrap().to_string());
        let response: Response<CommitOffsetResponse> = self.kafka_client.send_request(
            None, ApiKeys::OffsetCommit, body, 4);
        response
    }

    pub fn list_offsets(&mut self, topics: &Vec<&str>) -> Response<ListOffsetsResponse> {
        let body = ListOffsetsRequest::new(topics);
        let response: Response<ListOffsetsResponse> = self.kafka_client.send_request(
            None, ApiKeys::ListOffsets, body, 3);
        response
    }

    pub fn join_group(&mut self, group_id: String) -> Response<JoinGroupResponse> {
        let body = JoinGroupRequest::new(group_id);
        let response: Response<JoinGroupResponse> = self.kafka_client.send_request(
            None, ApiKeys::JoinGroup, body, 3);
        response
    }

    pub fn find_coordinator(&mut self) -> Response<FindCoordinatorResponse> {
        let body = FindCoordinatorRequest::new();
        let response: Response<FindCoordinatorResponse> = self.kafka_client.send_request(
            None, ApiKeys::FindCoordinator, body, 2);
        response
    }

    pub fn produce(&mut self, topics: Vec<&str>) -> Response<ProduceResponse> {
        if !self.topics_metadata_in_cache(&topics) {
            self.kafka_client.update_topics_metadata();
        }
        let body = ProduceRequest::new();
        let response: Response<ProduceResponse> = self.kafka_client.send_request(
            None, ApiKeys::Produce, body, 6);
        response
    }
}
