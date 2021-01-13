// use std::net::TcpStream;
// use std::io::{Write, Cursor, Read};
// use crate::protocol::response::Response;
// use crate::protocol::header::RequestHeader;
// use crate::protocol::create_topic::{CreateTopicRequest, CreateTopicResponse};
// use crate::clients::kafka_client::KafkaClient;
// use crate::protocol::api_keys::ApiKeys;
//
//
// const CORRELATION_ID: i32 = 19;
// const CLIENT_ID: &str = "admin-client";
//
//
// pub struct AdminClient<'a> {
//     client: KafkaClient<'a>,
// }
//
// impl <'a> AdminClient<'a> {
//
//     pub fn new(hosts: Vec<&str>) -> Self {
//         Self {
//             client: KafkaClient::new(&hosts, CLIENT_ID.to_string()),
//         }
//     }
//
//     // pub fn create_topic(&mut self, name: &str, num_partitions: i32, replication_factor: i16) -> Response<CreateTopicResponse> {
//     //     let body = CreateTopicRequest::new(name, num_partitions, replication_factor);
//     //     let response: Response<CreateTopicResponse> = self.client.send_request(ApiKeys::CreateTopics, body, 3);
//     //     response
//     // }
// }
//
