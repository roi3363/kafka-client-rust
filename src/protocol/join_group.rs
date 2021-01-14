use crate::protocol::request::ToBytes;
use crate::protocol::response::FromBytes;
use std::io::Cursor;
use crate::protocol::primitives::{KafkaString, KafkaPrimitive, KafkaArray};
use crate::protocol::kafka_error_codes::check_errors;


/// JoinGroup Request (Version: 3) => group_id session_timeout_ms rebalance_timeout_ms member_id protocol_type [protocols]
///   group_id => STRING
///   session_timeout_ms => INT32
///   rebalance_timeout_ms => INT32
///   member_id => STRING
///   protocol_type => STRING
///   protocols => name metadata
///     name => STRING
///     metadata => BYTES
///
pub struct JoinGroupRequest {
    group_id: KafkaString,
    session_timeout_ms: i32,
    rebalance_timeout_ms: i32,
    member_id: KafkaString,
    protocol_type: KafkaString,
    protocols: Vec<Protocol>,
}

struct Protocol {
    name: KafkaString,
    metadata: ProtocolMetadata,
}

struct ProtocolMetadata {
    version: i16,
    topics: KafkaArray<String>,
    user_data: Vec<u8>,
}

impl JoinGroupRequest {
    pub fn new(group_id: String) -> Self {
        let metadata = ProtocolMetadata {
            version: 3,
            topics: KafkaArray(vec!["test".to_string()]),
            user_data: vec![]
        };
        let protocols = vec![
            Protocol { name: KafkaString("".to_string()), metadata }
        ];
        Self {
            group_id: KafkaString(group_id),
            session_timeout_ms: 6000,
            rebalance_timeout_ms: 6000,
            member_id: KafkaString("".to_string()),
            protocol_type: KafkaString("consumer".to_string()),
            protocols,
        }
    }
}


impl ToBytes for JoinGroupRequest {
    fn get_in_bytes(&self) -> Vec<u8> {
        let mut buffer = Vec::new();
        self.group_id.write_to_buffer(&mut buffer);
        self.session_timeout_ms.write_to_buffer(&mut buffer);
        self.rebalance_timeout_ms.write_to_buffer(&mut buffer);
        self.member_id.write_to_buffer(&mut buffer);
        self.protocol_type.write_to_buffer(&mut buffer);

        let protocols_len = self.protocols.len() as i32;
        protocols_len.write_to_buffer(&mut buffer);
        for protocol in self.protocols.iter() {
            protocol.name.write_to_buffer(&mut buffer);
            protocol.metadata.topics.write_to_buffer(&mut buffer);
            protocol.metadata.version.write_to_buffer(&mut buffer);
            protocol.metadata.user_data.write_to_buffer(&mut buffer);
        }
        buffer
    }
}

/// JoinGroup Response (Version: 3) => throttle_time_ms error_code generation_id protocol_name leader member_id [members]
///   throttle_time_ms => INT32
///   error_code => INT16
///   generation_id => INT32
///   protocol_name => STRING
///   leader => STRING
///   member_id => STRING
///   members => member_id metadata
///     member_id => STRING
///     metadata => BYTES
///
#[derive(Debug)]
pub struct JoinGroupResponse {
    throttle_time_ms: i32,
    error_code: i16,
    generation_id: i32,
    protocol_name: KafkaString,
    leader: KafkaString,
    member_id: KafkaString,
    members: Vec<Member>,
}

#[derive(Debug)]
struct Member {
    member_id: KafkaString,
    metadata: Vec<u8>,
}

impl FromBytes for JoinGroupResponse {
    fn get_from_bytes(buffer: &mut Cursor<Vec<u8>>) -> Self {
        let mut response = Self {
            throttle_time_ms: i32::read_from_buffer(buffer),
            error_code: i16::read_from_buffer(buffer),
            generation_id: i32::read_from_buffer(buffer),
            protocol_name: KafkaString::read_from_buffer(buffer),
            leader: KafkaString::read_from_buffer(buffer),
            member_id: KafkaString::read_from_buffer(buffer),
            members: vec![],
        };
        check_errors(response.error_code);

        let members_len = i32::read_from_buffer(buffer);
        for _ in 0..members_len {
            let member = Member {
                member_id: KafkaString::read_from_buffer(buffer),
                metadata: vec![],
            };
            response.members.push(member);
        }
        response
    }
}