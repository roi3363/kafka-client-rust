use std::collections::HashMap;
use std::process::exit;

#[derive(Debug)]
pub struct KafkaError {
    pub code: i16,
    pub error: String,
    pub description: String,
}

pub fn check_errors(error_code: i16) {
    if error_code != 0 {
        let kafka_error = KAFKA_ERRORS.get(&error_code).unwrap();
        eprintln!("Error: {}\nReason: {}", kafka_error.error, kafka_error.description);
        exit(1);
    }
}

fn kafka_error(code: i16, error: &str, description: &str) -> KafkaError {
    KafkaError {
        code,
        error: error.to_string(),
        description: description.to_string(),
    }
}

lazy_static! {
    pub static ref KAFKA_ERRORS: HashMap<i16, KafkaError> = {
        let mut m = HashMap::new();
        m.insert(-1, kafka_error(-1, "UNKNOWN_SERVER_ERROR", "The server experienced an unexpected error when processing the request."));
        m.insert(0, kafka_error(0, "NONE", ""));
        m.insert(1, kafka_error(1, "OFFSET_OUT_OF_RANGE", "The requested offset is not within the range of offsets maintained by the server."));
        m.insert(2, kafka_error(2, "CORRUPT_MESSAGE", "This message has failed its CRC checksum, exceeds the valid size, has a null key for a compacted topic, or is otherwise corrupt."));
        m.insert(3, kafka_error(3, "UNKNOWN_TOPIC_OR_PARTITION", "This server does not host this topic-partition."));
        m.insert(4, kafka_error(4, "INVALID_FETCH_SIZE", "The requested fetch size is invalid."));
        m.insert(5, kafka_error(5, "LEADER_NOT_AVAILABLE", "There is no leader for this topic-partition as we are in the middle of a leadership election."));
        m.insert(6, kafka_error(6, "NOT_LEADER_OR_FOLLOWER", "For requests intended only for the leader, this error indicates that the broker is not the current leader. For requests intended for any replica, this error indicates that the broker is not a replica of the topic partition."));
        m.insert(7, kafka_error(7, "REQUEST_TIMED_OUT", "The request timed out."));
        m.insert(8, kafka_error(8, "BROKER_NOT_AVAILABLE", "The broker is not available."));
        m.insert(9, kafka_error(9, "REPLICA_NOT_AVAILABLE", "The replica is not available for the requested topic-partition. Produce/Fetch requests and other requests intended only for the leader or follower return NOT_LEADER_OR_FOLLOWER if the broker is not a replica of the topic-partition."));
        m.insert(10, kafka_error(10, "MESSAGE_TOO_LARGE", "The request included a message larger than the max message size the server will accept."));
        m.insert(11, kafka_error(11, "STALE_CONTROLLER_EPOCH", "The controller moved to another broker."));
        m.insert(12, kafka_error(12, "OFFSET_METADATA_TOO_LARGE", "The metadata field of the offset request was too large."));
        m.insert(13, kafka_error(13, "NETWORK_EXCEPTION", "The server disconnected before a response was received."));
        m.insert(14, kafka_error(14, "COORDINATOR_LOAD_IN_PROGRESS", "The coordinator is loading and hence can't process requests."));
        m.insert(15, kafka_error(15, "COORDINATOR_NOT_AVAILABLE", "The coordinator is not available."));
        m.insert(16, kafka_error(16, "NOT_COORDINATOR", "This is not the correct coordinator."));
        m.insert(17, kafka_error(17, "INVALID_TOPIC_EXCEPTION", "The request attempted to perform an operation on an invalid topic."));
        m.insert(18, kafka_error(18, "RECORD_LIST_TOO_LARGE", "The request included message batch larger than the configured segment size on the server."));
        m.insert(19, kafka_error(19, "NOT_ENOUGH_REPLICAS", "Messages are rejected since there are fewer in-sync replicas than required."));
        m.insert(20, kafka_error(20, "NOT_ENOUGH_REPLICAS_AFTER_APPEND", "Messages are written to the log, but to fewer in-sync replicas than required."));
        m.insert(21, kafka_error(21, "INVALID_REQUIRED_ACKS", "Produce request specified an invalid value for required acks."));
        m.insert(22, kafka_error(22, "ILLEGAL_GENERATION", "Specified group generation id is not valid."));
        m.insert(23, kafka_error(23, "INCONSISTENT_GROUP_PROTOCOL", "The group member's supported protocols are incompatible with those of existing members or first group member tried to join with empty protocol type or empty protocol list."));
        m.insert(24, kafka_error(24, "INVALID_GROUP_ID", "The configured groupId is invalid."));
        m.insert(25, kafka_error(25, "UNKNOWN_MEMBER_ID", "The coordinator is not aware of this member."));
        m.insert(26, kafka_error(26, "INVALID_SESSION_TIMEOUT", "The session timeout is not within the range allowed by the broker (as configured by group.min.session.timeout.ms and group.max.session.timeout.ms)."));
        m.insert(27, kafka_error(27, "REBALANCE_IN_PROGRESS", "The group is rebalancing, so a rejoin is needed."));
        m.insert(28, kafka_error(28, "INVALID_COMMIT_OFFSET_SIZE", "The committing offset data size is not valid."));
        m.insert(29, kafka_error(29, "TOPIC_AUTHORIZATION_FAILED", "Topic authorization failed."));
        m.insert(30, kafka_error(30, "GROUP_AUTHORIZATION_FAILED", "Group authorization failed."));
        m.insert(31, kafka_error(31, "CLUSTER_AUTHORIZATION_FAILED", "Cluster authorization failed."));
        m.insert(32, kafka_error(32, "INVALID_TIMESTAMP", "The timestamp of the message is out of acceptable range."));
        m.insert(33, kafka_error(33, "UNSUPPORTED_SASL_MECHANISM", "The broker does not support the requested SASL mechanism."));
        m.insert(34, kafka_error(34, "ILLEGAL_SASL_STATE", "Request is not valid given the current SASL state."));
        m.insert(35, kafka_error(35, "UNSUPPORTED_VERSION", "The version of API is not supported."));
        m.insert(36, kafka_error(36, "TOPIC_ALREADY_EXISTS", "Topic with this name already exists."));
        m.insert(37, kafka_error(37, "INVALID_PARTITIONS", "Number of partitions is below 1."));
        m.insert(38, kafka_error(38, "INVALID_REPLICATION_FACTOR", "Replication factor is below 1 or larger than the number of available brokers."));
        m.insert(39, kafka_error(39, "INVALID_REPLICA_ASSIGNMENT", "Replica assignment is invalid."));
        m.insert(40, kafka_error(40, "INVALID_CONFIG", "Configuration is invalid."));
        m.insert(41, kafka_error(41, "NOT_CONTROLLER", "This is not the correct controller for this cluster."));
        m.insert(42, kafka_error(42, "INVALID_REQUEST", "This most likely occurs because of a request being malformed by the client library or the message was sent to an incompatible broker. See the broker logs for more details."));
        m.insert(43, kafka_error(43, "UNSUPPORTED_FOR_MESSAGE_FORMAT", "The message format version on the broker does not support the request."));
        m.insert(44, kafka_error(44, "POLICY_VIOLATION", "Request parameters do not satisfy the configured policy."));
        m.insert(45, kafka_error(45, "OUT_OF_ORDER_SEQUENCE_NUMBER", "The broker received an out of order sequence number."));
        m.insert(46, kafka_error(46, "DUPLICATE_SEQUENCE_NUMBER", "The broker received a duplicate sequence number."));
        m.insert(47, kafka_error(47, "INVALID_PRODUCER_EPOCH", "Producer attempted to produce with an old epoch."));
        m.insert(48, kafka_error(48, "INVALID_TXN_STATE", "The producer attempted a transactional operation in an invalid state."));
        m.insert(49, kafka_error(49, "INVALID_PRODUCER_ID_MAPPING", "The producer attempted to use a producer id which is not currently assigned to its transactional id."));
        m.insert(50, kafka_error(50, "INVALID_TRANSACTION_TIMEOUT", "The transaction timeout is larger than the maximum value allowed by the broker (as configured by transaction.max.timeout.ms)."));
        m.insert(51, kafka_error(51, "CONCURRENT_TRANSACTIONS", "The producer attempted to update a transaction while another concurrent operation on the same transaction was ongoing."));
        m.insert(52, kafka_error(52, "TRANSACTION_COORDINATOR_FENCED", "Indicates that the transaction coordinator sending a WriteTxnMarker is no longer the current coordinator for a given producer."));
        m.insert(53, kafka_error(53, "TRANSACTIONAL_ID_AUTHORIZATION_FAILED", "Transactional Id authorization failed."));
        m.insert(54, kafka_error(54, "SECURITY_DISABLED", "Security features are disabled."));
        m.insert(55, kafka_error(55, "OPERATION_NOT_ATTEMPTED", "The broker did not attempt to execute this operation. This may happen for batched RPCs where some operations in the batch failed, causing the broker to respond without trying the rest."));
        m.insert(56, kafka_error(56, "KAFKA_STORAGE_ERROR", "Disk error when trying to access log file on the disk."));
        m.insert(57, kafka_error(57, "LOG_DIR_NOT_FOUND", "The user-specified log directory is not found in the broker config."));
        m.insert(58, kafka_error(58, "SASL_AUTHENTICATION_FAILED", "SASL Authentication failed."));
        m.insert(59, kafka_error(59, "UNKNOWN_PRODUCER_ID", "This exception is raised by the broker if it could not locate the producer metadata associated with the producerId in question. This could happen if, for instance, the producer's records were deleted because their retention time had elapsed. Once the last records of the producerId are removed, the producer's metadata is removed from the broker, and future appends by the producer will return this exception."));
        m.insert(60, kafka_error(60, "REASSIGNMENT_IN_PROGRESS", "A partition reassignment is in progress."));
        m.insert(61, kafka_error(61, "DELEGATION_TOKEN_AUTH_DISABLED", "Delegation Token feature is not enabled."));
        m.insert(62, kafka_error(62, "DELEGATION_TOKEN_NOT_FOUND", "Delegation Token is not found on server."));
        m.insert(63, kafka_error(63, "DELEGATION_TOKEN_OWNER_MISMATCH", "Specified Principal is not valid Owner/Renewer."));
        m.insert(64, kafka_error(64, "DELEGATION_TOKEN_REQUEST_NOT_ALLOWED", "Delegation Token requests are not allowed on PLAINTEXT/1-way SSL channels and on delegation token authenticated channels."));
        m.insert(65, kafka_error(65, "DELEGATION_TOKEN_AUTHORIZATION_FAILED", "Delegation Token authorization failed."));
        m.insert(66, kafka_error(66, "DELEGATION_TOKEN_EXPIRED", "Delegation Token is expired."));
        m.insert(67, kafka_error(67, "INVALID_PRINCIPAL_TYPE", "Supplied principalType is not supported."));
        m.insert(68, kafka_error(68, "NON_EMPTY_GROUP", "The group is not empty."));
        m.insert(69, kafka_error(69, "GROUP_ID_NOT_FOUND", "The group id does not exist."));
        m.insert(70, kafka_error(70, "FETCH_SESSION_ID_NOT_FOUND", "The fetch session ID was not found."));
        m.insert(71, kafka_error(71, "INVALID_FETCH_SESSION_EPOCH", "The fetch session epoch is invalid."));
        m.insert(72, kafka_error(72, "LISTENER_NOT_FOUND", "There is no listener on the leader broker that matches the listener on which metadata request was processed."));
        m.insert(73, kafka_error(73, "TOPIC_DELETION_DISABLED", "Topic deletion is disabled."));
        m.insert(74, kafka_error(74, "FENCED_LEADER_EPOCH", "The leader epoch in the request is older than the epoch on the broker."));
        m.insert(75, kafka_error(75, "UNKNOWN_LEADER_EPOCH", "The leader epoch in the request is newer than the epoch on the broker."));
        m.insert(76, kafka_error(76, "UNSUPPORTED_COMPRESSION_TYPE", "The requesting client does not support the compression type of given partition."));
        m.insert(77, kafka_error(77, "STALE_BROKER_EPOCH", "Broker epoch has changed."));
        m.insert(78, kafka_error(78, "OFFSET_NOT_AVAILABLE", "The leader high watermark has not caught up from a recent leader election so the offsets cannot be guaranteed to be monotonically increasing."));
        m.insert(79, kafka_error(79, "MEMBER_ID_REQUIRED", "The group member needs to have a valid member id before actually entering a consumer group."));
        m.insert(80, kafka_error(80, "PREFERRED_LEADER_NOT_AVAILABLE", "The preferred leader was not available."));
        m.insert(81, kafka_error(81, "GROUP_MAX_SIZE_REACHED", "The consumer group has reached its max size."));
        m.insert(82, kafka_error(82, "FENCED_INSTANCE_ID", "The broker rejected this static consumer since another consumer with the same group.instance.id has registered with a different member.id."));
        m.insert(83, kafka_error(83, "ELIGIBLE_LEADERS_NOT_AVAILABLE", "Eligible topic partition leaders are not available."));
        m.insert(84, kafka_error(84, "ELECTION_NOT_NEEDED", "Leader election not needed for topic partition."));
        m.insert(85, kafka_error(85, "NO_REASSIGNMENT_IN_PROGRESS", "No partition reassignment is in progress."));
        m.insert(86, kafka_error(86, "GROUP_SUBSCRIBED_TO_TOPIC", "Deleting offsets of a topic is forbidden while the consumer group is actively subscribed to it."));
        m.insert(87, kafka_error(87, "INVALID_RECORD", "This record has failed the validation on broker and hence will be rejected."));
        m.insert(88, kafka_error(88, "UNSTABLE_OFFSET_COMMIT", "There are unstable offsets that need to be cleared."));
        m.insert(89, kafka_error(89, "THROTTLING_QUOTA_EXCEEDED", "The throttling quota has been exceeded."));
        m.insert(90, kafka_error(90, "PRODUCER_FENCED", "There is a newer producer with the same transactionalId which fences the current one."));
        m.insert(91, kafka_error(91, "RESOURCE_NOT_FOUND", "A request illegally referred to a resource that does not exist."));
        m.insert(92, kafka_error(92, "DUPLICATE_RESOURCE", "A request illegally referred to the same resource twice."));
        m.insert(93, kafka_error(93, "UNACCEPTABLE_CREDENTIAL", "Requested credential would not meet criteria for acceptability."));
        m.insert(94, kafka_error(94, "INCONSISTENT_VOTER_SET", "Indicates that the either the sender or recipient of a voter-only request is not one of the expected voters"));
        m.insert(95, kafka_error(95, "INVALID_UPDATE_VERSION", "The given update version was invalid."));
        m.insert(96, kafka_error(96, "FEATURE_UPDATE_FAILED", "Unable to update finalized features due to an unexpected server error."));
        m
   };
}