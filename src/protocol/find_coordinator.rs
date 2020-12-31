/// FindCoordinator Request (Version: 2) => key key_type
///   key => STRING
///   key_type => INT8
pub struct FindCoordinatorRequest {
    key: String,
    key_type: i8,
}