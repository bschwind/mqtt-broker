use std::convert::TryFrom;

#[derive(Debug)]
pub enum ParseError {
    InvalidPacketType,
    InvalidRemainingLength,
}

#[derive(Debug)]
pub enum PacketType {
    Connect,
    ConnectAck,
    Publish,
    PublishAck,
    PublishReceived,
    PublishRelease,
    PublishComplete,
    Subscribe,
    SubscribeAck,
    Unsubscribe,
    UnsubscribeAck,
    PingRequest,
    PingResponse,
    Disconnect,
    Auth,
}

#[derive(Debug)]
pub struct Packet {
    packet_type: PacketType,
    payload: Vec<u8>,
}

impl Packet {
    pub fn new(packet_type: PacketType, payload: &[u8]) -> Self {
        Self { packet_type, payload: payload.into() }
    }
}

impl TryFrom<u8> for PacketType {
    type Error = ParseError;

    fn try_from(byte: u8) -> Result<Self, Self::Error> {
        match byte {
            1 => Ok(PacketType::Connect),
            2 => Ok(PacketType::ConnectAck),
            3 => Ok(PacketType::Publish),
            4 => Ok(PacketType::PublishAck),
            5 => Ok(PacketType::PublishReceived),
            6 => Ok(PacketType::PublishRelease),
            7 => Ok(PacketType::PublishComplete),
            8 => Ok(PacketType::Subscribe),
            9 => Ok(PacketType::SubscribeAck),
            10 => Ok(PacketType::Unsubscribe),
            11 => Ok(PacketType::UnsubscribeAck),
            12 => Ok(PacketType::PingRequest),
            13 => Ok(PacketType::PingResponse),
            14 => Ok(PacketType::Disconnect),
            15 => Ok(PacketType::Auth),
            _ => Err(ParseError::InvalidPacketType),
        }
    }
}

// Property structs
#[derive(Debug)]
pub struct PayloadFormatIndicator(u8);
#[derive(Debug)]
pub struct MessageExpiryInterval(u32);
#[derive(Debug)]
pub struct ContentType(String);
#[derive(Debug)]
pub struct RepsonseTopic(String);
#[derive(Debug)]
pub struct CorrelationData(Vec<u8>);
#[derive(Debug)]
pub struct SubscriptionIdentifier(u32);
#[derive(Debug)]
pub struct SessionExpiryInterval(u32);
#[derive(Debug)]
pub struct AssignedClientIdentifier(String);
#[derive(Debug)]
pub struct ServerKeepAlive(u16);
#[derive(Debug)]
pub struct AuthenticationMethod(String);
#[derive(Debug)]
pub struct AuthenticationData(Vec<u8>);
#[derive(Debug)]
pub struct RequestProblemInformation(u8);
#[derive(Debug)]
pub struct WilLDelayInterval(u32);
#[derive(Debug)]
pub struct RequestResponseInformation(u8);
#[derive(Debug)]
pub struct ResponseInformation(String);
#[derive(Debug)]
pub struct ServerReference(String);
#[derive(Debug)]
pub struct ReasonString(String);
#[derive(Debug)]
pub struct ReceiveMaximum(u16);
#[derive(Debug)]
pub struct TopicAliasMaximum(u16);
#[derive(Debug)]
pub struct TopicAlias(u16);
#[derive(Debug)]
pub struct MaximumQos(u8);
#[derive(Debug)]
pub struct RetainAvailable(u8);
#[derive(Debug)]
pub struct UserProperty(String, String);
#[derive(Debug)]
pub struct MaximumPacketSize(u32);
#[derive(Debug)]
pub struct WildcardSubscriptionAvailable(u8);
#[derive(Debug)]
pub struct SubscriptionIdentifierAvailable(u8);
#[derive(Debug)]
pub struct SharedSubscriptionAvailable(u8);

pub enum ConnectReason {
    Success,
    UnspecifiedError,
    MalformedPacket,
    ProtocolError,
    ImplementationSpecificError,
    UnsupportedProtocolVersion,
    ClientIdentifierNotValid,
    BadUserNameOrPassword,
    NotAuthorized,
    ServerUnavailable,
    ServerBusy,
    Banned,
    BadAuthenticationMethod,
    TopicNameInvalid,
    PacketTooLarge,
    QuotaExceeded,
    PayloadFormatInvalid,
    RetainNotSupported,
    QosNotSupported,
    UseAnotherServer,
    ServerMoved,
    ConnectionRateExceeded,
}

// Variable headers
#[derive(Debug)]
pub struct ConnectVariableHeader {
    pub protocol_name: String,
    pub protocol_level: u8,
    pub connect_flags: u8,
    pub keep_alive: u16,

    // Properties
    pub session_expiry_interval: Option<SessionExpiryInterval>,
    pub receive_maximum: Option<ReceiveMaximum>,
    pub maximum_packet_size: Option<MaximumPacketSize>,
    pub topic_alias_maximum: Option<TopicAliasMaximum>,
    pub request_response_information: Option<RequestResponseInformation>,
    pub request_problem_information: Option<RequestProblemInformation>,
    pub user_properties: Vec<UserProperty>,
    pub authentication_method: Option<AuthenticationMethod>,
    pub authentication_data: Option<AuthenticationData>,
}

pub struct ConnectAckVariableHeader {
    pub session_present: bool,
    pub reason: ConnectReason,

    // Properties
    pub session_expiry_interval: Option<SessionExpiryInterval>,
    pub receive_maximum: Option<ReceiveMaximum>,
    pub maximum_qus: Option<MaximumQos>,
    pub retain_available: Option<RetainAvailable>,
    pub maximum_packet_size: Option<MaximumPacketSize>,
    pub assigned_client_identifier: Option<AssignedClientIdentifier>,
    pub topic_alias_maximum: Option<TopicAliasMaximum>,
    pub reason_string: Option<ReasonString>,
    pub user_properties: Vec<UserProperty>,
    pub wildcard_subscription_available: Option<WildcardSubscriptionAvailable>,
    pub subscription_identifiers_available: Option<SubscriptionIdentifierAvailable>,
    pub shared_subscription_available: Option<SharedSubscriptionAvailable>,
    pub server_keep_alive: Option<ServerKeepAlive>,
    pub response_information: Option<ResponseInformation>,
    pub server_reference: Option<ServerReference>,
    pub authentication_method: Option<AuthenticationMethod>,
    pub authentication_data: Option<AuthenticationData>,
}

pub struct PublishVariableHeader {
    pub topic_name: String,
    pub packet_id: Option<u16>,
}
