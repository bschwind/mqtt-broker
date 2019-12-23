use num_enum::TryFromPrimitive;
use properties::*;

#[derive(Debug)]
pub enum DecodeError {
    InvalidPacketType,
    InvalidRemainingLength,
    PacketTooLarge,
    InvalidUtf8,
    InvalidQoS,
    InvalidRetainHandling,
    InvalidConnectReason,
    InvalidDisconnectReason,
    InvalidPublishAckReason,
    InvalidPublishReceivedReason,
    InvalidPublishReleaseReason,
    InvalidPublishCompleteReason,
    InvalidSubscribeAckReason,
    InvalidUnsubscribeAckReason,
    InvalidAuthenticateReason,
    InvalidPropertyId,
    InvalidPropertyForPacket,
    Io(std::io::Error),
}

impl From<std::io::Error> for DecodeError {
    fn from(err: std::io::Error) -> Self {
        DecodeError::Io(err)
    }
}

#[repr(u8)]
#[derive(Debug, TryFromPrimitive)]
pub enum PacketType {
    Connect = 1,
    ConnectAck = 2,
    Publish = 3,
    PublishAck = 4,
    PublishReceived = 5,
    PublishRelease = 6,
    PublishComplete = 7,
    Subscribe = 8,
    SubscribeAck = 9,
    Unsubscribe = 10,
    UnsubscribeAck = 11,
    PingRequest = 12,
    PingResponse = 13,
    Disconnect = 14,
    Authenticate = 15,
}

#[repr(u8)]
#[derive(Clone, Copy, Debug, TryFromPrimitive)]
pub enum QoS {
    AtMostOnce = 0,  // QoS 0
    AtLeastOnce = 1, // QoS 1
    ExactlyOnce = 2, // QoS 2
}

#[repr(u8)]
#[derive(Debug, TryFromPrimitive)]
pub enum RetainHandling {
    SendAtSubscribeTime = 0,
    SendAtSubscribeTimeIfNonexistent = 1,
    DoNotSend = 2,
}

pub mod properties {
    use super::QoS;
    // Property structs
    #[derive(Debug)]
    pub struct PayloadFormatIndicator(pub u8);
    #[derive(Debug)]
    pub struct MessageExpiryInterval(pub u32);
    #[derive(Debug)]
    pub struct ContentType(pub String);
    #[derive(Debug)]
    pub struct ResponseTopic(pub String);
    #[derive(Debug)]
    pub struct CorrelationData(pub Vec<u8>);
    #[derive(Debug)]
    pub struct SubscriptionIdentifier(pub u32);
    #[derive(Debug)]
    pub struct SessionExpiryInterval(pub u32);
    #[derive(Debug)]
    pub struct AssignedClientIdentifier(pub String);
    #[derive(Debug)]
    pub struct ServerKeepAlive(pub u16);
    #[derive(Debug)]
    pub struct AuthenticationMethod(pub String);
    #[derive(Debug)]
    pub struct AuthenticationData(pub Vec<u8>);
    #[derive(Debug)]
    pub struct RequestProblemInformation(pub u8);
    #[derive(Debug)]
    pub struct WillDelayInterval(pub u32);
    #[derive(Debug)]
    pub struct RequestResponseInformation(pub u8);
    #[derive(Debug)]
    pub struct ResponseInformation(pub String);
    #[derive(Debug)]
    pub struct ServerReference(pub String);
    #[derive(Debug)]
    pub struct ReasonString(pub String);
    #[derive(Debug)]
    pub struct ReceiveMaximum(pub u16);
    #[derive(Debug)]
    pub struct TopicAliasMaximum(pub u16);
    #[derive(Debug)]
    pub struct TopicAlias(pub u16);
    #[derive(Debug)]
    pub struct MaximumQos(pub QoS);
    #[derive(Debug)]
    pub struct RetainAvailable(pub u8);
    #[derive(Debug)]
    pub struct UserProperty(pub String, pub String);
    #[derive(Debug)]
    pub struct MaximumPacketSize(pub u32);
    #[derive(Debug)]
    pub struct WildcardSubscriptionAvailable(pub u8);
    #[derive(Debug)]
    pub struct SubscriptionIdentifierAvailable(pub u8);
    #[derive(Debug)]
    pub struct SharedSubscriptionAvailable(pub u8);

    #[derive(Debug)]
    pub enum Property {
        PayloadFormatIndicator(PayloadFormatIndicator),
        MessageExpiryInterval(MessageExpiryInterval),
        ContentType(ContentType),
        ResponseTopic(ResponseTopic),
        CorrelationData(CorrelationData),
        SubscriptionIdentifier(SubscriptionIdentifier),
        SessionExpiryInterval(SessionExpiryInterval),
        AssignedClientIdentifier(AssignedClientIdentifier),
        ServerKeepAlive(ServerKeepAlive),
        AuthenticationMethod(AuthenticationMethod),
        AuthenticationData(AuthenticationData),
        RequestProblemInformation(RequestProblemInformation),
        WillDelayInterval(WillDelayInterval),
        RequestResponseInformation(RequestResponseInformation),
        ResponseInformation(ResponseInformation),
        ServerReference(ServerReference),
        ReasonString(ReasonString),
        ReceiveMaximum(ReceiveMaximum),
        TopicAliasMaximum(TopicAliasMaximum),
        TopicAlias(TopicAlias),
        MaximumQos(MaximumQos),
        RetainAvailable(RetainAvailable),
        UserProperty(UserProperty),
        MaximumPacketSize(MaximumPacketSize),
        WildcardSubscriptionAvailable(WildcardSubscriptionAvailable),
        SubscriptionIdentifierAvailable(SubscriptionIdentifierAvailable),
        SharedSubscriptionAvailable(SharedSubscriptionAvailable),
    }
}

#[repr(u8)]
#[derive(Debug, TryFromPrimitive)]
pub enum ConnectReason {
    Success = 0,
    UnspecifiedError = 128,
    MalformedPacket = 129,
    ProtocolError = 130,
    ImplementationSpecificError = 131,
    UnsupportedProtocolVersion = 132,
    ClientIdentifierNotValid = 133,
    BadUserNameOrPassword = 134,
    NotAuthorized = 135,
    ServerUnavailable = 136,
    ServerBusy = 137,
    Banned = 138,
    BadAuthenticationMethod = 140,
    TopicNameInvalid = 144,
    PacketTooLarge = 149,
    QuotaExceeded = 151,
    PayloadFormatInvalid = 153,
    RetainNotSupported = 154,
    QosNotSupported = 155,
    UseAnotherServer = 156,
    ServerMoved = 157,
    ConnectionRateExceeded = 159,
}

#[repr(u8)]
#[derive(Debug, TryFromPrimitive)]
pub enum PublishAckReason {
    Success = 0,
    NoMatchingSubscribers = 16,
    UnspecifiedError = 128,
    ImplementationSpecificError = 131,
    NotAuthorized = 135,
    TopicNameInvalid = 144,
    PacketIdentifierInUse = 145,
    QuotaExceeded = 151,
    PayloadFormatInvalid = 153,
}

#[repr(u8)]
#[derive(Debug, TryFromPrimitive)]
pub enum PublishReceivedReason {
    Success = 0,
    NoMatchingSubscribers = 16,
    UnspecifiedError = 128,
    ImplementationSpecificError = 131,
    NotAuthorized = 135,
    TopicNameInvalid = 144,
    PacketIdentifierInUse = 145,
    QuotaExceeded = 151,
    PayloadFormatInvalid = 153,
}

#[repr(u8)]
#[derive(Debug, TryFromPrimitive)]
pub enum PublishReleaseReason {
    Success = 0,
    PacketIdentifierNotFound = 146,
}

#[repr(u8)]
#[derive(Debug, TryFromPrimitive)]
pub enum PublishCompleteReason {
    Success = 0,
    PacketIdentifierNotFound = 146,
}

#[repr(u8)]
#[derive(Clone, Copy, Debug, TryFromPrimitive)]
pub enum SubscribeAckReason {
    GrantedQoSZero = 0,
    GrantedQoSOne = 1,
    GrantedQoSTwo = 2,
    UnspecifiedError = 128,
    ImplementationSpecificError = 131,
    NotAuthorized = 135,
    TopicFilterInvalid = 143,
    PacketIdentifierInUse = 145,
    QuotaExceeded = 151,
    SharedSubscriptionsNotSupported = 158,
    SubscriptionIdentifiersNotSupported = 161,
    WildcardSubscriptionsNotSupported = 162,
}

#[repr(u8)]
#[derive(Debug, TryFromPrimitive)]
pub enum UnsubscribeAckReason {
    Success = 0,
    NoSubscriptionExisted = 17,
    UnspecifiedError = 128,
    ImplementationSpecificError = 131,
    NotAuthorized = 135,
    TopicFilterInvalid = 143,
    PacketIdentifierInUse = 145,
}

#[repr(u8)]
#[derive(Debug, TryFromPrimitive)]
pub enum DisconnectReason {
    NormalDisconnection = 0,
    DisconnectWithWillMessage = 4,
    UnspecifiedError = 128,
    MalformedPacket = 129,
    ProtocolError = 130,
    ImplementationSpecificError = 131,
    NotAuthorized = 135,
    ServerBusy = 137,
    ServerShuttingDown = 139,
    KeepAliveTimeout = 141,
    SessionTakenOver = 142,
    TopicFilterInvalid = 143,
    TopicNameInvalid = 144,
    ReceiveMaximumExceeded = 147,
    TopicAliasInvalid = 148,
    PacketTooLarge = 149,
    MessageRateTooHigh = 150,
    QuotaExceeded = 151,
    AdministrativeAction = 152,
    PayloadFormatInvalid = 153,
    RetainNotSupported = 154,
    QosNotSupported = 155,
    UseAnotherServer = 156,
    ServerMoved = 157,
    SharedSubscriptionNotAvailable = 158,
    ConnectionRateExceeded = 159,
    MaximumConnectTime = 160,
    SubscriptionIdentifiersNotAvailable = 161,
    WildcardSubscriptionsNotAvailable = 162,
}

#[repr(u8)]
#[derive(Debug, TryFromPrimitive)]
pub enum AuthenticateReason {
    Success = 0,
    ContinueAuthentication = 24,
    ReAuthenticate = 25,
}

// Payloads
#[derive(Debug)]
pub struct FinalWill {
    pub topic: String,
    pub payload: Vec<u8>,
    pub qos: QoS,
    pub should_retain: bool,

    // Properties
    pub will_delay_interval: Option<WillDelayInterval>,
    pub payload_format_indicator: Option<PayloadFormatIndicator>,
    pub message_expiry_interval: Option<MessageExpiryInterval>,
    pub content_type: Option<ContentType>,
    pub response_topic: Option<ResponseTopic>,
    pub correlation_data: Option<CorrelationData>,
    pub user_properties: Vec<UserProperty>,
}

#[derive(Debug)]
pub struct SubscriptionTopic {
    pub topic: String,
    pub maximum_qos: QoS,
    pub no_local: bool,
    pub retain_as_published: bool,
    pub retain_handling: RetainHandling,
}

// Control Packets
#[derive(Debug)]
pub struct ConnectPacket {
    // Variable Header
    pub protocol_name: String,
    pub protocol_level: u8,
    pub clean_start: bool,
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

    // Payload
    pub client_id: String,
    pub will: Option<FinalWill>,
    pub user_name: Option<String>,
    pub password: Option<String>,
}

#[derive(Debug)]
pub struct ConnectAckPacket {
    // Variable header
    pub session_present: bool,
    pub reason_code: ConnectReason,

    // Properties
    pub session_expiry_interval: Option<SessionExpiryInterval>,
    pub receive_maximum: Option<ReceiveMaximum>,
    pub maximum_qos: Option<MaximumQos>,
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

#[derive(Debug)]
pub struct PublishPacket {
    // Fixed header
    pub is_duplicate: bool,
    pub qos: QoS,
    pub retain: bool,

    // Variable header
    pub topic_name: String,
    pub packet_id: Option<u16>,

    // Properties
    pub payload_format_indicator: Option<PayloadFormatIndicator>, // TODO(bschwind) - Is this truly optional?
    pub message_expiry_interval: Option<MessageExpiryInterval>,
    pub topic_alias: Option<TopicAlias>,
    pub response_topic: Option<ResponseTopic>,
    pub correlation_data: Option<CorrelationData>,
    pub user_properties: Vec<UserProperty>,
    pub subscription_identifier: Option<SubscriptionIdentifier>,
    pub content_type: Option<ContentType>,

    // Payload
    pub payload: Vec<u8>,
}

#[derive(Debug)]
pub struct PublishAckPacket {
    // Variable header
    pub packet_id: u16,
    pub reason_code: PublishAckReason,

    // Properties
    pub reason_string: Option<ReasonString>,
    pub user_properties: Vec<UserProperty>,
}

#[derive(Debug)]
pub struct PublishReceivedPacket {
    // Variable header
    pub packet_id: u16,
    pub reason_code: PublishReceivedReason,

    // Properties
    pub reason_string: Option<ReasonString>,
    pub user_properties: Vec<UserProperty>,
}

#[derive(Debug)]
pub struct PublishReleasePacket {
    // Variable header
    pub packet_id: u16,
    pub reason_code: PublishReleaseReason,

    // Properties
    pub reason_string: Option<ReasonString>,
    pub user_properties: Vec<UserProperty>,
}

#[derive(Debug)]
pub struct PublishCompletePacket {
    // Variable header
    pub packet_id: u16,
    pub reason_code: PublishCompleteReason,

    // Properties
    pub reason_string: Option<ReasonString>,
    pub user_properties: Vec<UserProperty>,
}

#[derive(Debug)]
pub struct SubscribePacket {
    // Variable header
    pub packet_id: u16,

    // Properties
    pub subscription_identifier: Option<SubscriptionIdentifier>,
    pub user_properties: Vec<UserProperty>,

    // Payload
    pub subscription_topics: Vec<SubscriptionTopic>,
}

#[derive(Debug)]
pub struct SubscribeAckPacket {
    // Variable header
    pub packet_id: u16,

    // Properties
    pub reason_string: Option<ReasonString>,
    pub user_properties: Vec<UserProperty>,

    // Payload
    pub reason_codes: Vec<SubscribeAckReason>,
}

#[derive(Debug)]
pub struct UnsubscribePacket {
    // Variable header
    pub packet_id: u16,

    // Properties
    pub user_properties: Vec<UserProperty>,

    // Payload
    pub topics: Vec<String>,
}

#[derive(Debug)]
pub struct UnsubscribeAckPacket {
    // Variable header
    pub packet_id: u16,

    // Properties
    pub reason_string: Option<ReasonString>,
    pub user_properties: Vec<UserProperty>,

    // Payload
    pub reason_codes: Vec<UnsubscribeAckReason>,
}

#[derive(Debug)]
pub struct DisconnectPacket {
    // Variable header
    pub reason_code: DisconnectReason,

    // Properties
    pub session_expiry_interval: Option<SessionExpiryInterval>,
    pub reason_string: Option<ReasonString>,
    pub user_properties: Vec<UserProperty>,
    pub server_reference: Option<ServerReference>,
}

#[derive(Debug)]
pub struct AuthenticatePacket {
    // Variable header
    pub reason_code: AuthenticateReason,

    // Properties
    pub authentication_method: Option<AuthenticationMethod>,
    pub authentication_data: Option<AuthenticationData>,
    pub reason_string: Option<ReasonString>,
    pub user_properties: Vec<UserProperty>,
}

#[derive(Debug)]
pub enum Packet {
    Connect(ConnectPacket),
    ConnectAck(ConnectAckPacket),
    Publish(PublishPacket),
    PublishAck(PublishAckPacket),
    PublishReceived(PublishReceivedPacket),
    PublishRelease(PublishReleasePacket),
    PublishComplete(PublishCompletePacket),
    Subscribe(SubscribePacket),
    SubscribeAck(SubscribeAckPacket),
    Unsubscribe(UnsubscribePacket),
    UnsubscribeAck(UnsubscribeAckPacket),
    PingRequest,
    PingResponse,
    Disconnect(DisconnectPacket),
    Authenticate(AuthenticatePacket),
}

impl Packet {
    pub fn to_byte(&self) -> u8 {
        match self {
            Packet::Connect(_) => 1,
            Packet::ConnectAck(_) => 2,
            Packet::Publish(_) => 3,
            Packet::PublishAck(_) => 4,
            Packet::PublishReceived(_) => 5,
            Packet::PublishRelease(_) => 6,
            Packet::PublishComplete(_) => 7,
            Packet::Subscribe(_) => 8,
            Packet::SubscribeAck(_) => 9,
            Packet::Unsubscribe(_) => 10,
            Packet::UnsubscribeAck(_) => 11,
            Packet::PingRequest => 12,
            Packet::PingResponse => 13,
            Packet::Disconnect(_) => 14,
            Packet::Authenticate(_) => 15,
        }
    }

    pub fn fixed_header_flags(&self) -> u8 {
        match self {
            Packet::Connect(_)
            | Packet::ConnectAck(_)
            | Packet::PublishAck(_)
            | Packet::PublishReceived(_)
            | Packet::PublishComplete(_)
            | Packet::SubscribeAck(_)
            | Packet::UnsubscribeAck(_)
            | Packet::PingRequest
            | Packet::PingResponse
            | Packet::Disconnect(_)
            | Packet::Authenticate(_) => 0b0000_0000,
            Packet::PublishRelease(_) | Packet::Subscribe(_) | Packet::Unsubscribe(_) => {
                0b0000_0010
            },
            Packet::Publish(publish_packet) => {
                let mut flags: u8 = 0;

                if publish_packet.is_duplicate {
                    flags |= 0b0000_1000;
                }

                let qos = publish_packet.qos as u8;
                let qos_bits = 0b0000_0110 & (qos << 1);
                flags |= qos_bits;

                if publish_packet.retain {
                    flags |= 0b0000_0001;
                }

                flags
            },
        }
    }
}
