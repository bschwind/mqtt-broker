use bytes::{BufMut, BytesMut};
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

#[derive(Debug)]
pub enum ProtocolError {
    MalformedPacket(DecodeError),
    ConnectTimedOut,
    FirstPacketNotConnect,
}

#[derive(Debug, PartialEq)]
pub struct VariableByteInt(pub u32);

impl VariableByteInt {
    pub fn encode_to_bytes(&self, bytes: &mut BytesMut) {
        let mut x = self.0;

        loop {
            let mut encoded_byte: u8 = (x % 128) as u8;
            x /= 128;

            if x > 0 {
                encoded_byte |= 128;
            }

            bytes.put_u8(encoded_byte);

            if x == 0 {
                break;
            }
        }
    }

    pub fn calculate_size(&self) -> u32 {
        self.calc_size()
    }
}

impl From<std::io::Error> for DecodeError {
    fn from(err: std::io::Error) -> Self {
        DecodeError::Io(err)
    }
}

trait PacketSize {
    fn calc_size(&self) -> u32;
}

pub trait PropertySize {
    fn property_size(&self) -> u32;
}

pub trait Encode {
    fn encode(&self, bytes: &mut BytesMut);
}

impl<T: Encode> Encode for Option<T> {
    fn encode(&self, bytes: &mut BytesMut) {
        if let Some(data) = self {
            data.encode(bytes);
        }
    }
}

impl Encode for Vec<UserProperty> {
    fn encode(&self, bytes: &mut BytesMut) {
        for property in self {
            property.encode(bytes);
        }
    }
}

impl PacketSize for u16 {
    fn calc_size(&self) -> u32 {
        2
    }
}

impl PacketSize for VariableByteInt {
    fn calc_size(&self) -> u32 {
        match self.0 {
            0..=127 => 1,
            128..=16_383 => 2,
            16384..=2_097_151 => 3,
            2_097_152..=268_435_455 => 4,
            _ => unreachable!(),
        }
    }
}

impl PacketSize for String {
    fn calc_size(&self) -> u32 {
        2 + self.len() as u32
    }
}

impl PacketSize for &[u8] {
    fn calc_size(&self) -> u32 {
        2 + self.len() as u32
    }
}

impl PacketSize for Vec<u8> {
    fn calc_size(&self) -> u32 {
        2 + self.len() as u32
    }
}

impl PacketSize for Vec<UserProperty> {
    fn calc_size(&self) -> u32 {
        self.iter().map(|x| x.calc_size()).sum()
    }
}

impl PacketSize for Vec<SubscriptionTopic> {
    fn calc_size(&self) -> u32 {
        self.iter().map(|x| x.calc_size()).sum()
    }
}

impl PacketSize for Vec<String> {
    fn calc_size(&self) -> u32 {
        self.iter().map(|x| x.calc_size()).sum()
    }
}

impl<T: PacketSize> PacketSize for Option<T> {
    fn calc_size(&self) -> u32 {
        match self {
            Some(p) => p.calc_size(),
            None => 0,
        }
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
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, TryFromPrimitive)]
pub enum QoS {
    AtMostOnce = 0,  // QoS 0
    AtLeastOnce = 1, // QoS 1
    ExactlyOnce = 2, // QoS 2
}

#[repr(u8)]
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, TryFromPrimitive)]
pub enum RetainHandling {
    SendAtSubscribeTime = 0,
    SendAtSubscribeTimeIfNonexistent = 1,
    DoNotSend = 2,
}

pub mod properties {
    use super::{PacketSize, QoS, VariableByteInt};
    use num_enum::TryFromPrimitive;

    // TODO - Technically property IDs are encoded as a variable
    //        byte int, so `1 + ` lines should be replaced with the
    //        variable byte count. But in practice they're all 1.
    // Property structs
    #[derive(Debug, PartialEq)]
    pub struct PayloadFormatIndicator(pub u8);
    impl PacketSize for PayloadFormatIndicator {
        fn calc_size(&self) -> u32 {
            1 + 1
        }
    }

    #[derive(Debug, PartialEq)]
    pub struct MessageExpiryInterval(pub u32);
    impl PacketSize for MessageExpiryInterval {
        fn calc_size(&self) -> u32 {
            1 + 4
        }
    }

    #[derive(Debug, PartialEq)]
    pub struct ContentType(pub String);
    impl PacketSize for ContentType {
        fn calc_size(&self) -> u32 {
            1 + self.0.calc_size()
        }
    }

    #[derive(Debug, PartialEq)]
    pub struct ResponseTopic(pub String);
    impl PacketSize for ResponseTopic {
        fn calc_size(&self) -> u32 {
            1 + self.0.calc_size()
        }
    }

    #[derive(Debug, PartialEq)]
    pub struct CorrelationData(pub Vec<u8>);
    impl PacketSize for CorrelationData {
        fn calc_size(&self) -> u32 {
            1 + self.0.calc_size()
        }
    }

    #[derive(Debug, PartialEq)]
    pub struct SubscriptionIdentifier(pub VariableByteInt);
    impl PacketSize for SubscriptionIdentifier {
        fn calc_size(&self) -> u32 {
            1 + self.0.calc_size()
        }
    }

    #[derive(Debug, PartialEq)]
    pub struct SessionExpiryInterval(pub u32);
    impl PacketSize for SessionExpiryInterval {
        fn calc_size(&self) -> u32 {
            1 + 4
        }
    }

    #[derive(Debug, PartialEq)]
    pub struct AssignedClientIdentifier(pub String);
    impl PacketSize for AssignedClientIdentifier {
        fn calc_size(&self) -> u32 {
            1 + self.0.calc_size()
        }
    }

    #[derive(Debug, PartialEq)]
    pub struct ServerKeepAlive(pub u16);
    impl PacketSize for ServerKeepAlive {
        fn calc_size(&self) -> u32 {
            1 + 2
        }
    }

    #[derive(Debug, PartialEq)]
    pub struct AuthenticationMethod(pub String);
    impl PacketSize for AuthenticationMethod {
        fn calc_size(&self) -> u32 {
            1 + self.0.calc_size()
        }
    }

    #[derive(Debug, PartialEq)]
    pub struct AuthenticationData(pub Vec<u8>);
    impl PacketSize for AuthenticationData {
        fn calc_size(&self) -> u32 {
            1 + self.0.calc_size()
        }
    }

    #[derive(Debug, PartialEq)]
    pub struct RequestProblemInformation(pub u8);
    impl PacketSize for RequestProblemInformation {
        fn calc_size(&self) -> u32 {
            1 + 1
        }
    }

    #[derive(Debug, PartialEq)]
    pub struct WillDelayInterval(pub u32);
    impl PacketSize for WillDelayInterval {
        fn calc_size(&self) -> u32 {
            1 + 4
        }
    }

    #[derive(Debug, PartialEq)]
    pub struct RequestResponseInformation(pub u8);
    impl PacketSize for RequestResponseInformation {
        fn calc_size(&self) -> u32 {
            1 + 1
        }
    }

    #[derive(Debug, PartialEq)]
    pub struct ResponseInformation(pub String);
    impl PacketSize for ResponseInformation {
        fn calc_size(&self) -> u32 {
            1 + self.0.calc_size()
        }
    }

    #[derive(Debug, PartialEq)]
    pub struct ServerReference(pub String);
    impl PacketSize for ServerReference {
        fn calc_size(&self) -> u32 {
            1 + self.0.calc_size()
        }
    }

    #[derive(Debug, PartialEq)]
    pub struct ReasonString(pub String);
    impl PacketSize for ReasonString {
        fn calc_size(&self) -> u32 {
            1 + self.0.calc_size()
        }
    }

    #[derive(Debug, PartialEq)]
    pub struct ReceiveMaximum(pub u16);
    impl PacketSize for ReceiveMaximum {
        fn calc_size(&self) -> u32 {
            1 + 2
        }
    }

    #[derive(Debug, PartialEq)]
    pub struct TopicAliasMaximum(pub u16);
    impl PacketSize for TopicAliasMaximum {
        fn calc_size(&self) -> u32 {
            1 + 2
        }
    }

    #[derive(Debug, PartialEq)]
    pub struct TopicAlias(pub u16);
    impl PacketSize for TopicAlias {
        fn calc_size(&self) -> u32 {
            1 + 2
        }
    }

    #[derive(Debug, PartialEq)]
    pub struct MaximumQos(pub QoS);
    impl PacketSize for MaximumQos {
        fn calc_size(&self) -> u32 {
            1 + 1
        }
    }

    #[derive(Debug, PartialEq)]
    pub struct RetainAvailable(pub u8);
    impl PacketSize for RetainAvailable {
        fn calc_size(&self) -> u32 {
            1 + 1
        }
    }

    #[derive(Debug, PartialEq)]
    pub struct UserProperty(pub String, pub String);
    impl PacketSize for UserProperty {
        fn calc_size(&self) -> u32 {
            1 + self.0.calc_size() + self.1.calc_size()
        }
    }

    #[derive(Debug, PartialEq)]
    pub struct MaximumPacketSize(pub u32);
    impl PacketSize for MaximumPacketSize {
        fn calc_size(&self) -> u32 {
            1 + 4
        }
    }

    #[derive(Debug, PartialEq)]
    pub struct WildcardSubscriptionAvailable(pub u8);
    impl PacketSize for WildcardSubscriptionAvailable {
        fn calc_size(&self) -> u32 {
            1 + 1
        }
    }

    #[derive(Debug, PartialEq)]
    pub struct SubscriptionIdentifierAvailable(pub u8);
    impl PacketSize for SubscriptionIdentifierAvailable {
        fn calc_size(&self) -> u32 {
            1 + 1
        }
    }

    #[derive(Debug, PartialEq)]
    pub struct SharedSubscriptionAvailable(pub u8);
    impl PacketSize for SharedSubscriptionAvailable {
        fn calc_size(&self) -> u32 {
            1 + 1
        }
    }

    #[repr(u32)]
    #[derive(Debug, PartialEq, TryFromPrimitive)]
    pub enum PropertyType {
        PayloadFormatIndicator = 1,
        MessageExpiryInterval = 2,
        ContentType = 3,
        ResponseTopic = 8,
        CorrelationData = 9,
        SubscriptionIdentifier = 11,
        SessionExpiryInterval = 17,
        AssignedClientIdentifier = 18,
        ServerKeepAlive = 19,
        AuthenticationMethod = 21,
        AuthenticationData = 22,
        RequestProblemInformation = 23,
        WillDelayInterval = 24,
        RequestResponseInformation = 25,
        ResponseInformation = 26,
        ServerReference = 28,
        ReasonString = 31,
        ReceiveMaximum = 33,
        TopicAliasMaximum = 34,
        TopicAlias = 35,
        MaximumQos = 36,
        RetainAvailable = 37,
        UserProperty = 38,
        MaximumPacketSize = 39,
        WildcardSubscriptionAvailable = 40,
        SubscriptionIdentifierAvailable = 41,
        SharedSubscriptionAvailable = 42,
    }

    #[derive(Debug, PartialEq)]
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

    impl Property {
        pub fn property_type(&self) -> PropertyType {
            match self {
                Property::PayloadFormatIndicator(_) => PropertyType::PayloadFormatIndicator,
                Property::MessageExpiryInterval(_) => PropertyType::MessageExpiryInterval,
                Property::ContentType(_) => PropertyType::ContentType,
                Property::ResponseTopic(_) => PropertyType::ResponseTopic,
                Property::CorrelationData(_) => PropertyType::CorrelationData,
                Property::SubscriptionIdentifier(_) => PropertyType::SubscriptionIdentifier,
                Property::SessionExpiryInterval(_) => PropertyType::SessionExpiryInterval,
                Property::AssignedClientIdentifier(_) => PropertyType::AssignedClientIdentifier,
                Property::ServerKeepAlive(_) => PropertyType::ServerKeepAlive,
                Property::AuthenticationMethod(_) => PropertyType::AuthenticationMethod,
                Property::AuthenticationData(_) => PropertyType::AuthenticationData,
                Property::RequestProblemInformation(_) => PropertyType::RequestProblemInformation,
                Property::WillDelayInterval(_) => PropertyType::WillDelayInterval,
                Property::RequestResponseInformation(_) => PropertyType::RequestResponseInformation,
                Property::ResponseInformation(_) => PropertyType::ResponseInformation,
                Property::ServerReference(_) => PropertyType::ServerReference,
                Property::ReasonString(_) => PropertyType::ReasonString,
                Property::ReceiveMaximum(_) => PropertyType::ReceiveMaximum,
                Property::TopicAliasMaximum(_) => PropertyType::TopicAliasMaximum,
                Property::TopicAlias(_) => PropertyType::TopicAlias,
                Property::MaximumQos(_) => PropertyType::MaximumQos,
                Property::RetainAvailable(_) => PropertyType::RetainAvailable,
                Property::UserProperty(_) => PropertyType::UserProperty,
                Property::MaximumPacketSize(_) => PropertyType::MaximumPacketSize,
                Property::WildcardSubscriptionAvailable(_) => {
                    PropertyType::WildcardSubscriptionAvailable
                },
                Property::SubscriptionIdentifierAvailable(_) => {
                    PropertyType::SubscriptionIdentifierAvailable
                },
                Property::SharedSubscriptionAvailable(_) => {
                    PropertyType::SharedSubscriptionAvailable
                },
            }
        }
    }
}

#[repr(u8)]
#[derive(Copy, Clone, Debug, PartialEq, TryFromPrimitive)]
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
#[derive(Copy, Clone, Debug, PartialEq, TryFromPrimitive)]
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
#[derive(Copy, Clone, Debug, PartialEq, TryFromPrimitive)]
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
#[derive(Copy, Clone, Debug, PartialEq, TryFromPrimitive)]
pub enum PublishReleaseReason {
    Success = 0,
    PacketIdentifierNotFound = 146,
}

#[repr(u8)]
#[derive(Copy, Clone, Debug, PartialEq, TryFromPrimitive)]
pub enum PublishCompleteReason {
    Success = 0,
    PacketIdentifierNotFound = 146,
}

#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, TryFromPrimitive)]
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
#[derive(Copy, Clone, Debug, PartialEq, TryFromPrimitive)]
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
#[derive(Copy, Clone, Debug, PartialEq, TryFromPrimitive)]
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
#[derive(Copy, Clone, Debug, PartialEq, TryFromPrimitive)]
pub enum AuthenticateReason {
    Success = 0,
    ContinueAuthentication = 24,
    ReAuthenticate = 25,
}

// Payloads
#[derive(Debug, PartialEq)]
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

impl PacketSize for FinalWill {
    fn calc_size(&self) -> u32 {
        let mut size = 0;

        size += self.topic.calc_size();
        size += self.payload.calc_size();

        let property_size = self.property_size();
        size += property_size + VariableByteInt(property_size).calc_size();

        size
    }
}

impl PropertySize for FinalWill {
    fn property_size(&self) -> u32 {
        let mut property_size = 0;
        property_size += self.will_delay_interval.calc_size();
        property_size += self.payload_format_indicator.calc_size();
        property_size += self.message_expiry_interval.calc_size();
        property_size += self.content_type.calc_size();
        property_size += self.response_topic.calc_size();
        property_size += self.correlation_data.calc_size();
        property_size += self.user_properties.calc_size();

        property_size
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SubscriptionTopic {
    pub topic: String,
    pub maximum_qos: QoS,
    pub no_local: bool,
    pub retain_as_published: bool,
    pub retain_handling: RetainHandling,
}

impl PacketSize for SubscriptionTopic {
    fn calc_size(&self) -> u32 {
        self.topic.calc_size() + 1
    }
}

// Control Packets
#[derive(Debug, PartialEq)]
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

impl PropertySize for ConnectPacket {
    fn property_size(&self) -> u32 {
        let mut property_size = 0;
        property_size += self.session_expiry_interval.calc_size();
        property_size += self.receive_maximum.calc_size();
        property_size += self.maximum_packet_size.calc_size();
        property_size += self.topic_alias_maximum.calc_size();
        property_size += self.request_response_information.calc_size();
        property_size += self.request_problem_information.calc_size();
        property_size += self.user_properties.calc_size();
        property_size += self.authentication_method.calc_size();
        property_size += self.authentication_data.calc_size();

        property_size
    }
}

#[derive(Debug, PartialEq)]
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

impl PropertySize for ConnectAckPacket {
    fn property_size(&self) -> u32 {
        let mut property_size = 0;
        property_size += self.session_expiry_interval.calc_size();
        property_size += self.receive_maximum.calc_size();
        property_size += self.maximum_qos.calc_size();
        property_size += self.retain_available.calc_size();
        property_size += self.maximum_packet_size.calc_size();
        property_size += self.assigned_client_identifier.calc_size();
        property_size += self.topic_alias_maximum.calc_size();
        property_size += self.reason_string.calc_size();
        property_size += self.user_properties.calc_size();
        property_size += self.wildcard_subscription_available.calc_size();
        property_size += self.subscription_identifiers_available.calc_size();
        property_size += self.shared_subscription_available.calc_size();
        property_size += self.server_keep_alive.calc_size();
        property_size += self.response_information.calc_size();
        property_size += self.server_reference.calc_size();
        property_size += self.authentication_method.calc_size();
        property_size += self.authentication_data.calc_size();

        property_size
    }
}

#[derive(Debug, PartialEq)]
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

impl PropertySize for PublishPacket {
    fn property_size(&self) -> u32 {
        let mut property_size = 0;
        property_size += self.payload_format_indicator.calc_size();
        property_size += self.message_expiry_interval.calc_size();
        property_size += self.topic_alias.calc_size();
        property_size += self.response_topic.calc_size();
        property_size += self.correlation_data.calc_size();
        property_size += self.user_properties.calc_size();
        property_size += self.subscription_identifier.calc_size();
        property_size += self.content_type.calc_size();

        property_size
    }
}

#[derive(Debug, PartialEq)]
pub struct PublishAckPacket {
    // Variable header
    pub packet_id: u16,
    pub reason_code: PublishAckReason,

    // Properties
    pub reason_string: Option<ReasonString>,
    pub user_properties: Vec<UserProperty>,
}

impl PropertySize for PublishAckPacket {
    fn property_size(&self) -> u32 {
        let mut property_size = 0;
        property_size += self.reason_string.calc_size();
        property_size += self.user_properties.calc_size();

        property_size
    }
}

#[derive(Debug, PartialEq)]
pub struct PublishReceivedPacket {
    // Variable header
    pub packet_id: u16,
    pub reason_code: PublishReceivedReason,

    // Properties
    pub reason_string: Option<ReasonString>,
    pub user_properties: Vec<UserProperty>,
}

impl PropertySize for PublishReceivedPacket {
    fn property_size(&self) -> u32 {
        let mut property_size = 0;
        property_size += self.reason_string.calc_size();
        property_size += self.user_properties.calc_size();

        property_size
    }
}

#[derive(Debug, PartialEq)]
pub struct PublishReleasePacket {
    // Variable header
    pub packet_id: u16,
    pub reason_code: PublishReleaseReason,

    // Properties
    pub reason_string: Option<ReasonString>,
    pub user_properties: Vec<UserProperty>,
}

impl PropertySize for PublishReleasePacket {
    fn property_size(&self) -> u32 {
        let mut property_size = 0;
        property_size += self.reason_string.calc_size();
        property_size += self.user_properties.calc_size();

        property_size
    }
}

#[derive(Debug, PartialEq)]
pub struct PublishCompletePacket {
    // Variable header
    pub packet_id: u16,
    pub reason_code: PublishCompleteReason,

    // Properties
    pub reason_string: Option<ReasonString>,
    pub user_properties: Vec<UserProperty>,
}

impl PropertySize for PublishCompletePacket {
    fn property_size(&self) -> u32 {
        let mut property_size = 0;
        property_size += self.reason_string.calc_size();
        property_size += self.user_properties.calc_size();

        property_size
    }
}

#[derive(Debug, PartialEq)]
pub struct SubscribePacket {
    // Variable header
    pub packet_id: u16,

    // Properties
    pub subscription_identifier: Option<SubscriptionIdentifier>,
    pub user_properties: Vec<UserProperty>,

    // Payload
    pub subscription_topics: Vec<SubscriptionTopic>,
}

impl PropertySize for SubscribePacket {
    fn property_size(&self) -> u32 {
        let mut property_size = 0;
        property_size += self.subscription_identifier.calc_size();
        property_size += self.user_properties.calc_size();

        property_size
    }
}

#[derive(Debug, PartialEq)]
pub struct SubscribeAckPacket {
    // Variable header
    pub packet_id: u16,

    // Properties
    pub reason_string: Option<ReasonString>,
    pub user_properties: Vec<UserProperty>,

    // Payload
    pub reason_codes: Vec<SubscribeAckReason>,
}

impl PropertySize for SubscribeAckPacket {
    fn property_size(&self) -> u32 {
        let mut property_size = 0;
        property_size += self.reason_string.calc_size();
        property_size += self.user_properties.calc_size();

        property_size
    }
}

#[derive(Debug, PartialEq)]
pub struct UnsubscribePacket {
    // Variable header
    pub packet_id: u16,

    // Properties
    pub user_properties: Vec<UserProperty>,

    // Payload
    pub topics: Vec<String>,
}

impl PropertySize for UnsubscribePacket {
    fn property_size(&self) -> u32 {
        let mut property_size = 0;
        property_size += self.user_properties.calc_size();

        property_size
    }
}

#[derive(Debug, PartialEq)]
pub struct UnsubscribeAckPacket {
    // Variable header
    pub packet_id: u16,

    // Properties
    pub reason_string: Option<ReasonString>,
    pub user_properties: Vec<UserProperty>,

    // Payload
    pub reason_codes: Vec<UnsubscribeAckReason>,
}

impl PropertySize for UnsubscribeAckPacket {
    fn property_size(&self) -> u32 {
        let mut property_size = 0;
        property_size += self.reason_string.calc_size();
        property_size += self.user_properties.calc_size();

        property_size
    }
}

#[derive(Debug, PartialEq)]
pub struct DisconnectPacket {
    // Variable header
    pub reason_code: DisconnectReason,

    // Properties
    pub session_expiry_interval: Option<SessionExpiryInterval>,
    pub reason_string: Option<ReasonString>,
    pub user_properties: Vec<UserProperty>,
    pub server_reference: Option<ServerReference>,
}

impl PropertySize for DisconnectPacket {
    fn property_size(&self) -> u32 {
        let mut property_size = 0;
        property_size += self.session_expiry_interval.calc_size();
        property_size += self.reason_string.calc_size();
        property_size += self.user_properties.calc_size();
        property_size += self.server_reference.calc_size();

        property_size
    }
}

#[derive(Debug, PartialEq)]
pub struct AuthenticatePacket {
    // Variable header
    pub reason_code: AuthenticateReason,

    // Properties
    pub authentication_method: Option<AuthenticationMethod>,
    pub authentication_data: Option<AuthenticationData>,
    pub reason_string: Option<ReasonString>,
    pub user_properties: Vec<UserProperty>,
}

impl PropertySize for AuthenticatePacket {
    fn property_size(&self) -> u32 {
        let mut property_size = 0;
        property_size += self.authentication_method.calc_size();
        property_size += self.authentication_data.calc_size();
        property_size += self.reason_string.calc_size();
        property_size += self.user_properties.calc_size();

        property_size
    }
}

#[derive(Debug, PartialEq)]
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

    pub fn calculate_size(&self) -> u32 {
        self.calc_size()
    }
}

impl PacketSize for Packet {
    fn calc_size(&self) -> u32 {
        match self {
            Packet::Connect(p) => {
                let mut size = p.protocol_name.calc_size();

                // Protocol level + connect flags + keep-alive
                size += 1 + 1 + 2;

                let property_size = p.property_size();
                size += property_size + VariableByteInt(property_size).calc_size();

                size += p.client_id.calc_size();
                size += p.will.calc_size();
                size += p.user_name.calc_size();
                size += p.password.calc_size();

                size
            },
            Packet::ConnectAck(p) => {
                // flags + reason code
                let mut size = 1 + 1;

                let property_size = p.property_size();
                size += property_size + VariableByteInt(property_size).calc_size();

                size
            },
            Packet::Publish(p) => {
                let mut size = p.topic_name.calc_size();
                size += p.packet_id.calc_size();

                let property_size = p.property_size();
                size += property_size + VariableByteInt(property_size).calc_size();

                // This payload does not have a length prefix
                size += p.payload.len() as u32;

                size
            },
            Packet::PublishAck(p) => {
                // packet_id + reason_code
                let mut size = 2 + 1;

                let property_size = p.property_size();
                size += property_size + VariableByteInt(property_size).calc_size();

                size
            },
            Packet::PublishReceived(p) => {
                // packet_id + reason_code
                let mut size = 2 + 1;

                let property_size = p.property_size();
                size += property_size + VariableByteInt(property_size).calc_size();

                size
            },
            Packet::PublishRelease(p) => {
                // packet_id + reason_code
                let mut size = 2 + 1;

                let property_size = p.property_size();
                size += property_size + VariableByteInt(property_size).calc_size();

                size
            },
            Packet::PublishComplete(p) => {
                // packet_id + reason_code
                let mut size = 2 + 1;

                let property_size = p.property_size();
                size += property_size + VariableByteInt(property_size).calc_size();

                size
            },
            Packet::Subscribe(p) => {
                // packet_id
                let mut size = 2;

                let property_size = p.property_size();
                size += property_size + VariableByteInt(property_size).calc_size();

                size += p.subscription_topics.calc_size();

                size
            },
            Packet::SubscribeAck(p) => {
                // Packet id
                let mut size = 2;

                let property_size = p.property_size();
                size += property_size + VariableByteInt(property_size).calc_size();

                size += p.reason_codes.len() as u32;

                size
            },
            Packet::Unsubscribe(p) => {
                // Packet id
                let mut size = 2;

                let property_size = p.property_size();
                size += property_size + VariableByteInt(property_size).calc_size();

                size += p.topics.calc_size();

                size
            },
            Packet::UnsubscribeAck(p) => {
                // Packet id
                let mut size = 2;

                let property_size = p.property_size();
                size += property_size + VariableByteInt(property_size).calc_size();

                size += p.reason_codes.len() as u32;

                size
            },
            Packet::PingRequest => 0,
            Packet::PingResponse => 0,
            Packet::Disconnect(p) => {
                // reason_code
                let mut size = 1;

                let property_size = p.property_size();
                size += property_size + VariableByteInt(property_size).calc_size();

                size
            },
            Packet::Authenticate(p) => {
                // reason_code
                let mut size = 1;

                let property_size = p.property_size();
                size += property_size + VariableByteInt(property_size).calc_size();

                size
            },
        }
    }
}
