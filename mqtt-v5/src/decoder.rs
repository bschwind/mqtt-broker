use crate::{
    topic::{Topic, TopicParseError},
    types::{
        properties::*, AuthenticatePacket, AuthenticateReason, ConnectAckPacket, ConnectPacket,
        ConnectReason, DecodeError, DisconnectPacket, DisconnectReason, FinalWill, Packet,
        PacketType, ProtocolVersion, PublishAckPacket, PublishAckReason, PublishCompletePacket,
        PublishCompleteReason, PublishPacket, PublishReceivedPacket, PublishReceivedReason,
        PublishReleasePacket, PublishReleaseReason, QoS, RetainHandling, SubscribeAckPacket,
        SubscribeAckReason, SubscribePacket, SubscriptionTopic, UnsubscribeAckPacket,
        UnsubscribeAckReason, UnsubscribePacket, VariableByteInt,
    },
};
use bytes::{Buf, Bytes, BytesMut};
use std::{convert::TryFrom, io::Cursor, str::FromStr};

macro_rules! return_if_none {
    ($x: expr) => {{
        let string_opt = $x;
        if string_opt.is_none() {
            return Ok(None);
        }

        string_opt.unwrap()
    }};
}

macro_rules! require_length {
    ($bytes: expr, $len: expr) => {{
        if $bytes.remaining() < $len {
            return Ok(None);
        }
    }};
}

macro_rules! read_u8 {
    ($bytes: expr) => {{
        if !$bytes.has_remaining() {
            return Ok(None);
        }

        $bytes.get_u8()
    }};
}

macro_rules! read_u16 {
    ($bytes: expr) => {{
        if $bytes.remaining() < 2 {
            return Ok(None);
        }

        $bytes.get_u16()
    }};
}

macro_rules! read_u32 {
    ($bytes: expr) => {{
        if $bytes.remaining() < 4 {
            return Ok(None);
        }

        $bytes.get_u32()
    }};
}

macro_rules! read_variable_int {
    ($bytes: expr) => {{
        return_if_none!(decode_variable_int($bytes)?)
    }};
}

macro_rules! read_string {
    ($bytes: expr) => {{
        return_if_none!(decode_string($bytes)?)
    }};
}

macro_rules! read_binary_data {
    ($bytes: expr) => {{
        return_if_none!(decode_binary_data($bytes)?)
    }};
}

macro_rules! read_string_pair {
    ($bytes: expr) => {{
        let string_key = read_string!($bytes);
        let string_value = read_string!($bytes);

        (string_key, string_value)
    }};
}

macro_rules! read_property {
    ($bytes: expr) => {{
        let property_id = read_variable_int!($bytes);
        return_if_none!(decode_property(property_id, $bytes)?)
    }};
}

fn decode_variable_int(bytes: &mut Cursor<&mut BytesMut>) -> Result<Option<u32>, DecodeError> {
    let mut multiplier: u32 = 1;
    let mut value: u32 = 0;

    for _ in 0..4 {
        let encoded_byte = read_u8!(bytes);

        value += ((encoded_byte & 0b0111_1111) as u32) * multiplier;

        multiplier *= 128;

        if encoded_byte & 0b1000_0000 == 0b0000_0000 {
            return Ok(Some(value));
        }
    }

    Err(DecodeError::InvalidRemainingLength)
}

fn decode_string(bytes: &mut Cursor<&mut BytesMut>) -> Result<Option<String>, DecodeError> {
    let str_size_bytes = read_u16!(bytes) as usize;

    require_length!(bytes, str_size_bytes);

    let position = bytes.position() as usize;

    // TODO - Use Cow<str> and from_utf8_lossy later for less copying
    match String::from_utf8(bytes.get_ref()[position..(position + str_size_bytes)].into()) {
        Ok(string) => {
            bytes.advance(str_size_bytes);
            Ok(Some(string))
        },
        Err(_) => Err(DecodeError::InvalidUtf8),
    }
}

fn decode_binary_data(bytes: &mut Cursor<&mut BytesMut>) -> Result<Option<Bytes>, DecodeError> {
    let data_size_bytes = read_u16!(bytes) as usize;
    require_length!(bytes, data_size_bytes);

    let position = bytes.position() as usize;

    let payload_bytes =
        BytesMut::from(&bytes.get_ref()[position..(position + data_size_bytes)]).freeze();
    let result = Ok(Some(payload_bytes));
    bytes.advance(data_size_bytes);

    result
}

fn decode_binary_data_with_size(
    bytes: &mut Cursor<&mut BytesMut>,
    size: usize,
) -> Result<Option<Bytes>, DecodeError> {
    require_length!(bytes, size);

    let position = bytes.position() as usize;
    let payload_bytes = BytesMut::from(&bytes.get_ref()[position..(position + size)]).freeze();
    let result = Ok(Some(payload_bytes));
    bytes.advance(size);

    result
}

fn decode_property(
    property_id: u32,
    bytes: &mut Cursor<&mut BytesMut>,
) -> Result<Option<Property>, DecodeError> {
    let property_type =
        PropertyType::try_from(property_id).map_err(|_| DecodeError::InvalidPropertyId)?;

    match property_type {
        PropertyType::PayloadFormatIndicator => {
            let format_indicator = read_u8!(bytes);
            Ok(Some(Property::PayloadFormatIndicator(PayloadFormatIndicator(format_indicator))))
        },
        PropertyType::MessageExpiryInterval => {
            let message_expiry_interval = read_u32!(bytes);
            Ok(Some(Property::MessageExpiryInterval(MessageExpiryInterval(
                message_expiry_interval,
            ))))
        },
        PropertyType::ContentType => {
            let content_type = read_string!(bytes);
            Ok(Some(Property::ContentType(ContentType(content_type))))
        },
        PropertyType::ResponseTopic => {
            let response_topic = read_string!(bytes);
            Ok(Some(Property::ResponseTopic(ResponseTopic(response_topic))))
        },
        PropertyType::CorrelationData => {
            let correlation_data = read_binary_data!(bytes);
            Ok(Some(Property::CorrelationData(CorrelationData(correlation_data))))
        },
        PropertyType::SubscriptionIdentifier => {
            let subscription_identifier = read_variable_int!(bytes);
            Ok(Some(Property::SubscriptionIdentifier(SubscriptionIdentifier(VariableByteInt(
                subscription_identifier,
            )))))
        },
        PropertyType::SessionExpiryInterval => {
            let session_expiry_interval = read_u32!(bytes);
            Ok(Some(Property::SessionExpiryInterval(SessionExpiryInterval(
                session_expiry_interval,
            ))))
        },
        PropertyType::AssignedClientIdentifier => {
            let assigned_client_identifier = read_string!(bytes);
            Ok(Some(Property::AssignedClientIdentifier(AssignedClientIdentifier(
                assigned_client_identifier,
            ))))
        },
        PropertyType::ServerKeepAlive => {
            let server_keep_alive = read_u16!(bytes);
            Ok(Some(Property::ServerKeepAlive(ServerKeepAlive(server_keep_alive))))
        },
        PropertyType::AuthenticationMethod => {
            let authentication_method = read_string!(bytes);
            Ok(Some(Property::AuthenticationMethod(AuthenticationMethod(authentication_method))))
        },
        PropertyType::AuthenticationData => {
            let authentication_data = read_binary_data!(bytes);
            Ok(Some(Property::AuthenticationData(AuthenticationData(authentication_data))))
        },
        PropertyType::RequestProblemInformation => {
            let request_problem_information = read_u8!(bytes);
            Ok(Some(Property::RequestProblemInformation(RequestProblemInformation(
                request_problem_information,
            ))))
        },
        PropertyType::WillDelayInterval => {
            let will_delay_interval = read_u32!(bytes);
            Ok(Some(Property::WillDelayInterval(WillDelayInterval(will_delay_interval))))
        },
        PropertyType::RequestResponseInformation => {
            let request_response_information = read_u8!(bytes);
            Ok(Some(Property::RequestResponseInformation(RequestResponseInformation(
                request_response_information,
            ))))
        },
        PropertyType::ResponseInformation => {
            let response_information = read_string!(bytes);
            Ok(Some(Property::ResponseInformation(ResponseInformation(response_information))))
        },
        PropertyType::ServerReference => {
            let server_reference = read_string!(bytes);
            Ok(Some(Property::ServerReference(ServerReference(server_reference))))
        },
        PropertyType::ReasonString => {
            let reason_string = read_string!(bytes);
            Ok(Some(Property::ReasonString(ReasonString(reason_string))))
        },
        PropertyType::ReceiveMaximum => {
            let receive_maximum = read_u16!(bytes);
            Ok(Some(Property::ReceiveMaximum(ReceiveMaximum(receive_maximum))))
        },
        PropertyType::TopicAliasMaximum => {
            let topic_alias_maximum = read_u16!(bytes);
            Ok(Some(Property::TopicAliasMaximum(TopicAliasMaximum(topic_alias_maximum))))
        },
        PropertyType::TopicAlias => {
            let topic_alias = read_u16!(bytes);
            Ok(Some(Property::TopicAlias(TopicAlias(topic_alias))))
        },
        PropertyType::MaximumQos => {
            let qos_byte = read_u8!(bytes);
            let qos = QoS::try_from(qos_byte).map_err(|_| DecodeError::InvalidQoS)?;

            Ok(Some(Property::MaximumQos(MaximumQos(qos))))
        },
        PropertyType::RetainAvailable => {
            let retain_available = read_u8!(bytes);
            Ok(Some(Property::RetainAvailable(RetainAvailable(retain_available))))
        },
        PropertyType::UserProperty => {
            let (key, value) = read_string_pair!(bytes);
            Ok(Some(Property::UserProperty(UserProperty(key, value))))
        },
        PropertyType::MaximumPacketSize => {
            let maximum_packet_size = read_u32!(bytes);
            Ok(Some(Property::MaximumPacketSize(MaximumPacketSize(maximum_packet_size))))
        },
        PropertyType::WildcardSubscriptionAvailable => {
            let wildcard_subscription_available = read_u8!(bytes);
            Ok(Some(Property::WildcardSubscriptionAvailable(WildcardSubscriptionAvailable(
                wildcard_subscription_available,
            ))))
        },
        PropertyType::SubscriptionIdentifierAvailable => {
            let subscription_identifier_available = read_u8!(bytes);
            Ok(Some(Property::SubscriptionIdentifierAvailable(SubscriptionIdentifierAvailable(
                subscription_identifier_available,
            ))))
        },
        PropertyType::SharedSubscriptionAvailable => {
            let shared_subscription_available = read_u8!(bytes);
            Ok(Some(Property::SharedSubscriptionAvailable(SharedSubscriptionAvailable(
                shared_subscription_available,
            ))))
        },
    }
}

fn decode_properties<F: FnMut(Property)>(
    bytes: &mut Cursor<&mut BytesMut>,
    mut closure: F,
) -> Result<Option<()>, DecodeError> {
    try_decode_properties(bytes, |property| {
        closure(property);
        Ok(())
    })
}

fn try_decode_properties<F: FnMut(Property) -> Result<(), DecodeError>>(
    bytes: &mut Cursor<&mut BytesMut>,
    mut closure: F,
) -> Result<Option<()>, DecodeError> {
    let property_length = read_variable_int!(bytes);

    if property_length == 0 {
        return Ok(Some(()));
    }

    require_length!(bytes, property_length as usize);

    let start_cursor_pos = bytes.position();

    loop {
        let cursor_pos = bytes.position();

        if cursor_pos - start_cursor_pos >= property_length as u64 {
            break;
        }

        let property = read_property!(bytes);
        closure(property)?;
    }

    Ok(Some(()))
}

fn decode_connect(bytes: &mut Cursor<&mut BytesMut>) -> Result<Option<Packet>, DecodeError> {
    let protocol_name = read_string!(bytes);
    let protocol_level = read_u8!(bytes);
    let connect_flags = read_u8!(bytes);
    let keep_alive = read_u16!(bytes);

    let protocol_version = ProtocolVersion::try_from(protocol_level)
        .map_err(|_| DecodeError::InvalidProtocolVersion)?;

    let mut session_expiry_interval = None;
    let mut receive_maximum = None;
    let mut maximum_packet_size = None;
    let mut topic_alias_maximum = None;
    let mut request_response_information = None;
    let mut request_problem_information = None;
    let mut user_properties = vec![];
    let mut authentication_method = None;
    let mut authentication_data = None;

    if protocol_version == ProtocolVersion::V500 {
        return_if_none!(decode_properties(bytes, |property| {
            match property {
                Property::SessionExpiryInterval(p) => session_expiry_interval = Some(p),
                Property::ReceiveMaximum(p) => receive_maximum = Some(p),
                Property::MaximumPacketSize(p) => maximum_packet_size = Some(p),
                Property::TopicAliasMaximum(p) => topic_alias_maximum = Some(p),
                Property::RequestResponseInformation(p) => request_response_information = Some(p),
                Property::RequestProblemInformation(p) => request_problem_information = Some(p),
                Property::UserProperty(p) => user_properties.push(p),
                Property::AuthenticationMethod(p) => authentication_method = Some(p),
                Property::AuthenticationData(p) => authentication_data = Some(p),
                _ => {}, // Invalid property for packet
            }
        })?);
    }

    // Start payload
    let clean_start = connect_flags & 0b0000_0010 == 0b0000_0010;
    let has_will = connect_flags & 0b0000_0100 == 0b0000_0100;
    let will_qos_val = (connect_flags & 0b0001_1000) >> 3;
    let will_qos = QoS::try_from(will_qos_val).map_err(|_| DecodeError::InvalidQoS)?;
    let retain_will = connect_flags & 0b0010_0000 == 0b0010_0000;
    let has_password = connect_flags & 0b0100_0000 == 0b0100_0000;
    let has_user_name = connect_flags & 0b1000_0000 == 0b1000_0000;

    let client_id = read_string!(bytes);

    let will = if has_will {
        let mut will_delay_interval = None;
        let mut payload_format_indicator = None;
        let mut message_expiry_interval = None;
        let mut content_type = None;
        let mut response_topic = None;
        let mut correlation_data = None;
        let mut user_properties = vec![];

        if protocol_version == ProtocolVersion::V500 {
            return_if_none!(decode_properties(bytes, |property| {
                match property {
                    Property::WillDelayInterval(p) => will_delay_interval = Some(p),
                    Property::PayloadFormatIndicator(p) => payload_format_indicator = Some(p),
                    Property::MessageExpiryInterval(p) => message_expiry_interval = Some(p),
                    Property::ContentType(p) => content_type = Some(p),
                    Property::ResponseTopic(p) => response_topic = Some(p),
                    Property::CorrelationData(p) => correlation_data = Some(p),
                    Property::UserProperty(p) => user_properties.push(p),
                    _ => {}, // Invalid property for packet
                }
            })?);
        }

        let topic = Topic::from_str(read_string!(bytes).as_str())
            .map_err(|_| DecodeError::InvalidTopicFilter(TopicParseError::TopicTooLong))?;
        let payload = read_binary_data!(bytes);

        Some(FinalWill {
            topic,
            payload,
            qos: will_qos,
            should_retain: retain_will,
            will_delay_interval,
            payload_format_indicator,
            message_expiry_interval,
            content_type,
            response_topic,
            correlation_data,
            user_properties,
        })
    } else {
        None
    };

    let mut user_name = None;
    let mut password = None;

    if has_user_name {
        user_name = Some(read_string!(bytes));
    }

    if has_password {
        password = Some(read_string!(bytes));
    }

    let packet = ConnectPacket {
        protocol_name,
        protocol_version,
        clean_start,
        keep_alive,
        session_expiry_interval,
        receive_maximum,
        maximum_packet_size,
        topic_alias_maximum,
        request_response_information,
        request_problem_information,
        user_properties,
        authentication_method,
        authentication_data,
        client_id,
        will,
        user_name,
        password,
    };

    Ok(Some(Packet::Connect(packet)))
}

fn decode_connect_ack(
    bytes: &mut Cursor<&mut BytesMut>,
    protocol_version: ProtocolVersion,
) -> Result<Option<Packet>, DecodeError> {
    let flags = read_u8!(bytes);
    let session_present = (flags & 0b0000_0001) == 0b0000_0001;

    let reason_code_byte = read_u8!(bytes);
    let reason_code =
        ConnectReason::try_from(reason_code_byte).map_err(|_| DecodeError::InvalidConnectReason)?;

    let mut session_expiry_interval = None;
    let mut receive_maximum = None;
    let mut maximum_qos = None;
    let mut retain_available = None;
    let mut maximum_packet_size = None;
    let mut assigned_client_identifier = None;
    let mut topic_alias_maximum = None;
    let mut reason_string = None;
    let mut user_properties = vec![];
    let mut wildcard_subscription_available = None;
    let mut subscription_identifiers_available = None;
    let mut shared_subscription_available = None;
    let mut server_keep_alive = None;
    let mut response_information = None;
    let mut server_reference = None;
    let mut authentication_method = None;
    let mut authentication_data = None;

    if protocol_version == ProtocolVersion::V500 {
        return_if_none!(decode_properties(bytes, |property| {
            match property {
                Property::SessionExpiryInterval(p) => session_expiry_interval = Some(p),
                Property::ReceiveMaximum(p) => receive_maximum = Some(p),
                Property::MaximumQos(p) => maximum_qos = Some(p),
                Property::RetainAvailable(p) => retain_available = Some(p),
                Property::MaximumPacketSize(p) => maximum_packet_size = Some(p),
                Property::AssignedClientIdentifier(p) => assigned_client_identifier = Some(p),
                Property::TopicAliasMaximum(p) => topic_alias_maximum = Some(p),
                Property::ReasonString(p) => reason_string = Some(p),
                Property::UserProperty(p) => user_properties.push(p),
                Property::WildcardSubscriptionAvailable(p) => {
                    wildcard_subscription_available = Some(p)
                },
                Property::SubscriptionIdentifierAvailable(p) => {
                    subscription_identifiers_available = Some(p)
                },
                Property::SharedSubscriptionAvailable(p) => shared_subscription_available = Some(p),
                Property::ServerKeepAlive(p) => server_keep_alive = Some(p),
                Property::ResponseInformation(p) => response_information = Some(p),
                Property::ServerReference(p) => server_reference = Some(p),
                Property::AuthenticationMethod(p) => authentication_method = Some(p),
                Property::AuthenticationData(p) => authentication_data = Some(p),
                _ => {}, // Invalid property for packet
            }
        })?);
    }

    let packet = ConnectAckPacket {
        session_present,
        reason_code,
        session_expiry_interval,
        receive_maximum,
        maximum_qos,
        retain_available,
        maximum_packet_size,
        assigned_client_identifier,
        topic_alias_maximum,
        reason_string,
        user_properties,
        wildcard_subscription_available,
        subscription_identifiers_available,
        shared_subscription_available,
        server_keep_alive,
        response_information,
        server_reference,
        authentication_method,
        authentication_data,
    };

    Ok(Some(Packet::ConnectAck(packet)))
}

fn decode_publish(
    bytes: &mut Cursor<&mut BytesMut>,
    first_byte: u8,
    remaining_packet_length: u32,
    protocol_version: ProtocolVersion,
) -> Result<Option<Packet>, DecodeError> {
    let is_duplicate = (first_byte & 0b0000_1000) == 0b0000_1000;
    let qos_val = (first_byte & 0b0000_0110) >> 1;
    let qos = QoS::try_from(qos_val).map_err(|_| DecodeError::InvalidQoS)?;
    let retain = (first_byte & 0b0000_0001) == 0b0000_0001;

    // Variable header start
    let start_cursor_pos = bytes.position();

    let topic_str = read_string!(bytes);
    let topic = topic_str.parse().map_err(DecodeError::InvalidTopic)?;

    let packet_id = match qos {
        QoS::AtMostOnce => None,
        QoS::AtLeastOnce | QoS::ExactlyOnce => Some(read_u16!(bytes)),
    };

    let mut payload_format_indicator = None;
    let mut message_expiry_interval = None;
    let mut topic_alias = None;
    let mut response_topic = None;
    let mut correlation_data = None;
    let mut user_properties = vec![];
    let mut subscription_identifiers = None;
    let mut content_type = None;

    if protocol_version == ProtocolVersion::V500 {
        try_decode_properties(bytes, |property| match property {
            Property::PayloadFormatIndicator(p) => {
                payload_format_indicator = Some(p);
                Ok(())
            },
            Property::MessageExpiryInterval(p) => {
                message_expiry_interval = Some(p);
                Ok(())
            },
            Property::TopicAlias(p) => {
                topic_alias = Some(p);
                Ok(())
            },
            Property::ResponseTopic(p) => {
                response_topic = Some(p);
                Ok(())
            },
            Property::CorrelationData(p) => {
                correlation_data = Some(p);
                Ok(())
            },
            Property::UserProperty(p) => {
                user_properties.push(p);
                Ok(())
            },
            Property::SubscriptionIdentifier(SubscriptionIdentifier(VariableByteInt(0))) => {
                Err(DecodeError::InvalidSubscriptionIdentifier)
            },
            Property::SubscriptionIdentifier(p) => {
                subscription_identifiers.get_or_insert(Vec::new()).push(p);
                Ok(())
            },
            Property::ContentType(p) => {
                content_type = Some(p);
                Ok(())
            },
            _ => Err(DecodeError::InvalidPropertyForPacket),
        })?;
    }

    let end_cursor_pos = bytes.position();
    let variable_header_size = (end_cursor_pos - start_cursor_pos) as u32;
    // Variable header end

    if remaining_packet_length < variable_header_size {
        return Err(DecodeError::InvalidRemainingLength);
    }
    let payload_size = remaining_packet_length - variable_header_size;
    let payload = return_if_none!(decode_binary_data_with_size(bytes, payload_size as usize)?);
    let subscription_identifiers =
        subscription_identifiers.unwrap_or_else(|| Vec::with_capacity(0));

    let packet = PublishPacket {
        is_duplicate,
        qos,
        retain,

        topic,
        packet_id,

        payload_format_indicator,
        message_expiry_interval,
        topic_alias,
        response_topic,
        correlation_data,
        user_properties,
        subscription_identifiers,
        content_type,

        payload,
    };

    Ok(Some(Packet::Publish(packet)))
}

fn decode_publish_ack(
    bytes: &mut Cursor<&mut BytesMut>,
    remaining_packet_length: u32,
    protocol_version: ProtocolVersion,
) -> Result<Option<Packet>, DecodeError> {
    let packet_id = read_u16!(bytes);

    if remaining_packet_length == 2 {
        return Ok(Some(Packet::PublishAck(PublishAckPacket {
            packet_id,
            reason_code: PublishAckReason::Success,
            reason_string: None,
            user_properties: vec![],
        })));
    }

    let reason_code_byte = read_u8!(bytes);
    let reason_code = PublishAckReason::try_from(reason_code_byte)
        .map_err(|_| DecodeError::InvalidPublishAckReason)?;

    let mut reason_string = None;
    let mut user_properties = vec![];

    if protocol_version == ProtocolVersion::V500 && remaining_packet_length >= 4 {
        return_if_none!(decode_properties(bytes, |property| {
            match property {
                Property::ReasonString(p) => reason_string = Some(p),
                Property::UserProperty(p) => user_properties.push(p),
                _ => {}, // Invalid property for packet
            }
        })?);
    }

    let packet = PublishAckPacket { packet_id, reason_code, reason_string, user_properties };

    Ok(Some(Packet::PublishAck(packet)))
}

fn decode_publish_received(
    bytes: &mut Cursor<&mut BytesMut>,
    remaining_packet_length: u32,
    protocol_version: ProtocolVersion,
) -> Result<Option<Packet>, DecodeError> {
    let packet_id = read_u16!(bytes);

    if remaining_packet_length == 2 {
        return Ok(Some(Packet::PublishReceived(PublishReceivedPacket {
            packet_id,
            reason_code: PublishReceivedReason::Success,
            reason_string: None,
            user_properties: vec![],
        })));
    }

    let reason_code_byte = read_u8!(bytes);
    let reason_code = PublishReceivedReason::try_from(reason_code_byte)
        .map_err(|_| DecodeError::InvalidPublishReceivedReason)?;

    let mut reason_string = None;
    let mut user_properties = vec![];

    if protocol_version == ProtocolVersion::V500 && remaining_packet_length >= 4 {
        return_if_none!(decode_properties(bytes, |property| {
            match property {
                Property::ReasonString(p) => reason_string = Some(p),
                Property::UserProperty(p) => user_properties.push(p),
                _ => {}, // Invalid property for packet
            }
        })?);
    }

    let packet = PublishReceivedPacket { packet_id, reason_code, reason_string, user_properties };

    Ok(Some(Packet::PublishReceived(packet)))
}

fn decode_publish_release(
    bytes: &mut Cursor<&mut BytesMut>,
    remaining_packet_length: u32,
    protocol_version: ProtocolVersion,
) -> Result<Option<Packet>, DecodeError> {
    let packet_id = read_u16!(bytes);

    if remaining_packet_length == 2 {
        return Ok(Some(Packet::PublishRelease(PublishReleasePacket {
            packet_id,
            reason_code: PublishReleaseReason::Success,
            reason_string: None,
            user_properties: vec![],
        })));
    }

    let reason_code_byte = read_u8!(bytes);
    let reason_code = PublishReleaseReason::try_from(reason_code_byte)
        .map_err(|_| DecodeError::InvalidPublishReleaseReason)?;

    let mut reason_string = None;
    let mut user_properties = vec![];

    if protocol_version == ProtocolVersion::V500 && remaining_packet_length >= 4 {
        return_if_none!(decode_properties(bytes, |property| {
            match property {
                Property::ReasonString(p) => reason_string = Some(p),
                Property::UserProperty(p) => user_properties.push(p),
                _ => {}, // Invalid property for packet
            }
        })?);
    }

    let packet = PublishReleasePacket { packet_id, reason_code, reason_string, user_properties };

    Ok(Some(Packet::PublishRelease(packet)))
}

fn decode_publish_complete(
    bytes: &mut Cursor<&mut BytesMut>,
    remaining_packet_length: u32,
    protocol_version: ProtocolVersion,
) -> Result<Option<Packet>, DecodeError> {
    let packet_id = read_u16!(bytes);

    if remaining_packet_length == 2 {
        return Ok(Some(Packet::PublishComplete(PublishCompletePacket {
            packet_id,
            reason_code: PublishCompleteReason::Success,
            reason_string: None,
            user_properties: vec![],
        })));
    }

    let reason_code_byte = read_u8!(bytes);
    let reason_code = PublishCompleteReason::try_from(reason_code_byte)
        .map_err(|_| DecodeError::InvalidPublishCompleteReason)?;

    let mut reason_string = None;
    let mut user_properties = vec![];

    if protocol_version == ProtocolVersion::V500 && remaining_packet_length >= 4 {
        return_if_none!(decode_properties(bytes, |property| {
            match property {
                Property::ReasonString(p) => reason_string = Some(p),
                Property::UserProperty(p) => user_properties.push(p),
                _ => {}, // Invalid property for packet
            }
        })?);
    }

    let packet = PublishCompletePacket { packet_id, reason_code, reason_string, user_properties };

    Ok(Some(Packet::PublishComplete(packet)))
}

fn decode_subscribe(
    bytes: &mut Cursor<&mut BytesMut>,
    remaining_packet_length: u32,
    protocol_version: ProtocolVersion,
) -> Result<Option<Packet>, DecodeError> {
    let start_cursor_pos = bytes.position();

    let packet_id = read_u16!(bytes);

    let mut subscription_identifier = None;
    let mut user_properties = vec![];

    if protocol_version == ProtocolVersion::V500 {
        try_decode_properties(bytes, |property| {
            match property {
                // [MQTT-3.8.2.1.2] The subscription identifier is allowed exactly once
                Property::SubscriptionIdentifier(_) if subscription_identifier.is_some() => {
                    Err(DecodeError::InvalidSubscriptionIdentifier)
                },
                // [MQTT-3.8.2.1.2] The subscription identifier must not be 0
                Property::SubscriptionIdentifier(SubscriptionIdentifier(VariableByteInt(0))) => {
                    Err(DecodeError::InvalidSubscriptionIdentifier)
                },
                Property::SubscriptionIdentifier(p) => {
                    subscription_identifier = Some(p);
                    Ok(())
                },
                Property::UserProperty(p) => {
                    user_properties.push(p);
                    Ok(())
                },
                _ => Err(DecodeError::InvalidPropertyForPacket),
            }
        })?;
    }

    let variable_header_size = (bytes.position() - start_cursor_pos) as u32;
    if remaining_packet_length < variable_header_size {
        return Err(DecodeError::InvalidRemainingLength);
    }
    let payload_size = remaining_packet_length - variable_header_size;

    let mut subscription_topics = vec![];
    let mut bytes_read: usize = 0;

    loop {
        if bytes_read >= payload_size as usize {
            break;
        }

        let start_cursor_pos = bytes.position();

        let topic_filter_str = read_string!(bytes);
        let topic_filter = topic_filter_str.parse().map_err(DecodeError::InvalidTopicFilter)?;

        let options_byte = read_u8!(bytes);

        let maximum_qos_val = options_byte & 0b0000_0011;
        let maximum_qos = QoS::try_from(maximum_qos_val).map_err(|_| DecodeError::InvalidQoS)?;

        let retain_handling_val = (options_byte & 0b0011_0000) >> 4;
        let retain_handling = RetainHandling::try_from(retain_handling_val)
            .map_err(|_| DecodeError::InvalidRetainHandling)?;

        let retain_as_published = (options_byte & 0b0000_1000) == 0b0000_1000;
        let no_local = (options_byte & 0b0000_0100) == 0b0000_0100;

        let subscription_topic = SubscriptionTopic {
            topic_filter,
            maximum_qos,
            no_local,
            retain_as_published,
            retain_handling,
        };

        subscription_topics.push(subscription_topic);

        let end_cursor_pos = bytes.position();
        bytes_read += (end_cursor_pos - start_cursor_pos) as usize;
    }

    let packet = SubscribePacket {
        packet_id,
        subscription_identifier,
        user_properties,
        subscription_topics,
    };

    Ok(Some(Packet::Subscribe(packet)))
}

fn decode_subscribe_ack(
    bytes: &mut Cursor<&mut BytesMut>,
    remaining_packet_length: u32,
    protocol_version: ProtocolVersion,
) -> Result<Option<Packet>, DecodeError> {
    let start_cursor_pos = bytes.position();

    let packet_id = read_u16!(bytes);

    let mut reason_string = None;
    let mut user_properties = vec![];

    if protocol_version == ProtocolVersion::V500 {
        return_if_none!(decode_properties(bytes, |property| {
            match property {
                Property::ReasonString(p) => reason_string = Some(p),
                Property::UserProperty(p) => user_properties.push(p),
                _ => {}, // Invalid property for packet
            }
        })?);
    }

    let variable_header_size = (bytes.position() - start_cursor_pos) as u32;
    if remaining_packet_length < variable_header_size {
        return Err(DecodeError::InvalidRemainingLength);
    }
    let payload_size = remaining_packet_length - variable_header_size;

    let mut reason_codes = vec![];
    for _ in 0..payload_size {
        let next_byte = read_u8!(bytes);
        let reason_code = SubscribeAckReason::try_from(next_byte)
            .map_err(|_| DecodeError::InvalidSubscribeAckReason)?;
        reason_codes.push(reason_code);
    }

    let packet = SubscribeAckPacket { packet_id, reason_string, user_properties, reason_codes };

    Ok(Some(Packet::SubscribeAck(packet)))
}

fn decode_unsubscribe(
    bytes: &mut Cursor<&mut BytesMut>,
    remaining_packet_length: u32,
    protocol_version: ProtocolVersion,
) -> Result<Option<Packet>, DecodeError> {
    let start_cursor_pos = bytes.position();

    let packet_id = read_u16!(bytes);

    let mut user_properties = vec![];

    if protocol_version == ProtocolVersion::V500 {
        return_if_none!(decode_properties(bytes, |property| {
            if let Property::UserProperty(p) = property {
                user_properties.push(p);
            }
        })?);
    }

    let variable_header_size = (bytes.position() - start_cursor_pos) as u32;
    if remaining_packet_length < variable_header_size {
        return Err(DecodeError::InvalidRemainingLength);
    }
    let payload_size = remaining_packet_length - variable_header_size;

    let mut topic_filters = vec![];
    let mut bytes_read: usize = 0;

    loop {
        if bytes_read >= payload_size as usize {
            break;
        }

        let start_cursor_pos = bytes.position();

        let topic_filter_str = read_string!(bytes);
        let topic_filter = topic_filter_str.parse().map_err(DecodeError::InvalidTopicFilter)?;
        topic_filters.push(topic_filter);

        let end_cursor_pos = bytes.position();
        bytes_read += (end_cursor_pos - start_cursor_pos) as usize;
    }

    let packet = UnsubscribePacket { packet_id, user_properties, topic_filters };

    Ok(Some(Packet::Unsubscribe(packet)))
}

fn decode_unsubscribe_ack(
    bytes: &mut Cursor<&mut BytesMut>,
    remaining_packet_length: u32,
    protocol_version: ProtocolVersion,
) -> Result<Option<Packet>, DecodeError> {
    let start_cursor_pos = bytes.position();

    let packet_id = read_u16!(bytes);

    let mut reason_string = None;
    let mut user_properties = vec![];

    if protocol_version == ProtocolVersion::V500 {
        return_if_none!(decode_properties(bytes, |property| {
            match property {
                Property::ReasonString(p) => reason_string = Some(p),
                Property::UserProperty(p) => user_properties.push(p),
                _ => {}, // Invalid property for packet
            }
        })?);
    }

    let variable_header_size = (bytes.position() - start_cursor_pos) as u32;
    if remaining_packet_length < variable_header_size {
        return Err(DecodeError::InvalidRemainingLength);
    }
    let payload_size = remaining_packet_length - variable_header_size;

    let mut reason_codes = vec![];
    for _ in 0..payload_size {
        let next_byte = read_u8!(bytes);
        let reason_code = UnsubscribeAckReason::try_from(next_byte)
            .map_err(|_| DecodeError::InvalidUnsubscribeAckReason)?;
        reason_codes.push(reason_code);
    }

    let packet = UnsubscribeAckPacket { packet_id, reason_string, user_properties, reason_codes };

    Ok(Some(Packet::UnsubscribeAck(packet)))
}

fn decode_disconnect(
    bytes: &mut Cursor<&mut BytesMut>,
    remaining_packet_length: u32,
    protocol_version: ProtocolVersion,
) -> Result<Option<Packet>, DecodeError> {
    if remaining_packet_length == 0 {
        return Ok(Some(Packet::Disconnect(DisconnectPacket {
            reason_code: DisconnectReason::NormalDisconnection,
            session_expiry_interval: None,
            reason_string: None,
            user_properties: vec![],
            server_reference: None,
        })));
    }

    let reason_code_byte = read_u8!(bytes);
    let reason_code = DisconnectReason::try_from(reason_code_byte)
        .map_err(|_| DecodeError::InvalidDisconnectReason)?;

    let mut session_expiry_interval = None;
    let mut reason_string = None;
    let mut user_properties = vec![];
    let mut server_reference = None;

    if protocol_version == ProtocolVersion::V500 && remaining_packet_length >= 2 {
        return_if_none!(decode_properties(bytes, |property| {
            match property {
                Property::SessionExpiryInterval(p) => session_expiry_interval = Some(p),
                Property::ReasonString(p) => reason_string = Some(p),
                Property::UserProperty(p) => user_properties.push(p),
                Property::ServerReference(p) => server_reference = Some(p),
                _ => {}, // Invalid property for packet
            }
        })?);
    }

    let packet = DisconnectPacket {
        reason_code,
        session_expiry_interval,
        reason_string,
        user_properties,
        server_reference,
    };

    Ok(Some(Packet::Disconnect(packet)))
}

fn decode_authenticate(
    bytes: &mut Cursor<&mut BytesMut>,
    remaining_packet_length: u32,
    protocol_version: ProtocolVersion,
) -> Result<Option<Packet>, DecodeError> {
    if remaining_packet_length == 0 {
        return Ok(Some(Packet::Authenticate(AuthenticatePacket {
            reason_code: AuthenticateReason::Success,
            authentication_method: None,
            authentication_data: None,
            reason_string: None,
            user_properties: vec![],
        })));
    }

    let reason_code_byte = read_u8!(bytes);
    let reason_code = AuthenticateReason::try_from(reason_code_byte)
        .map_err(|_| DecodeError::InvalidAuthenticateReason)?;

    let mut authentication_method = None;
    let mut authentication_data = None;
    let mut reason_string = None;
    let mut user_properties = vec![];

    if protocol_version == ProtocolVersion::V500 && remaining_packet_length >= 2 {
        return_if_none!(decode_properties(bytes, |property| {
            match property {
                Property::AuthenticationMethod(p) => authentication_method = Some(p),
                Property::AuthenticationData(p) => authentication_data = Some(p),
                Property::ReasonString(p) => reason_string = Some(p),
                Property::UserProperty(p) => user_properties.push(p),
                _ => {}, // Invalid property for packet
            }
        })?);
    }

    let packet = AuthenticatePacket {
        reason_code,
        authentication_method,
        authentication_data,
        reason_string,
        user_properties,
    };

    Ok(Some(Packet::Authenticate(packet)))
}

fn decode_packet(
    protocol_version: ProtocolVersion,
    packet_type: &PacketType,
    bytes: &mut Cursor<&mut BytesMut>,
    remaining_packet_length: u32,
    first_byte: u8,
) -> Result<Option<Packet>, DecodeError> {
    match packet_type {
        PacketType::Connect => decode_connect(bytes),
        PacketType::ConnectAck => decode_connect_ack(bytes, protocol_version),
        PacketType::Publish => {
            decode_publish(bytes, first_byte, remaining_packet_length, protocol_version)
        },
        PacketType::PublishAck => {
            decode_publish_ack(bytes, remaining_packet_length, protocol_version)
        },
        PacketType::PublishReceived => {
            decode_publish_received(bytes, remaining_packet_length, protocol_version)
        },
        PacketType::PublishRelease => {
            decode_publish_release(bytes, remaining_packet_length, protocol_version)
        },
        PacketType::PublishComplete => {
            decode_publish_complete(bytes, remaining_packet_length, protocol_version)
        },
        PacketType::Subscribe => decode_subscribe(bytes, remaining_packet_length, protocol_version),
        PacketType::SubscribeAck => {
            decode_subscribe_ack(bytes, remaining_packet_length, protocol_version)
        },
        PacketType::Unsubscribe => {
            decode_unsubscribe(bytes, remaining_packet_length, protocol_version)
        },
        PacketType::UnsubscribeAck => {
            decode_unsubscribe_ack(bytes, remaining_packet_length, protocol_version)
        },
        PacketType::PingRequest => Ok(Some(Packet::PingRequest)),
        PacketType::PingResponse => Ok(Some(Packet::PingResponse)),
        PacketType::Disconnect => {
            decode_disconnect(bytes, remaining_packet_length, protocol_version)
        },
        PacketType::Authenticate => {
            decode_authenticate(bytes, remaining_packet_length, protocol_version)
        },
    }
}

pub fn decode_mqtt(
    bytes: &mut BytesMut,
    protocol_version: ProtocolVersion,
) -> Result<Option<Packet>, DecodeError> {
    let mut bytes = Cursor::new(bytes);
    let first_byte = read_u8!(bytes);

    let first_byte_val = (first_byte & 0b1111_0000) >> 4;
    let packet_type =
        PacketType::try_from(first_byte_val).map_err(|_| DecodeError::InvalidPacketType)?;
    let remaining_packet_length = read_variable_int!(&mut bytes);

    let cursor_pos = bytes.position() as usize;
    let remaining_buffer_amount = bytes.get_ref().len() - cursor_pos;

    if remaining_buffer_amount < remaining_packet_length as usize {
        // If we don't have the full payload, just bail
        return Ok(None);
    }

    let packet = return_if_none!(decode_packet(
        protocol_version,
        &packet_type,
        &mut bytes,
        remaining_packet_length,
        first_byte
    )?);

    let cursor_pos = bytes.position() as usize;
    let bytes = bytes.into_inner();

    let _rest = bytes.split_to(cursor_pos);

    Ok(Some(packet))
}

#[cfg(test)]
mod tests {
    use crate::{decoder::*, topic::TopicFilter, types::*};
    use bytes::BytesMut;

    #[test]
    fn test_invalid_remaining_length() {
        let mut bytes = BytesMut::new();
        bytes.extend_from_slice(&[136, 1, 0, 36, 0, 0]); // Discovered from fuzz test

        let _ = decode_mqtt(&mut bytes, ProtocolVersion::V500);
    }

    #[test]
    fn test_decode_variable_int() {
        // TODO - Maybe it would be better to add an abnormal system test.

        fn normal_test(encoded_variable_int: &[u8], expected_variable_int: u32) {
            let bytes = &mut BytesMut::new();
            bytes.extend_from_slice(encoded_variable_int);
            match decode_variable_int(&mut Cursor::new(bytes)) {
                Ok(val) => match val {
                    Some(get_variable_int) => assert_eq!(get_variable_int, expected_variable_int),
                    None => panic!("variable_int is None"),
                },
                Err(err) => panic!("Error decoding variable int: {:?}", err),
            }
        }

        // Digits 1
        normal_test(&[0x00], 0);
        normal_test(&[0x7F], 127);

        // Digits 2
        normal_test(&[0x80, 0x01], 128);
        normal_test(&[0xFF, 0x7F], 16383);

        // Digits 3
        normal_test(&[0x80, 0x80, 0x01], 16384);
        normal_test(&[0xFF, 0xFF, 0x7F], 2097151);

        // Digits 4
        normal_test(&[0x80, 0x80, 0x80, 0x01], 2097152);
        normal_test(&[0xFF, 0xFF, 0xFF, 0x7F], 268435455);
    }

    #[test]
    fn test_decode_subscribe() {
        // Subscribe packet *without* Subscription Identifier
        let mut without_subscription_identifier = BytesMut::from(
            [0x82, 0x0a, 0x00, 0x01, 0x00, 0x00, 0x04, 0x74, 0x65, 0x73, 0x74, 0x00].as_slice(),
        );
        let without_subscription_identifier_expected = Packet::Subscribe(SubscribePacket {
            packet_id: 1,
            subscription_identifier: None,
            user_properties: vec![],
            subscription_topics: vec![SubscriptionTopic {
                topic_filter: TopicFilter::Concrete { filter: "test".into(), level_count: 1 },
                maximum_qos: QoS::AtMostOnce,
                no_local: false,
                retain_as_published: false,
                retain_handling: RetainHandling::SendAtSubscribeTime,
            }],
        });
        let decoded = decode_mqtt(&mut without_subscription_identifier, ProtocolVersion::V500)
            .unwrap()
            .unwrap();
        assert_eq!(without_subscription_identifier_expected, decoded);

        // Subscribe packet with Subscription Identifier
        let mut packet = BytesMut::from(
            [0x82, 0x0c, 0xff, 0xf6, 0x02, 0x0b, 0x01, 0x00, 0x04, 0x74, 0x65, 0x73, 0x74, 0x02]
                .as_slice(),
        );
        let decoded = decode_mqtt(&mut packet, ProtocolVersion::V500).unwrap().unwrap();
        let with_subscription_identifier_expected = Packet::Subscribe(SubscribePacket {
            packet_id: 65526,
            subscription_identifier: Some(SubscriptionIdentifier(VariableByteInt(1))),
            user_properties: vec![],
            subscription_topics: vec![SubscriptionTopic {
                topic_filter: TopicFilter::Concrete { filter: "test".into(), level_count: 1 },
                maximum_qos: QoS::ExactlyOnce,
                no_local: false,
                retain_as_published: false,
                retain_handling: RetainHandling::SendAtSubscribeTime,
            }],
        });
        assert_eq!(with_subscription_identifier_expected, decoded);
    }
    #[test]
    fn test_decode_variable_int_crash() {
        let number: u32 = u32::MAX;
        let result = decode_variable_int(&mut Cursor::new(&mut BytesMut::from(
            number.to_be_bytes().as_slice(),
        )));

        assert!(matches!(result, Err(DecodeError::InvalidRemainingLength)));
    }
}
