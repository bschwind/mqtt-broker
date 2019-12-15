use crate::types::{
    properties::*, ConnectPayload, ConnectVariableHeader, FinalWill, Packet, PacketType,
    ParseError, QoS,
};
use bytes::{Buf, BytesMut};
use std::{convert::TryFrom, io::Cursor};

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
        return_if_none!(parse_string($bytes)?)
    }};
}

macro_rules! read_binary_data {
    ($bytes: expr) => {{
        return_if_none!(parse_binary_data($bytes)?)
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
        return_if_none!(parse_property(property_id, $bytes)?)
    }};
}

fn decode_variable_int(bytes: &mut Cursor<&mut BytesMut>) -> Result<Option<u32>, ParseError> {
    let mut multiplier = 1;
    let mut value: u32 = 0;

    loop {
        let encoded_byte = read_u8!(bytes);

        value += ((encoded_byte & 0b0111_1111) as u32) * multiplier;
        multiplier *= 128;

        if multiplier > (128 * 128 * 128) {
            return Err(ParseError::InvalidRemainingLength);
        }

        if encoded_byte & 0b1000_0000 == 0b0000_0000 {
            break;
        }
    }

    Ok(Some(value))
}

fn encode_variable_int(value: u32, buf: &mut [u8]) -> usize {
    let mut x = value;
    let mut byte_counter = 0;

    loop {
        let mut encoded_byte: u8 = (x % 128) as u8;
        x /= 128;

        if x > 0 {
            encoded_byte |= 128;
        }

        buf[byte_counter] = encoded_byte;

        byte_counter += 1;

        if x == 0 {
            break;
        }
    }

    byte_counter
}

fn parse_string(bytes: &mut Cursor<&mut BytesMut>) -> Result<Option<String>, ParseError> {
    let str_size_bytes = read_u16!(bytes) as usize;

    require_length!(bytes, str_size_bytes);

    let position = bytes.position() as usize;

    // TODO - Use Cow<str> and from_utf8_lossy later for less copying
    match String::from_utf8(bytes.get_ref()[position..(position + str_size_bytes)].into()) {
        Ok(string) => {
            bytes.advance(str_size_bytes);
            Ok(Some(string))
        },
        Err(_) => Err(ParseError::InvalidUtf8),
    }
}

fn parse_binary_data(bytes: &mut Cursor<&mut BytesMut>) -> Result<Option<Vec<u8>>, ParseError> {
    let data_size_bytes = read_u16!(bytes) as usize;
    require_length!(bytes, data_size_bytes);

    let position = bytes.position() as usize;

    Ok(Some(bytes.get_ref()[position..(position + data_size_bytes)].into()))
}

fn parse_property(
    property_id: u32,
    bytes: &mut Cursor<&mut BytesMut>,
) -> Result<Option<Property>, ParseError> {
    match property_id {
        1 => {
            let format_indicator = read_u8!(bytes);
            Ok(Some(Property::PayloadFormatIndicator(PayloadFormatIndicator(format_indicator))))
        },
        2 => {
            let message_expiry_interval = read_u32!(bytes);
            Ok(Some(Property::MessageExpiryInterval(MessageExpiryInterval(
                message_expiry_interval,
            ))))
        },
        3 => {
            let content_type = read_string!(bytes);
            Ok(Some(Property::ContentType(ContentType(content_type))))
        },
        8 => {
            let response_topic = read_string!(bytes);
            Ok(Some(Property::RepsonseTopic(RepsonseTopic(response_topic))))
        },
        9 => {
            let correlation_data = read_binary_data!(bytes);
            Ok(Some(Property::CorrelationData(CorrelationData(correlation_data))))
        },
        11 => {
            let subscription_identifier = read_u32!(bytes);
            Ok(Some(Property::SubscriptionIdentifier(SubscriptionIdentifier(
                subscription_identifier,
            ))))
        },
        17 => {
            let session_expiry_interval = read_u32!(bytes);
            Ok(Some(Property::SessionExpiryInterval(SessionExpiryInterval(
                session_expiry_interval,
            ))))
        },
        18 => {
            let assigned_client_identifier = read_string!(bytes);
            Ok(Some(Property::AssignedClientIdentifier(AssignedClientIdentifier(
                assigned_client_identifier,
            ))))
        },
        19 => {
            let server_keep_alive = read_u16!(bytes);
            Ok(Some(Property::ServerKeepAlive(ServerKeepAlive(server_keep_alive))))
        },
        21 => {
            let authentication_method = read_string!(bytes);
            Ok(Some(Property::AuthenticationMethod(AuthenticationMethod(authentication_method))))
        },
        22 => {
            let authentication_data = read_binary_data!(bytes);
            Ok(Some(Property::AuthenticationData(AuthenticationData(authentication_data))))
        },
        23 => {
            let request_problem_information = read_u8!(bytes);
            Ok(Some(Property::RequestProblemInformation(RequestProblemInformation(
                request_problem_information,
            ))))
        },
        24 => {
            let will_delay_interval = read_u32!(bytes);
            Ok(Some(Property::WillDelayInterval(WillDelayInterval(will_delay_interval))))
        },
        25 => {
            let request_response_information = read_u8!(bytes);
            Ok(Some(Property::RequestResponseInformation(RequestResponseInformation(
                request_response_information,
            ))))
        },
        26 => {
            let response_information = read_string!(bytes);
            Ok(Some(Property::ResponseInformation(ResponseInformation(response_information))))
        },
        28 => {
            let server_reference = read_string!(bytes);
            Ok(Some(Property::ServerReference(ServerReference(server_reference))))
        },
        31 => {
            let reason_string = read_string!(bytes);
            Ok(Some(Property::ReasonString(ReasonString(reason_string))))
        },
        33 => {
            let receive_maximum = read_u16!(bytes);
            Ok(Some(Property::ReceiveMaximum(ReceiveMaximum(receive_maximum))))
        },
        34 => {
            let topic_alias_maximum = read_u16!(bytes);
            Ok(Some(Property::TopicAliasMaximum(TopicAliasMaximum(topic_alias_maximum))))
        },
        35 => {
            let topic_alias = read_u16!(bytes);
            Ok(Some(Property::TopicAlias(TopicAlias(topic_alias))))
        },
        36 => {
            let qos_byte = read_u8!(bytes);
            let qos = QoS::try_from(qos_byte)?;

            Ok(Some(Property::MaximumQos(MaximumQos(qos))))
        },
        37 => {
            let retain_available = read_u8!(bytes);
            Ok(Some(Property::RetainAvailable(RetainAvailable(retain_available))))
        },
        38 => {
            let (key, value) = read_string_pair!(bytes);
            Ok(Some(Property::UserProperty(UserProperty(key, value))))
        },
        39 => {
            let maximum_packet_size = read_u32!(bytes);
            Ok(Some(Property::MaximumPacketSize(MaximumPacketSize(maximum_packet_size))))
        },
        40 => {
            let wildcard_subscription_available = read_u8!(bytes);
            Ok(Some(Property::WildcardSubscriptionAvailable(WildcardSubscriptionAvailable(
                wildcard_subscription_available,
            ))))
        },
        41 => {
            let subscription_identifier_available = read_u8!(bytes);
            Ok(Some(Property::SubscriptionIdentifierAvailable(SubscriptionIdentifierAvailable(
                subscription_identifier_available,
            ))))
        },
        42 => {
            let shared_subscription_available = read_u8!(bytes);
            Ok(Some(Property::SharedSubscriptionAvailable(SharedSubscriptionAvailable(
                shared_subscription_available,
            ))))
        },
        _ => Err(ParseError::InvalidPropertyId),
    }
}

fn parse_properties<F: FnMut(Property)>(
    bytes: &mut Cursor<&mut BytesMut>,
    mut closure: F,
) -> Result<Option<()>, ParseError> {
    let property_length = read_variable_int!(bytes);

    if property_length == 0 {
        return Ok(Some(()));
    }

    let start_cursor_pos = bytes.position();

    loop {
        let cursor_pos = bytes.position();

        if cursor_pos - start_cursor_pos >= property_length as u64 {
            break;
        }

        let property = read_property!(bytes);
        closure(property);
    }

    Ok(Some(()))
}

fn parse_variable_header(
    packet_type: &PacketType,
    bytes: &mut Cursor<&mut BytesMut>,
) -> Result<Option<ConnectVariableHeader>, ParseError> {
    match packet_type {
        PacketType::Connect => {
            let protocol_name = read_string!(bytes);
            let protocol_level = read_u8!(bytes);
            let connect_flags = read_u8!(bytes);
            let keep_alive = read_u16!(bytes);

            let mut session_expiry_interval = None;
            let mut receive_maximum = None;
            let mut maximum_packet_size = None;
            let mut topic_alias_maximum = None;
            let mut request_response_information = None;
            let mut request_problem_information = None;
            let mut user_properties = vec![];
            let mut authentication_method = None;
            let mut authentication_data = None;

            return_if_none!(parse_properties(bytes, |property| {
                match property {
                    Property::SessionExpiryInterval(p) => session_expiry_interval = Some(p),
                    Property::ReceiveMaximum(p) => receive_maximum = Some(p),
                    Property::MaximumPacketSize(p) => maximum_packet_size = Some(p),
                    Property::TopicAliasMaximum(p) => topic_alias_maximum = Some(p),
                    Property::RequestResponseInformation(p) => {
                        request_response_information = Some(p)
                    },
                    Property::RequestProblemInformation(p) => request_problem_information = Some(p),
                    Property::UserProperty(p) => user_properties.push(p),
                    Property::AuthenticationMethod(p) => authentication_method = Some(p),
                    Property::AuthenticationData(p) => authentication_data = Some(p),
                    _ => {}, // Invalid property for packet
                }
            })?);

            // Start payload
            let clean_start = 0b0000_0010 & connect_flags == 0b0000_0010;
            let has_will = 0b0000_0100 & connect_flags == 0b0000_0100;
            let will_qos_val = (0b0001_1000 & connect_flags) >> 3;
            let will_qos = QoS::try_from(will_qos_val)?;
            let retain_will = 0b0010_0000 & connect_flags == 0b0010_0000;
            let has_password = 0b0100_0000 & connect_flags == 0b0100_0000;
            let has_user_name = 0b1000_0000 & connect_flags == 0b1000_0000;

            let client_id = read_string!(bytes);
            let mut will = None;

            if has_will {
                let mut will_delay_interval = None;
                let mut payload_format_indicator = None;
                let mut message_expiry_interval = None;
                let mut content_type = None;
                let mut response_topic = None;
                let mut correlation_data = None;
                let mut user_properties = vec![];

                return_if_none!(parse_properties(bytes, |property| {
                    match property {
                        Property::WillDelayInterval(p) => will_delay_interval = Some(p),
                        Property::PayloadFormatIndicator(p) => payload_format_indicator = Some(p),
                        Property::MessageExpiryInterval(p) => message_expiry_interval = Some(p),
                        Property::ContentType(p) => content_type = Some(p),
                        Property::RepsonseTopic(p) => response_topic = Some(p),
                        Property::CorrelationData(p) => correlation_data = Some(p),
                        Property::UserProperty(p) => user_properties.push(p),
                        _ => {}, // Invalid property for packet
                    }
                })?);

                let topic = read_string!(bytes);
                let payload = read_binary_data!(bytes);

                will = Some(FinalWill {
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
                });
            }

            let mut user_name = None;
            let mut password = None;

            if has_user_name {
                user_name = Some(read_string!(bytes));
            }

            if has_password {
                password = Some(read_string!(bytes));
            }

            let connect_payload = ConnectPayload { client_id, will, user_name, password };

            dbg!(connect_payload);

            // End payload

            Ok(Some(ConnectVariableHeader {
                protocol_name,
                protocol_level,
                connect_flags,
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
            }))
        },
        _ => Ok(None),
    }
}

pub fn parse_mqtt(bytes: &mut BytesMut) -> Result<Option<Packet>, ParseError> {
    let mut bytes = Cursor::new(bytes);

    let first_byte = read_u8!(bytes);

    let first_byte_val = (first_byte & 0b1111_0000) >> 4;
    let packet_type = PacketType::try_from(first_byte_val)?;
    let remaining_packet_length = read_variable_int!(&mut bytes);

    let cursor_pos = bytes.position() as usize;
    let remaining_buffer_amount = bytes.get_ref().len() - cursor_pos;

    if remaining_buffer_amount < remaining_packet_length as usize {
        // If we don't have the full payload, just bail
        return Ok(None);
    }

    // TODO - use return_if_none! here after finishing parse_variable_header function
    let variable_header = parse_variable_header(&packet_type, &mut bytes)?;

    println!("variable_header: {:?}", variable_header);

    let cursor_pos = bytes.position() as usize;
    let bytes = bytes.into_inner();

    let rest = bytes.split_to(cursor_pos);
    let packet = Packet::new(packet_type, &rest);

    Ok(Some(packet))
}
