use crate::types::{
    properties::Property, ConnectAckPacket, DecodeError, Packet, SubscribeAckPacket,
};
use bytes::{BufMut, BytesMut};

fn encode_variable_int(value: u32, bytes: &mut BytesMut) -> usize {
    let mut x = value;
    let mut byte_counter = 0;

    loop {
        let mut encoded_byte: u8 = (x % 128) as u8;
        x /= 128;

        if x > 0 {
            encoded_byte |= 128;
        }

        bytes.put_u8(encoded_byte);

        byte_counter += 1;

        if x == 0 {
            break;
        }
    }

    byte_counter
}

fn encode_string(value: &str, bytes: &mut BytesMut) {
    bytes.put_u16(value.len() as u16);
    bytes.put_slice(value.as_bytes());
}

fn encode_binary_data(value: &[u8], bytes: &mut BytesMut) {
    bytes.put_u16(value.len() as u16);
    bytes.put_slice(value);
}

fn encode_property(property: &Property, bytes: &mut BytesMut) {
    use crate::types::properties::*;

    let property_id = property.property_type() as u32;
    encode_variable_int(property_id, bytes);

    match property {
        Property::PayloadFormatIndicator(PayloadFormatIndicator(p)) => {
            bytes.put_u8(*p);
        },
        Property::MessageExpiryInterval(MessageExpiryInterval(p)) => {
            bytes.put_u32(*p);
        },
        Property::ContentType(ContentType(p)) => {
            encode_string(p, bytes);
        },
        Property::ResponseTopic(ResponseTopic(p)) => {
            encode_string(p, bytes);
        },
        Property::CorrelationData(CorrelationData(p)) => {
            encode_binary_data(p, bytes);
        },
        Property::SubscriptionIdentifier(SubscriptionIdentifier(p)) => {
            encode_variable_int(p.0, bytes);
        },
        Property::SessionExpiryInterval(SessionExpiryInterval(p)) => {
            bytes.put_u32(*p);
        },
        Property::AssignedClientIdentifier(AssignedClientIdentifier(p)) => {
            encode_string(p, bytes);
        },
        Property::ServerKeepAlive(ServerKeepAlive(p)) => bytes.put_u16(*p),
        Property::AuthenticationMethod(AuthenticationMethod(p)) => {
            encode_string(p, bytes);
        },
        Property::AuthenticationData(AuthenticationData(p)) => {
            encode_binary_data(p, bytes);
        },
        Property::RequestProblemInformation(RequestProblemInformation(p)) => {
            bytes.put_u8(*p);
        },
        Property::WillDelayInterval(WillDelayInterval(p)) => {
            bytes.put_u32(*p);
        },
        Property::RequestResponseInformation(RequestResponseInformation(p)) => {
            bytes.put_u8(*p);
        },
        Property::ResponseInformation(ResponseInformation(p)) => {
            encode_string(p, bytes);
        },
        Property::ServerReference(ServerReference(p)) => {
            encode_string(p, bytes);
        },
        Property::ReasonString(ReasonString(p)) => {
            encode_string(p, bytes);
        },
        Property::ReceiveMaximum(ReceiveMaximum(p)) => {
            bytes.put_u16(*p);
        },
        Property::TopicAliasMaximum(TopicAliasMaximum(p)) => {
            bytes.put_u16(*p);
        },
        Property::TopicAlias(TopicAlias(p)) => {
            bytes.put_u16(*p);
        },
        Property::MaximumQos(MaximumQos(p)) => {
            bytes.put_u8(*p as u8);
        },
        Property::RetainAvailable(RetainAvailable(p)) => {
            bytes.put_u8(*p);
        },
        Property::UserProperty(UserProperty(k, v)) => {
            encode_string(k, bytes);
            encode_string(v, bytes);
        },
        Property::MaximumPacketSize(MaximumPacketSize(p)) => {
            bytes.put_u32(*p);
        },
        Property::WildcardSubscriptionAvailable(WildcardSubscriptionAvailable(p)) => {
            bytes.put_u8(*p);
        },
        Property::SubscriptionIdentifierAvailable(SubscriptionIdentifierAvailable(p)) => {
            bytes.put_u8(*p);
        },
        Property::SharedSubscriptionAvailable(SharedSubscriptionAvailable(p)) => {
            bytes.put_u8(*p);
        },
    }
}

fn encode_connect_ack(packet: &ConnectAckPacket, bytes: &mut BytesMut) -> Result<(), DecodeError> {
    let mut connect_ack_flags = 0b0000_0000;
    if packet.session_present {
        connect_ack_flags |= 0b0000_0001;
    }

    bytes.put_u8(connect_ack_flags);

    // TODO - replace with actual reason code
    let reason_code = 0;
    bytes.put_u8(reason_code);

    // TODO - replace with actual property length
    let property_length = 0;
    encode_variable_int(property_length, bytes);

    Ok(())
}

fn encode_subscribe_ack(
    packet: &SubscribeAckPacket,
    bytes: &mut BytesMut,
) -> Result<(), DecodeError> {
    bytes.put_u16(packet.packet_id);

    // TODO - replace with actual property length
    let property_length = 0;
    encode_variable_int(property_length, bytes);

    for code in &packet.reason_codes {
        bytes.put_u8((*code) as u8);
    }

    Ok(())
}

pub fn encode_mqtt(packet: &Packet, bytes: &mut BytesMut) -> Result<(), DecodeError> {
    // TODO - reserve the exact amount
    // bytes.reserve(packet_size);
    let first_byte = packet.to_byte();
    let mut first_byte_val = (first_byte << 4) & 0b1111_0000;
    first_byte_val |= packet.fixed_header_flags();

    bytes.put_u8(first_byte_val);

    let remaining_length = packet.calculate_size();
    encode_variable_int(remaining_length as u32, bytes);

    match packet {
        Packet::ConnectAck(p) => encode_connect_ack(p, bytes)?,
        Packet::SubscribeAck(p) => encode_subscribe_ack(p, bytes)?,
        _ => {},
    }

    Ok(())
}
