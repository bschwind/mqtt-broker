use crate::types::{
    properties::*, ConnectAckPacket, ConnectPacket, Encode, Packet, PropertySize,
    SubscribeAckPacket,
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

impl Encode for PayloadFormatIndicator {
    fn encode(&self, bytes: &mut BytesMut) {
        bytes.put_u8(self.0);
    }
}
impl Encode for MessageExpiryInterval {
    fn encode(&self, bytes: &mut BytesMut) {
        bytes.put_u32(self.0);
    }
}
impl Encode for ContentType {
    fn encode(&self, bytes: &mut BytesMut) {
        encode_string(&self.0, bytes);
    }
}
impl Encode for ResponseTopic {
    fn encode(&self, bytes: &mut BytesMut) {
        encode_string(&self.0, bytes);
    }
}
impl Encode for CorrelationData {
    fn encode(&self, bytes: &mut BytesMut) {
        encode_binary_data(&self.0, bytes);
    }
}
impl Encode for SubscriptionIdentifier {
    fn encode(&self, bytes: &mut BytesMut) {
        encode_variable_int((self.0).0, bytes);
    }
}
impl Encode for SessionExpiryInterval {
    fn encode(&self, bytes: &mut BytesMut) {
        bytes.put_u32(self.0);
    }
}
impl Encode for AssignedClientIdentifier {
    fn encode(&self, bytes: &mut BytesMut) {
        encode_string(&self.0, bytes);
    }
}
impl Encode for ServerKeepAlive {
    fn encode(&self, bytes: &mut BytesMut) {
        bytes.put_u16(self.0)
    }
}
impl Encode for AuthenticationMethod {
    fn encode(&self, bytes: &mut BytesMut) {
        encode_string(&self.0, bytes);
    }
}
impl Encode for AuthenticationData {
    fn encode(&self, bytes: &mut BytesMut) {
        encode_binary_data(&self.0, bytes);
    }
}
impl Encode for RequestProblemInformation {
    fn encode(&self, bytes: &mut BytesMut) {
        bytes.put_u8(self.0);
    }
}
impl Encode for WillDelayInterval {
    fn encode(&self, bytes: &mut BytesMut) {
        bytes.put_u32(self.0);
    }
}
impl Encode for RequestResponseInformation {
    fn encode(&self, bytes: &mut BytesMut) {
        bytes.put_u8(self.0);
    }
}
impl Encode for ResponseInformation {
    fn encode(&self, bytes: &mut BytesMut) {
        encode_string(&self.0, bytes);
    }
}
impl Encode for ServerReference {
    fn encode(&self, bytes: &mut BytesMut) {
        encode_string(&self.0, bytes);
    }
}
impl Encode for ReasonString {
    fn encode(&self, bytes: &mut BytesMut) {
        encode_string(&self.0, bytes);
    }
}
impl Encode for ReceiveMaximum {
    fn encode(&self, bytes: &mut BytesMut) {
        bytes.put_u16(self.0);
    }
}
impl Encode for TopicAliasMaximum {
    fn encode(&self, bytes: &mut BytesMut) {
        bytes.put_u16(self.0);
    }
}
impl Encode for TopicAlias {
    fn encode(&self, bytes: &mut BytesMut) {
        bytes.put_u16(self.0);
    }
}
impl Encode for MaximumQos {
    fn encode(&self, bytes: &mut BytesMut) {
        bytes.put_u8(self.0 as u8);
    }
}
impl Encode for RetainAvailable {
    fn encode(&self, bytes: &mut BytesMut) {
        bytes.put_u8(self.0);
    }
}
impl Encode for UserProperty {
    fn encode(&self, bytes: &mut BytesMut) {
        encode_string(&self.0, bytes);
        encode_string(&self.1, bytes);
    }
}
impl Encode for MaximumPacketSize {
    fn encode(&self, bytes: &mut BytesMut) {
        bytes.put_u32(self.0);
    }
}
impl Encode for WildcardSubscriptionAvailable {
    fn encode(&self, bytes: &mut BytesMut) {
        bytes.put_u8(self.0);
    }
}
impl Encode for SubscriptionIdentifierAvailable {
    fn encode(&self, bytes: &mut BytesMut) {
        bytes.put_u8(self.0);
    }
}
impl Encode for SharedSubscriptionAvailable {
    fn encode(&self, bytes: &mut BytesMut) {
        bytes.put_u8(self.0);
    }
}

fn encode_connect(packet: &ConnectPacket, bytes: &mut BytesMut) {
    encode_string(&packet.protocol_name, bytes);
    bytes.put_u8(packet.protocol_level);

    let mut connect_flags: u8 = 0b0000_0000;

    if packet.user_name.is_some() {
        connect_flags |= 0b1000_0000;
    }

    if packet.password.is_some() {
        connect_flags |= 0b0100_0000;
    }

    if let Some(will) = &packet.will {
        if will.should_retain {
            connect_flags |= 0b0100_0000;
        }

        let qos_byte: u8 = will.qos as u8;
        connect_flags |= (qos_byte & 0b0000_0011) << 3;
        connect_flags |= 0b0000_0100;
    }

    if packet.clean_start {
        connect_flags |= 0b0000_0010;
    }

    bytes.put_u8(connect_flags);
    bytes.put_u16(packet.keep_alive);

    let property_length = packet.property_size();
    encode_variable_int(property_length, bytes);

    packet.session_expiry_interval.encode(bytes);
    packet.receive_maximum.encode(bytes);
    packet.maximum_packet_size.encode(bytes);
    packet.topic_alias_maximum.encode(bytes);
    packet.request_response_information.encode(bytes);
    packet.request_problem_information.encode(bytes);
    packet.user_properties.encode(bytes);
    packet.authentication_method.encode(bytes);
    packet.authentication_data.encode(bytes);

    encode_string(&packet.client_id, bytes);

    if let Some(will) = &packet.will {
        let property_length = will.property_size();
        encode_variable_int(property_length, bytes);

        will.will_delay_interval.encode(bytes);
        will.payload_format_indicator.encode(bytes);
        will.message_expiry_interval.encode(bytes);
        will.content_type.encode(bytes);
        will.response_topic.encode(bytes);
        will.correlation_data.encode(bytes);
        will.user_properties.encode(bytes);

        encode_string(&will.topic, bytes);
        encode_binary_data(&will.payload, bytes);
    }

    if let Some(user_name) = &packet.user_name {
        encode_string(&user_name, bytes);
    }

    if let Some(password) = &packet.password {
        encode_string(&password, bytes);
    }
}

fn encode_connect_ack(packet: &ConnectAckPacket, bytes: &mut BytesMut) {
    let mut connect_ack_flags: u8 = 0b0000_0000;
    if packet.session_present {
        connect_ack_flags |= 0b0000_0001;
    }

    bytes.put_u8(connect_ack_flags);
    bytes.put_u8(packet.reason_code as u8);

    let property_length = packet.property_size();
    encode_variable_int(property_length, bytes);
}

fn encode_subscribe_ack(packet: &SubscribeAckPacket, bytes: &mut BytesMut) {
    bytes.put_u16(packet.packet_id);

    let property_length = packet.property_size();
    encode_variable_int(property_length, bytes);

    for code in &packet.reason_codes {
        bytes.put_u8((*code) as u8);
    }
}

pub fn encode_mqtt(packet: &Packet, bytes: &mut BytesMut) {
    // TODO - reserve the exact amount
    // bytes.reserve(packet_size);
    let first_byte = packet.to_byte();
    let mut first_byte_val = (first_byte << 4) & 0b1111_0000;
    first_byte_val |= packet.fixed_header_flags();

    bytes.put_u8(first_byte_val);

    let remaining_length = packet.calculate_size();
    encode_variable_int(remaining_length as u32, bytes);

    match packet {
        Packet::Connect(p) => encode_connect(p, bytes),
        Packet::ConnectAck(p) => encode_connect_ack(p, bytes),
        Packet::SubscribeAck(p) => encode_subscribe_ack(p, bytes),
        _ => {},
    }
}
