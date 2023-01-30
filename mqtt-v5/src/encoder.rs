use crate::types::{
    properties::*, AuthenticatePacket, ConnectAckPacket, ConnectPacket, DisconnectPacket, Encode,
    Packet, PropertySize, ProtocolVersion, PublishAckPacket, PublishCompletePacket, PublishPacket,
    PublishReceivedPacket, PublishReleasePacket, SubscribeAckPacket, SubscribePacket,
    UnsubscribeAckPacket, UnsubscribePacket, VariableByteInt,
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
        bytes.put_u8(PropertyType::PayloadFormatIndicator as u8);
        bytes.put_u8(self.0);
    }
}
impl Encode for MessageExpiryInterval {
    fn encode(&self, bytes: &mut BytesMut) {
        bytes.put_u8(PropertyType::MessageExpiryInterval as u8);
        bytes.put_u32(self.0);
    }
}
impl Encode for ContentType {
    fn encode(&self, bytes: &mut BytesMut) {
        bytes.put_u8(PropertyType::ContentType as u8);
        encode_string(&self.0, bytes);
    }
}
impl Encode for ResponseTopic {
    fn encode(&self, bytes: &mut BytesMut) {
        bytes.put_u8(PropertyType::ResponseTopic as u8);
        encode_string(&self.0, bytes);
    }
}
impl Encode for CorrelationData {
    fn encode(&self, bytes: &mut BytesMut) {
        bytes.put_u8(PropertyType::CorrelationData as u8);
        encode_binary_data(&self.0, bytes);
    }
}
impl Encode for SubscriptionIdentifier {
    fn encode(&self, bytes: &mut BytesMut) {
        bytes.put_u8(PropertyType::SubscriptionIdentifier as u8);
        encode_variable_int((self.0).0, bytes);
    }
}
impl Encode for SessionExpiryInterval {
    fn encode(&self, bytes: &mut BytesMut) {
        bytes.put_u8(PropertyType::SessionExpiryInterval as u8);
        bytes.put_u32(self.0);
    }
}
impl Encode for AssignedClientIdentifier {
    fn encode(&self, bytes: &mut BytesMut) {
        bytes.put_u8(PropertyType::AssignedClientIdentifier as u8);
        encode_string(&self.0, bytes);
    }
}
impl Encode for ServerKeepAlive {
    fn encode(&self, bytes: &mut BytesMut) {
        bytes.put_u8(PropertyType::ServerKeepAlive as u8);
        bytes.put_u16(self.0)
    }
}
impl Encode for AuthenticationMethod {
    fn encode(&self, bytes: &mut BytesMut) {
        bytes.put_u8(PropertyType::AuthenticationMethod as u8);
        encode_string(&self.0, bytes);
    }
}
impl Encode for AuthenticationData {
    fn encode(&self, bytes: &mut BytesMut) {
        bytes.put_u8(PropertyType::AuthenticationData as u8);
        encode_binary_data(&self.0, bytes);
    }
}
impl Encode for RequestProblemInformation {
    fn encode(&self, bytes: &mut BytesMut) {
        bytes.put_u8(PropertyType::RequestProblemInformation as u8);
        bytes.put_u8(self.0);
    }
}
impl Encode for WillDelayInterval {
    fn encode(&self, bytes: &mut BytesMut) {
        bytes.put_u8(PropertyType::WillDelayInterval as u8);
        bytes.put_u32(self.0);
    }
}
impl Encode for RequestResponseInformation {
    fn encode(&self, bytes: &mut BytesMut) {
        bytes.put_u8(PropertyType::RequestResponseInformation as u8);
        bytes.put_u8(self.0);
    }
}
impl Encode for ResponseInformation {
    fn encode(&self, bytes: &mut BytesMut) {
        bytes.put_u8(PropertyType::ResponseInformation as u8);
        encode_string(&self.0, bytes);
    }
}
impl Encode for ServerReference {
    fn encode(&self, bytes: &mut BytesMut) {
        bytes.put_u8(PropertyType::ServerReference as u8);
        encode_string(&self.0, bytes);
    }
}
impl Encode for ReasonString {
    fn encode(&self, bytes: &mut BytesMut) {
        bytes.put_u8(PropertyType::ReasonString as u8);
        encode_string(&self.0, bytes);
    }
}
impl Encode for ReceiveMaximum {
    fn encode(&self, bytes: &mut BytesMut) {
        bytes.put_u8(PropertyType::ReceiveMaximum as u8);
        bytes.put_u16(self.0);
    }
}
impl Encode for TopicAliasMaximum {
    fn encode(&self, bytes: &mut BytesMut) {
        bytes.put_u8(PropertyType::TopicAliasMaximum as u8);
        bytes.put_u16(self.0);
    }
}
impl Encode for TopicAlias {
    fn encode(&self, bytes: &mut BytesMut) {
        bytes.put_u8(PropertyType::TopicAlias as u8);
        bytes.put_u16(self.0);
    }
}
impl Encode for MaximumQos {
    fn encode(&self, bytes: &mut BytesMut) {
        bytes.put_u8(PropertyType::MaximumQos as u8);
        bytes.put_u8(self.0 as u8);
    }
}
impl Encode for RetainAvailable {
    fn encode(&self, bytes: &mut BytesMut) {
        bytes.put_u8(PropertyType::RetainAvailable as u8);
        bytes.put_u8(self.0);
    }
}
impl Encode for UserProperty {
    fn encode(&self, bytes: &mut BytesMut) {
        bytes.put_u8(PropertyType::UserProperty as u8);
        encode_string(&self.0, bytes);
        encode_string(&self.1, bytes);
    }
}
impl Encode for MaximumPacketSize {
    fn encode(&self, bytes: &mut BytesMut) {
        bytes.put_u8(PropertyType::MaximumPacketSize as u8);
        bytes.put_u32(self.0);
    }
}
impl Encode for WildcardSubscriptionAvailable {
    fn encode(&self, bytes: &mut BytesMut) {
        bytes.put_u8(PropertyType::WildcardSubscriptionAvailable as u8);
        bytes.put_u8(self.0);
    }
}
impl Encode for SubscriptionIdentifierAvailable {
    fn encode(&self, bytes: &mut BytesMut) {
        bytes.put_u8(PropertyType::SubscriptionIdentifierAvailable as u8);
        bytes.put_u8(self.0);
    }
}
impl Encode for SharedSubscriptionAvailable {
    fn encode(&self, bytes: &mut BytesMut) {
        bytes.put_u8(PropertyType::SharedSubscriptionAvailable as u8);
        bytes.put_u8(self.0);
    }
}

fn encode_connect(packet: &ConnectPacket, bytes: &mut BytesMut, protocol_version: ProtocolVersion) {
    encode_string(&packet.protocol_name, bytes);
    bytes.put_u8(packet.protocol_version as u8);

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

    if protocol_version == ProtocolVersion::V500 {
        let property_length = packet.property_size(protocol_version);
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
    }

    encode_string(&packet.client_id, bytes);

    if let Some(will) = &packet.will {
        if protocol_version == ProtocolVersion::V500 {
            let property_length = will.property_size(protocol_version);
            encode_variable_int(property_length, bytes);

            will.will_delay_interval.encode(bytes);
            will.payload_format_indicator.encode(bytes);
            will.message_expiry_interval.encode(bytes);
            will.content_type.encode(bytes);
            will.response_topic.encode(bytes);
            will.correlation_data.encode(bytes);
            will.user_properties.encode(bytes);
        }

        encode_string(&will.topic.topic_name(), bytes);
        encode_binary_data(&will.payload, bytes);
    }

    if let Some(user_name) = &packet.user_name {
        encode_string(user_name, bytes);
    }

    if let Some(password) = &packet.password {
        encode_string(password, bytes);
    }
}

fn encode_connect_ack(
    packet: &ConnectAckPacket,
    bytes: &mut BytesMut,
    protocol_version: ProtocolVersion,
) {
    let mut connect_ack_flags: u8 = 0b0000_0000;
    if packet.session_present {
        connect_ack_flags |= 0b0000_0001;
    }

    bytes.put_u8(connect_ack_flags);
    bytes.put_u8(packet.reason_code as u8);

    if protocol_version == ProtocolVersion::V500 {
        let property_length = packet.property_size(protocol_version);
        encode_variable_int(property_length, bytes);

        packet.session_expiry_interval.encode(bytes);
        packet.receive_maximum.encode(bytes);
        packet.maximum_qos.encode(bytes);
        packet.retain_available.encode(bytes);
        packet.maximum_packet_size.encode(bytes);
        packet.assigned_client_identifier.encode(bytes);
        packet.topic_alias_maximum.encode(bytes);
        packet.reason_string.encode(bytes);
        packet.user_properties.encode(bytes);
        packet.wildcard_subscription_available.encode(bytes);
        packet.subscription_identifiers_available.encode(bytes);
        packet.shared_subscription_available.encode(bytes);
        packet.server_keep_alive.encode(bytes);
        packet.response_information.encode(bytes);
        packet.server_reference.encode(bytes);
        packet.authentication_method.encode(bytes);
        packet.authentication_data.encode(bytes);
    }
}

fn encode_publish(packet: &PublishPacket, bytes: &mut BytesMut, protocol_version: ProtocolVersion) {
    encode_string(&packet.topic.to_string(), bytes);

    if let Some(packet_id) = packet.packet_id {
        bytes.put_u16(packet_id);
    }

    if protocol_version == ProtocolVersion::V500 {
        let property_length = packet.property_size(protocol_version);
        encode_variable_int(property_length, bytes);

        packet.payload_format_indicator.encode(bytes);
        packet.message_expiry_interval.encode(bytes);
        packet.topic_alias.encode(bytes);
        packet.response_topic.encode(bytes);
        packet.correlation_data.encode(bytes);
        packet.user_properties.encode(bytes);
        packet.subscription_identifiers.encode(bytes);
        packet.content_type.encode(bytes);
    }

    bytes.put_slice(&packet.payload);
}

fn encode_publish_ack(
    packet: &PublishAckPacket,
    bytes: &mut BytesMut,
    protocol_version: ProtocolVersion,
) {
    bytes.put_u16(packet.packet_id);

    if protocol_version == ProtocolVersion::V500 {
        bytes.put_u8(packet.reason_code as u8);

        let property_length = packet.property_size(protocol_version);
        encode_variable_int(property_length, bytes);

        packet.reason_string.encode(bytes);
        packet.user_properties.encode(bytes);
    }
}

fn encode_publish_received(
    packet: &PublishReceivedPacket,
    bytes: &mut BytesMut,
    protocol_version: ProtocolVersion,
) {
    bytes.put_u16(packet.packet_id);

    if protocol_version == ProtocolVersion::V500 {
        bytes.put_u8(packet.reason_code as u8);

        let property_length = packet.property_size(protocol_version);
        encode_variable_int(property_length, bytes);

        packet.reason_string.encode(bytes);
        packet.user_properties.encode(bytes);
    }
}

fn encode_publish_release(
    packet: &PublishReleasePacket,
    bytes: &mut BytesMut,
    protocol_version: ProtocolVersion,
) {
    bytes.put_u16(packet.packet_id);

    if protocol_version == ProtocolVersion::V500 {
        bytes.put_u8(packet.reason_code as u8);

        let property_length = packet.property_size(protocol_version);
        encode_variable_int(property_length, bytes);

        packet.reason_string.encode(bytes);
        packet.user_properties.encode(bytes);
    }
}

fn encode_publish_complete(
    packet: &PublishCompletePacket,
    bytes: &mut BytesMut,
    protocol_version: ProtocolVersion,
) {
    bytes.put_u16(packet.packet_id);

    if protocol_version == ProtocolVersion::V500 {
        bytes.put_u8(packet.reason_code as u8);

        let property_length = packet.property_size(protocol_version);
        encode_variable_int(property_length, bytes);

        packet.reason_string.encode(bytes);
        packet.user_properties.encode(bytes);
    }
}

fn encode_subscribe(
    packet: &SubscribePacket,
    bytes: &mut BytesMut,
    protocol_version: ProtocolVersion,
) {
    bytes.put_u16(packet.packet_id);

    if protocol_version == ProtocolVersion::V500 {
        let property_length = packet.property_size(protocol_version);
        encode_variable_int(property_length, bytes);

        packet.subscription_identifier.encode(bytes);
        packet.user_properties.encode(bytes);
    }

    for topic in &packet.subscription_topics {
        encode_string(&topic.topic_filter.to_string(), bytes);

        let mut options_byte = 0b0000_0000;
        let retain_handling_byte = topic.retain_handling as u8;
        options_byte |= (retain_handling_byte & 0b0000_0011) << 4;

        if topic.retain_as_published {
            options_byte |= 0b0000_1000;
        }

        if topic.no_local {
            options_byte |= 0b0000_0100;
        }

        let qos_byte = topic.maximum_qos as u8;
        options_byte |= qos_byte & 0b0000_0011;

        bytes.put_u8(options_byte);
    }
}

fn encode_subscribe_ack(
    packet: &SubscribeAckPacket,
    bytes: &mut BytesMut,
    protocol_version: ProtocolVersion,
) {
    bytes.put_u16(packet.packet_id);

    if protocol_version == ProtocolVersion::V500 {
        let property_length = packet.property_size(protocol_version);
        encode_variable_int(property_length, bytes);

        packet.reason_string.encode(bytes);
        packet.user_properties.encode(bytes);
    }

    for code in &packet.reason_codes {
        bytes.put_u8((*code) as u8);
    }
}

fn encode_unsubscribe(
    packet: &UnsubscribePacket,
    bytes: &mut BytesMut,
    protocol_version: ProtocolVersion,
) {
    bytes.put_u16(packet.packet_id);

    if protocol_version == ProtocolVersion::V500 {
        let property_length = packet.property_size(protocol_version);
        encode_variable_int(property_length, bytes);

        packet.user_properties.encode(bytes);
    }

    for topic_filter in &packet.topic_filters {
        encode_string(&topic_filter.to_string(), bytes);
    }
}

fn encode_unsubscribe_ack(
    packet: &UnsubscribeAckPacket,
    bytes: &mut BytesMut,
    protocol_version: ProtocolVersion,
) {
    bytes.put_u16(packet.packet_id);

    if protocol_version == ProtocolVersion::V500 {
        let property_length = packet.property_size(protocol_version);
        encode_variable_int(property_length, bytes);

        packet.reason_string.encode(bytes);
        packet.user_properties.encode(bytes);
    }

    for code in &packet.reason_codes {
        bytes.put_u8((*code) as u8);
    }
}

fn encode_disconnect(
    packet: &DisconnectPacket,
    bytes: &mut BytesMut,
    protocol_version: ProtocolVersion,
) {
    if protocol_version == ProtocolVersion::V500 {
        bytes.put_u8(packet.reason_code as u8);

        let property_length = packet.property_size(protocol_version);
        encode_variable_int(property_length, bytes);

        packet.session_expiry_interval.encode(bytes);
        packet.reason_string.encode(bytes);
        packet.user_properties.encode(bytes);
        packet.server_reference.encode(bytes);
    }
}

fn encode_authenticate(
    packet: &AuthenticatePacket,
    bytes: &mut BytesMut,
    protocol_version: ProtocolVersion,
) {
    bytes.put_u8(packet.reason_code as u8);

    if protocol_version == ProtocolVersion::V500 {
        let property_length = packet.property_size(protocol_version);
        encode_variable_int(property_length, bytes);

        packet.authentication_method.encode(bytes);
        packet.authentication_data.encode(bytes);
        packet.reason_string.encode(bytes);
        packet.user_properties.encode(bytes);
    }
}

pub fn encode_mqtt(packet: &Packet, bytes: &mut BytesMut, protocol_version: ProtocolVersion) {
    let remaining_length = packet.calculate_size(protocol_version);
    let packet_size =
        1 + VariableByteInt(remaining_length).calculate_size(protocol_version) + remaining_length;
    bytes.reserve(packet_size as usize);

    let first_byte = packet.to_byte();
    let mut first_byte_val = (first_byte << 4) & 0b1111_0000;
    first_byte_val |= packet.fixed_header_flags();

    bytes.put_u8(first_byte_val);
    encode_variable_int(remaining_length, bytes);

    match packet {
        Packet::Connect(p) => encode_connect(p, bytes, protocol_version),
        Packet::ConnectAck(p) => encode_connect_ack(p, bytes, protocol_version),
        Packet::Publish(p) => encode_publish(p, bytes, protocol_version),
        Packet::PublishAck(p) => encode_publish_ack(p, bytes, protocol_version),
        Packet::PublishReceived(p) => encode_publish_received(p, bytes, protocol_version),
        Packet::PublishRelease(p) => encode_publish_release(p, bytes, protocol_version),
        Packet::PublishComplete(p) => encode_publish_complete(p, bytes, protocol_version),
        Packet::Subscribe(p) => encode_subscribe(p, bytes, protocol_version),
        Packet::SubscribeAck(p) => encode_subscribe_ack(p, bytes, protocol_version),
        Packet::Unsubscribe(p) => encode_unsubscribe(p, bytes, protocol_version),
        Packet::UnsubscribeAck(p) => encode_unsubscribe_ack(p, bytes, protocol_version),
        Packet::PingRequest => {},
        Packet::PingResponse => {},
        Packet::Disconnect(p) => encode_disconnect(p, bytes, protocol_version),
        Packet::Authenticate(p) => encode_authenticate(p, bytes, protocol_version),
    }
}

#[cfg(test)]
mod tests {
    use crate::{decoder::*, encoder::*, types::*};
    use bytes::BytesMut;

    #[test]
    fn connect_roundtrip() {
        let packet = Packet::Connect(ConnectPacket {
            protocol_name: "MQTT".to_string(),
            protocol_version: ProtocolVersion::V500,
            clean_start: true,
            keep_alive: 200,

            session_expiry_interval: None,
            receive_maximum: None,
            maximum_packet_size: None,
            topic_alias_maximum: None,
            request_response_information: None,
            request_problem_information: None,
            user_properties: vec![],
            authentication_method: None,
            authentication_data: None,

            client_id: "test_client".to_string(),
            will: None,
            user_name: None,
            password: None,
        });

        let mut bytes = BytesMut::new();
        encode_mqtt(&packet, &mut bytes, ProtocolVersion::V500);
        let decoded = decode_mqtt(&mut bytes, ProtocolVersion::V500).unwrap().unwrap();

        assert_eq!(packet, decoded);
    }

    #[test]
    fn connect_ack_roundtrip() {
        let packet = Packet::ConnectAck(ConnectAckPacket {
            session_present: false,
            reason_code: ConnectReason::Success,

            session_expiry_interval: None,
            receive_maximum: None,
            maximum_qos: None,
            retain_available: None,
            maximum_packet_size: None,
            assigned_client_identifier: None,
            topic_alias_maximum: None,
            reason_string: None,
            user_properties: vec![],
            wildcard_subscription_available: None,
            subscription_identifiers_available: None,
            shared_subscription_available: None,
            server_keep_alive: None,
            response_information: None,
            server_reference: None,
            authentication_method: None,
            authentication_data: None,
        });

        let mut bytes = BytesMut::new();
        encode_mqtt(&packet, &mut bytes, ProtocolVersion::V500);
        let decoded = decode_mqtt(&mut bytes, ProtocolVersion::V500).unwrap().unwrap();

        assert_eq!(packet, decoded);
    }

    #[test]
    fn publish_roundtrip() {
        let packet = Packet::Publish(PublishPacket {
            is_duplicate: false,
            qos: QoS::AtLeastOnce,
            retain: false,

            topic: "test_topic".parse().unwrap(),
            packet_id: Some(42),

            payload_format_indicator: None,
            message_expiry_interval: None,
            topic_alias: None,
            response_topic: None,
            correlation_data: None,
            user_properties: vec![],
            subscription_identifiers: Vec::with_capacity(0),
            content_type: None,

            payload: vec![22; 100].into(),
        });

        let mut bytes = BytesMut::new();
        encode_mqtt(&packet, &mut bytes, ProtocolVersion::V500);
        let decoded = decode_mqtt(&mut bytes, ProtocolVersion::V500).unwrap().unwrap();

        assert_eq!(packet, decoded);
    }

    #[test]
    fn publish_ack_roundtrip() {
        let packet = Packet::PublishAck(PublishAckPacket {
            packet_id: 1500,
            reason_code: PublishAckReason::Success,

            reason_string: None,
            user_properties: vec![],
        });

        let mut bytes = BytesMut::new();
        encode_mqtt(&packet, &mut bytes, ProtocolVersion::V500);
        let decoded = decode_mqtt(&mut bytes, ProtocolVersion::V500).unwrap().unwrap();

        assert_eq!(packet, decoded);
    }

    #[test]
    fn publish_received_roundtrip() {
        let packet = Packet::PublishReceived(PublishReceivedPacket {
            packet_id: 1500,
            reason_code: PublishReceivedReason::Success,

            reason_string: None,
            user_properties: vec![],
        });

        let mut bytes = BytesMut::new();
        encode_mqtt(&packet, &mut bytes, ProtocolVersion::V500);
        let decoded = decode_mqtt(&mut bytes, ProtocolVersion::V500).unwrap().unwrap();

        assert_eq!(packet, decoded);
    }

    #[test]
    fn publish_release_roundtrip() {
        let packet = Packet::PublishRelease(PublishReleasePacket {
            packet_id: 1500,
            reason_code: PublishReleaseReason::Success,

            reason_string: None,
            user_properties: vec![],
        });

        let mut bytes = BytesMut::new();
        encode_mqtt(&packet, &mut bytes, ProtocolVersion::V500);
        let decoded = decode_mqtt(&mut bytes, ProtocolVersion::V500).unwrap().unwrap();

        assert_eq!(packet, decoded);
    }

    #[test]
    fn publish_complete_roundtrip() {
        let packet = Packet::PublishComplete(PublishCompletePacket {
            packet_id: 1500,
            reason_code: PublishCompleteReason::Success,

            reason_string: None,
            user_properties: vec![],
        });

        let mut bytes = BytesMut::new();
        encode_mqtt(&packet, &mut bytes, ProtocolVersion::V500);
        let decoded = decode_mqtt(&mut bytes, ProtocolVersion::V500).unwrap().unwrap();

        assert_eq!(packet, decoded);
    }

    #[test]
    fn subscribe_roundtrip() {
        let packet = Packet::Subscribe(SubscribePacket {
            packet_id: 4500,

            subscription_identifier: None,
            user_properties: vec![],

            subscription_topics: vec![SubscriptionTopic {
                topic_filter: "test_topic".parse().unwrap(),
                maximum_qos: QoS::AtLeastOnce,
                no_local: false,
                retain_as_published: false,
                retain_handling: RetainHandling::SendAtSubscribeTime,
            }],
        });

        let mut bytes = BytesMut::new();
        encode_mqtt(&packet, &mut bytes, ProtocolVersion::V500);
        let decoded = decode_mqtt(&mut bytes, ProtocolVersion::V500).unwrap().unwrap();

        assert_eq!(packet, decoded);
    }

    #[test]
    fn subscribe_ack_roundtrip() {
        let packet = Packet::SubscribeAck(SubscribeAckPacket {
            packet_id: 1234,

            reason_string: None,
            user_properties: vec![],

            reason_codes: vec![SubscribeAckReason::GrantedQoSZero],
        });

        let mut bytes = BytesMut::new();
        encode_mqtt(&packet, &mut bytes, ProtocolVersion::V500);
        let decoded = decode_mqtt(&mut bytes, ProtocolVersion::V500).unwrap().unwrap();

        assert_eq!(packet, decoded);
    }

    #[test]
    fn unsubscribe_roundtrip() {
        let packet = Packet::Unsubscribe(UnsubscribePacket {
            packet_id: 1234,

            user_properties: vec![],

            topic_filters: vec!["test_topic".parse().unwrap()],
        });

        let mut bytes = BytesMut::new();
        encode_mqtt(&packet, &mut bytes, ProtocolVersion::V500);
        let decoded = decode_mqtt(&mut bytes, ProtocolVersion::V500).unwrap().unwrap();

        assert_eq!(packet, decoded);
    }

    #[test]
    fn unsubscribe_ack_roundtrip() {
        let packet = Packet::UnsubscribeAck(UnsubscribeAckPacket {
            packet_id: 4321,

            reason_string: None,
            user_properties: vec![],

            reason_codes: vec![UnsubscribeAckReason::Success],
        });

        let mut bytes = BytesMut::new();
        encode_mqtt(&packet, &mut bytes, ProtocolVersion::V500);
        let decoded = decode_mqtt(&mut bytes, ProtocolVersion::V500).unwrap().unwrap();

        assert_eq!(packet, decoded);
    }

    #[test]
    fn ping_request_roundtrip() {
        let packet = Packet::PingRequest;
        let mut bytes = BytesMut::new();
        encode_mqtt(&packet, &mut bytes, ProtocolVersion::V500);
        let decoded = decode_mqtt(&mut bytes, ProtocolVersion::V500).unwrap().unwrap();

        assert_eq!(packet, decoded);
    }

    #[test]
    fn ping_response_roundtrip() {
        let packet = Packet::PingResponse;
        let mut bytes = BytesMut::new();
        encode_mqtt(&packet, &mut bytes, ProtocolVersion::V500);
        let decoded = decode_mqtt(&mut bytes, ProtocolVersion::V500).unwrap().unwrap();

        assert_eq!(packet, decoded);
    }

    #[test]
    fn disconnect_roundtrip() {
        let packet = Packet::Disconnect(DisconnectPacket {
            reason_code: DisconnectReason::NormalDisconnection,

            session_expiry_interval: None,
            reason_string: None,
            user_properties: vec![],
            server_reference: None,
        });
        let mut bytes = BytesMut::new();
        encode_mqtt(&packet, &mut bytes, ProtocolVersion::V500);
        let decoded = decode_mqtt(&mut bytes, ProtocolVersion::V500).unwrap().unwrap();

        assert_eq!(packet, decoded);
    }

    #[test]
    fn authenticate_roundtrip() {
        let packet = Packet::Authenticate(AuthenticatePacket {
            reason_code: AuthenticateReason::Success,

            authentication_method: None,
            authentication_data: None,
            reason_string: None,
            user_properties: vec![],
        });
        let mut bytes = BytesMut::new();
        encode_mqtt(&packet, &mut bytes, ProtocolVersion::V500);
        let decoded = decode_mqtt(&mut bytes, ProtocolVersion::V500).unwrap().unwrap();

        assert_eq!(packet, decoded);
    }
}
