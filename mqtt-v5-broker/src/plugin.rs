use log::{trace, warn};
use mqtt_v5::types::{
    AuthenticatePacket, ConnectPacket, ConnectReason, PublishAckPacket, PublishAckReason,
    PublishPacket, PublishReceivedPacket, PublishReceivedReason, QoS, SubscribeAckPacket,
    SubscribeAckReason, SubscribePacket,
};

pub struct Noop;

/// Result of a authentication attempt
pub enum AuthentificationResult {
    /// Authentification reason
    Reason(ConnectReason),
    /// Send this auth packet to the client and wait for the response.
    Packet(AuthenticatePacket),
}

/// Broker plugin
pub trait Plugin {
    /// Called on connect packet reception
    fn on_connect(&mut self, packet: &ConnectPacket) -> AuthentificationResult;

    /// Called on authenticate packet reception
    fn on_authenticate(&mut self, packet: &AuthenticatePacket) -> AuthentificationResult;

    /// Called on subscribe packets reception
    fn on_subscribe(&mut self, packet: &SubscribePacket) -> SubscribeAckPacket;

    /// Called on publish packets reception for QoS 0. Return true if the packet should be published to the clients.
    fn on_publish_received_qos0(&mut self, packet: &PublishPacket) -> bool;
    /// Called on publish packets reception for QoS 1. Return if the packet should be published to the clients and
    /// the publish ack packet to be sent to the publisher.

    fn on_publish_received_qos1(
        &mut self,
        packet: &PublishPacket,
    ) -> (bool, Option<PublishAckPacket>);

    /// Called on publish packets reception for QoS 2. Return if the packet should be published to the clients and
    /// the publish received packet to be sent to the publisher.
    fn on_publish_received_qos2(
        &mut self,
        packet: &PublishPacket,
    ) -> (bool, Option<PublishReceivedPacket>);
}

/// Default noop authenticator
impl Plugin for Noop {
    fn on_connect(&mut self, packet: &ConnectPacket) -> AuthentificationResult {
        // Just a hacky test...
        if packet.user_name.is_some() && packet.user_name == packet.password {
            AuthentificationResult::Reason(ConnectReason::Success)
        } else {
            AuthentificationResult::Reason(ConnectReason::BadUserNameOrPassword)
        }
    }

    fn on_authenticate(&mut self, _: &AuthenticatePacket) -> AuthentificationResult {
        AuthentificationResult::Reason(ConnectReason::Success)
    }

    fn on_subscribe(&mut self, packet: &SubscribePacket) -> SubscribeAckPacket {
        SubscribeAckPacket {
            packet_id: packet.packet_id,
            reason_string: None,
            user_properties: vec![],
            reason_codes: packet
                .subscription_topics
                .iter()
                .inspect(|filter| {
                    trace!("Granting subscribe to {}", filter.topic_filter);
                })
                .map(|filter| match filter.maximum_qos {
                    QoS::AtMostOnce => SubscribeAckReason::GrantedQoSZero,
                    QoS::AtLeastOnce => SubscribeAckReason::GrantedQoSOne,
                    QoS::ExactlyOnce => SubscribeAckReason::GrantedQoSTwo,
                })
                .collect(),
        }
    }

    fn on_publish_received_qos0(&mut self, packet: &PublishPacket) -> bool {
        trace!("Granting QoS 0 publish on topic \"{}\"", packet.topic);
        true
    }

    fn on_publish_received_qos1(
        &mut self,
        packet: &PublishPacket,
    ) -> (bool, Option<PublishAckPacket>) {
        if let Some(packet_id) = packet.packet_id {
            let ack = PublishAckPacket {
                packet_id,
                reason_code: PublishAckReason::Success,
                reason_string: None,
                user_properties: Vec::with_capacity(0),
            };
            trace!("Granting QoS 1 publish on topic \"{}\"", packet.topic);
            (true, Some(ack))
        } else {
            warn!("Publish packet with QoS 1 without packet id");
            (false, None)
        }
    }

    fn on_publish_received_qos2(
        &mut self,
        packet: &PublishPacket,
    ) -> (bool, Option<PublishReceivedPacket>) {
        if let Some(packet_id) = packet.packet_id {
            let ack = PublishReceivedPacket {
                packet_id,
                reason_code: PublishReceivedReason::Success,
                reason_string: None,
                user_properties: Vec::with_capacity(0),
            };
            trace!("Granting QoS 2 publish on topic \"{}\"", packet.topic);
            (true, Some(ack))
        } else {
            warn!("Publish packet with QoS 2 without packet id");
            (false, None)
        }
    }
}
