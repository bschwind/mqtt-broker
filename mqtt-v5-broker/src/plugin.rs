use log::{trace, warn};
use mqtt_v5::types::{
    properties::{AuthenticationData, AuthenticationMethod},
    *,
};

pub struct Noop;

/// Result of a authentication attempt
pub enum AuthentificationResult {
    /// Authentification is successful.
    Success,
    /// Authentification failed. Send connect reason code to the client (ConnectAck)
    Fail(ConnectReason),
    /// Send this auth packet to the client and wait for the response.
    Packet(AuthenticatePacket),
}

/// Broker plugin
pub trait Plugin {
    /// Called on connect packet reception
    fn on_connect(
        &mut self,
        method: Option<&AuthenticationMethod>,
        data: Option<&AuthenticationData>,
    ) -> AuthentificationResult;

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
    fn on_connect(
        &mut self,
        _: Option<&AuthenticationMethod>,
        _: Option<&AuthenticationData>,
    ) -> AuthentificationResult {
        AuthentificationResult::Success
    }

    fn on_authenticate(&mut self, _: &AuthenticatePacket) -> AuthentificationResult {
        AuthentificationResult::Success
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
