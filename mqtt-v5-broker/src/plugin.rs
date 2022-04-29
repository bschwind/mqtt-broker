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

pub enum PublishReceivedResult {
    Placeholder,
}

pub enum SubscribeResult {
    Placeholder,
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

    fn on_subscribe(&mut self, packet: &SubscribePacket) -> SubscribeResult;
    fn on_publish_received(&mut self, packet: &PublishPacket) -> PublishReceivedResult;
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

    fn on_subscribe(&mut self, _: &SubscribePacket) -> SubscribeResult {
        SubscribeResult::Placeholder
    }

    fn on_publish_received(&mut self, _: &PublishPacket) -> PublishReceivedResult {
        PublishReceivedResult::Placeholder
    }
}
