use crate::{
    types::{ConnectAckPacket, ConnectReason, Packet, ProtocolError},
    MqttCodec,
};
use futures::{SinkExt, StreamExt};
use std::time::Duration;
use tokio::{
    io::{AsyncRead, AsyncWrite},
    time,
};
use tokio_util::codec::Framed;

pub struct UnconnectedClient<T> {
    framed_stream: Framed<T, MqttCodec>,
}

impl<T: AsyncRead + AsyncWrite + Unpin> UnconnectedClient<T> {
    pub fn new(framed_stream: Framed<T, MqttCodec>) -> Self {
        Self { framed_stream }
    }

    pub async fn handshake(mut self) -> Result<Client<T>, ProtocolError> {
        let first_packet = time::timeout(Duration::from_secs(2), self.framed_stream.next())
            .await
            .map_err(|_| ProtocolError::ConnectTimedOut)?;

        println!("got a packet: {:?}", first_packet);

        match first_packet {
            Some(Ok(Packet::Connect(_connect_packet))) => {
                let connect_ack = ConnectAckPacket {
                    // Variable header
                    session_present: false,
                    reason_code: ConnectReason::Success,

                    // Properties
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
                };

                self.framed_stream
                    .send(Packet::ConnectAck(connect_ack))
                    .await
                    .expect("Couldn't forward packet to framed socket");

                Ok(Client::new(self.framed_stream))
            },
            Some(Ok(_)) => Err(ProtocolError::FirstPacketNotConnect),
            Some(Err(e)) => Err(ProtocolError::MalformedPacket(e)),
            None => {
                // TODO(bschwind) - Technically end of stream?
                Err(ProtocolError::FirstPacketNotConnect)
            },
        }
    }
}

pub struct Client<T: AsyncRead + AsyncWrite + Unpin> {
    framed_stream: Framed<T, MqttCodec>,
}

impl<T: AsyncRead + AsyncWrite + Unpin> Client<T> {
    pub fn new(framed_stream: Framed<T, MqttCodec>) -> Self {
        Self { framed_stream }
    }
}
