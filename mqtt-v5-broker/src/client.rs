use crate::broker::BrokerMessage;
use futures::{Sink, SinkExt, Stream, StreamExt};
use mqtt_v5::types::{DecodeError, EncodeError, Packet, ProtocolError, ProtocolVersion, QoS};
use nanoid::nanoid;
use std::{marker::Unpin, time::Duration};
use tokio::{
    sync::mpsc::{self, Receiver, Sender},
    time,
};

type PacketResult = Result<Packet, DecodeError>;

pub struct UnconnectedClient<ST: Stream<Item = PacketResult>, SI: Sink<Packet, Error = EncodeError>>
{
    packet_stream: ST,
    packet_sink: SI,
    broker_tx: Sender<BrokerMessage>,
}

impl<ST: Stream<Item = PacketResult> + Unpin, SI: Sink<Packet, Error = EncodeError>>
    UnconnectedClient<ST, SI>
{
    pub fn new(packet_stream: ST, packet_sink: SI, broker_tx: Sender<BrokerMessage>) -> Self {
        Self { packet_stream, packet_sink, broker_tx }
    }

    pub async fn handshake(mut self) -> Result<Client<ST, SI>, ProtocolError> {
        let first_packet = time::timeout(Duration::from_secs(2), self.packet_stream.next())
            .await
            .map_err(|_| ProtocolError::ConnectTimedOut)?;

        println!("got a packet: {:?}", first_packet);

        match first_packet {
            Some(Ok(Packet::Connect(mut connect_packet))) => {
                let (sender, receiver) = mpsc::channel(5);

                if connect_packet.client_id.is_empty() {
                    connect_packet.client_id = nanoid!();
                }

                let client_id = connect_packet.client_id.clone();

                let protocol_version = connect_packet.protocol_version;
                let self_tx = sender.clone();

                self.broker_tx
                    .send(BrokerMessage::NewClient(Box::new(connect_packet), sender))
                    .await
                    .expect("Couldn't send NewClient message to broker");

                Ok(Client::new(
                    client_id,
                    protocol_version,
                    self.packet_stream,
                    self.packet_sink,
                    self.broker_tx,
                    receiver,
                    self_tx,
                ))
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

#[allow(clippy::large_enum_variant)]
#[derive(Debug, PartialEq)]
pub enum ClientMessage {
    Packet(Packet),
    Disconnect,
}

pub struct Client<ST: Stream<Item = PacketResult>, SI: Sink<Packet, Error = EncodeError>> {
    id: String,
    _protocol_version: ProtocolVersion,
    packet_stream: ST,
    packet_sink: SI,
    broker_tx: Sender<BrokerMessage>,
    broker_rx: Receiver<ClientMessage>,
    self_tx: Sender<ClientMessage>,
}

impl<ST: Stream<Item = PacketResult> + Unpin, SI: Sink<Packet, Error = EncodeError>>
    Client<ST, SI>
{
    pub fn new(
        id: String,
        protocol_version: ProtocolVersion,
        packet_stream: ST,
        packet_sink: SI,
        broker_tx: Sender<BrokerMessage>,
        broker_rx: Receiver<ClientMessage>,
        self_tx: Sender<ClientMessage>,
    ) -> Self {
        Self {
            id,
            _protocol_version: protocol_version,
            packet_stream,
            packet_sink,
            broker_tx,
            broker_rx,
            self_tx,
        }
    }

    async fn handle_socket_reads(
        mut stream: ST,
        client_id: String,
        mut broker_tx: Sender<BrokerMessage>,
        mut self_tx: Sender<ClientMessage>,
    ) {
        while let Some(frame) = stream.next().await {
            match frame {
                Ok(frame) => match frame {
                    Packet::Subscribe(packet) => {
                        broker_tx
                            .send(BrokerMessage::Subscribe(client_id.clone(), packet))
                            .await
                            .expect("Couldn't send Subscribe message to broker");
                    },
                    Packet::Publish(packet) => {
                        match packet.qos {
                            QoS::AtMostOnce => {},
                            QoS::AtLeastOnce => {
                                assert!(
                                    packet.packet_id.is_some(),
                                    "Packets with QoS 1&2 need packet identifiers"
                                );
                            },
                            QoS::ExactlyOnce => {},
                        }

                        broker_tx
                            .send(BrokerMessage::Publish(client_id.clone(), packet))
                            .await
                            .expect("Couldn't send Publish message to broker");
                    },
                    Packet::PublishAck(packet) => {
                        broker_tx
                            .send(BrokerMessage::PublishAck(client_id.clone(), packet))
                            .await
                            .expect("Couldn't send Publish message to broker");
                    },
                    Packet::PingRequest => {
                        self_tx
                            .send(ClientMessage::Packet(Packet::PingResponse))
                            .await
                            .expect("Couldn't send PingResponse message to self");
                    },
                    _ => {},
                },
                Err(err) => {
                    println!("Error while reading frame: {:?}", err);
                    break;
                },
            }
        }

        broker_tx
            .send(BrokerMessage::Disconnect(client_id.clone()))
            .await
            .expect("Couldn't send Disconnect message to broker");
    }

    async fn handle_socket_writes(sink: SI, mut broker_rx: Receiver<ClientMessage>) {
        tokio::pin!(sink);

        while let Some(frame) = broker_rx.recv().await {
            match frame {
                ClientMessage::Packet(packet) => {
                    sink.send(packet).await.expect("Couldn't forward packet to framed socket");
                },
                ClientMessage::Disconnect => println!("broker told the client to disconnect"),
            }
        }
    }

    pub async fn run(self) {
        let task_rx =
            Self::handle_socket_reads(self.packet_stream, self.id, self.broker_tx, self.self_tx);
        let task_tx = Self::handle_socket_writes(self.packet_sink, self.broker_rx);

        tokio::select! {
            _ = task_rx => println!("rx"),
            _ = task_tx => println!("tx"),
            else => println!("done"),
        }
    }
}
