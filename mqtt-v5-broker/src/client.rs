use crate::broker::{BrokerMessage, WillDisconnectLogic};
use futures::{Sink, SinkExt, Stream, StreamExt};
use mqtt_v5::types::{
    DecodeError, DisconnectPacket, DisconnectReason, EncodeError, Packet, ProtocolError,
    ProtocolVersion, QoS,
};
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
                let protocol_version = connect_packet.protocol_version;

                if connect_packet.protocol_name != "MQTT" {
                    if protocol_version == ProtocolVersion::V500 {
                        // TODO(bschwind) - Respond to the client with
                        //                  0x84 (Unsupported Protocol Version)
                    }

                    return Err(ProtocolError::InvalidProtocolName);
                }

                let (sender, receiver) = mpsc::channel(5);

                if connect_packet.client_id.is_empty() {
                    connect_packet.client_id = nanoid!();
                }

                let client_id = connect_packet.client_id.clone();
                let keepalive_seconds = if connect_packet.keep_alive == 0 {
                    None
                } else {
                    Some(connect_packet.keep_alive)
                };

                let self_tx = sender.clone();

                self.broker_tx
                    .send(BrokerMessage::NewClient(Box::new(connect_packet), sender))
                    .await
                    .expect("Couldn't send NewClient message to broker");

                Ok(Client::new(
                    client_id,
                    protocol_version,
                    keepalive_seconds,
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
    Packets(Vec<Packet>),
    Disconnect(DisconnectReason),
}

pub struct Client<ST: Stream<Item = PacketResult>, SI: Sink<Packet, Error = EncodeError>> {
    id: String,
    _protocol_version: ProtocolVersion,
    keepalive_seconds: Option<u16>,
    packet_stream: ST,
    packet_sink: SI,
    broker_tx: Sender<BrokerMessage>,
    broker_rx: Receiver<ClientMessage>,
    self_tx: Sender<ClientMessage>,
}

impl<ST: Stream<Item = PacketResult> + Unpin, SI: Sink<Packet, Error = EncodeError>>
    Client<ST, SI>
{
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        id: String,
        protocol_version: ProtocolVersion,
        keepalive_seconds: Option<u16>,
        packet_stream: ST,
        packet_sink: SI,
        broker_tx: Sender<BrokerMessage>,
        broker_rx: Receiver<ClientMessage>,
        self_tx: Sender<ClientMessage>,
    ) -> Self {
        Self {
            id,
            _protocol_version: protocol_version,
            keepalive_seconds,
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
        keepalive_seconds: Option<u16>,
        broker_tx: Sender<BrokerMessage>,
        self_tx: Sender<ClientMessage>,
    ) {
        // The keepalive should be 1.5 times the specified keepalive value in the connect packet.
        let keepalive_duration = keepalive_seconds
            .into_iter()
            .map(|k| ((k as f32) * 1.5) as u64)
            .map(Duration::from_secs)
            .next();

        loop {
            let next_packet = {
                if let Some(keepalive_duration) = keepalive_duration {
                    let next_packet = time::timeout(keepalive_duration, stream.next())
                        .await
                        .map_err(|_| ProtocolError::KeepAliveTimeout);

                    if let Err(ProtocolError::KeepAliveTimeout) = next_packet {
                        break;
                    }

                    next_packet.unwrap()
                } else {
                    stream.next().await
                }
            };

            if let Some(frame) = next_packet {
                match frame {
                    Ok(frame) => match frame {
                        Packet::Subscribe(packet) => {
                            broker_tx
                                .send(BrokerMessage::Subscribe(client_id.clone(), packet))
                                .await
                                .expect("Couldn't send Subscribe message to broker");
                        },
                        Packet::Unsubscribe(packet) => {
                            broker_tx
                                .send(BrokerMessage::Unsubscribe(client_id.clone(), packet))
                                .await
                                .expect("Couldn't send Unsubscribe message to broker");
                        },
                        Packet::Publish(packet) => {
                            match packet.qos {
                                QoS::AtMostOnce => {},
                                QoS::AtLeastOnce | QoS::ExactlyOnce => {
                                    assert!(
                                        packet.packet_id.is_some(),
                                        "Packets with QoS 1&2 need packet identifiers"
                                    );
                                },
                            }

                            broker_tx
                                .send(BrokerMessage::Publish(client_id.clone(), Box::new(packet)))
                                .await
                                .expect("Couldn't send Publish message to broker");
                        },
                        Packet::PublishAck(packet) => {
                            broker_tx
                                .send(BrokerMessage::PublishAck(client_id.clone(), packet))
                                .await
                                .expect("Couldn't send PublishAck message to broker");
                        },
                        Packet::PublishRelease(packet) => {
                            broker_tx
                                .send(BrokerMessage::PublishRelease(client_id.clone(), packet))
                                .await
                                .expect("Couldn't send PublishRelease message to broker");
                        },
                        Packet::PublishReceived(packet) => {
                            broker_tx
                                .send(BrokerMessage::PublishReceived(client_id.clone(), packet))
                                .await
                                .expect("Couldn't send PublishReceive message to broker");
                        },
                        Packet::PublishComplete(packet) => {
                            broker_tx
                                .send(BrokerMessage::PublishComplete(client_id.clone(), packet))
                                .await
                                .expect("Couldn't send PublishCompelte message to broker");
                        },
                        Packet::PingRequest => {
                            self_tx
                                .send(ClientMessage::Packet(Packet::PingResponse))
                                .await
                                .expect("Couldn't send PingResponse message to self");
                        },
                        Packet::Disconnect(packet) => {
                            let will_disconnect_logic =
                                if packet.reason_code == DisconnectReason::NormalDisconnection {
                                    WillDisconnectLogic::DoNotSend
                                } else {
                                    WillDisconnectLogic::Send
                                };

                            broker_tx
                                .send(BrokerMessage::Disconnect(
                                    client_id.clone(),
                                    will_disconnect_logic,
                                ))
                                .await
                                .expect("Couldn't send Disconnect message to broker");

                            return;
                        },
                        _ => {},
                    },
                    Err(err) => {
                        println!("Error while reading frame: {:?}", err);
                        break;
                    },
                }
            } else {
                break;
            }
        }

        broker_tx
            .send(BrokerMessage::Disconnect(client_id.clone(), WillDisconnectLogic::Send))
            .await
            .expect("Couldn't send Disconnect message to broker");
    }

    async fn handle_socket_writes(sink: SI, mut broker_rx: Receiver<ClientMessage>) {
        tokio::pin!(sink);

        while let Some(frame) = broker_rx.recv().await {
            match frame {
                ClientMessage::Packets(packets) => {
                    sink.send_all(&mut futures::stream::iter(packets).map(Ok))
                        .await
                        .expect("Couldn't forward packets to framed socket");
                },
                ClientMessage::Packet(packet) => {
                    sink.send(packet).await.expect("Couldn't forward packet to framed socket");
                },
                ClientMessage::Disconnect(reason_code) => {
                    let disconnect_packet = DisconnectPacket {
                        reason_code,
                        session_expiry_interval: None,
                        reason_string: None,
                        user_properties: vec![],
                        server_reference: None,
                    };

                    sink.send(Packet::Disconnect(disconnect_packet))
                        .await
                        .expect("Couldn't forward disconnect packet to framed socket");

                    println!("broker told the client to disconnect");

                    break;
                },
            }
        }
    }

    pub async fn run(self) {
        let task_rx = Self::handle_socket_reads(
            self.packet_stream,
            self.id,
            self.keepalive_seconds,
            self.broker_tx,
            self.self_tx,
        );
        let task_tx = Self::handle_socket_writes(self.packet_sink, self.broker_rx);

        // Note:
        // https://docs.rs/tokio/1.7.0/tokio/macro.select.html#runtime-characteristics
        // By running all async expressions on the current task, the expressions are
        // able to run concurrently but not in parallel. This means all expressions
        // are run on the same thread and if one branch blocks the thread, all other
        // expressions will be unable to continue. If parallelism is required, spawn
        // each async expression using tokio::spawn and pass the join handle to select!.
        tokio::select! {
            _ = task_rx => println!("rx"),
            _ = task_tx => println!("tx"),
            else => println!("done"),
        }
    }
}
