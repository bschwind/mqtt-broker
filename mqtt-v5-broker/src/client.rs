use crate::broker::{BrokerMessage, WillDisconnectLogic};
use futures::{
    future::{self, Either},
    stream, Sink, SinkExt, Stream, StreamExt,
};
use log::{debug, info, trace, warn};
use mqtt_v5::{
    codec::MqttCodec,
    types::{
        DecodeError, DisconnectPacket, DisconnectReason, EncodeError, Packet, ProtocolError,
        ProtocolVersion, QoS,
    },
};
use nanoid::nanoid;
use std::{marker::Unpin, time::Duration};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    sync::mpsc::{self, Receiver, Sender},
    task, time,
};
use tokio_util::codec::Framed;

use bytes::BytesMut;
use mqtt_v5::{
    encoder,
    websocket::{
        codec::{Message, MessageCodec as WsMessageCodec, Opcode},
        WsUpgraderCodec,
    },
};

type PacketResult = Result<Packet, DecodeError>;

/// Timeout when writing to a client sink
const SINK_SEND_TIMEOUT: Duration = Duration::from_secs(1);

/// Process MQTT connect on `stream` and spawn a task for this connection
/// TOOD(flxo): Move to dedicated module `io`?
pub fn spawn<S>(stream: S, broker_tx: Sender<BrokerMessage>)
where
    S: AsyncRead + AsyncWrite + Send + Sync + 'static,
{
    let (packet_sink, packet_stream) = Framed::new(stream, MqttCodec::new()).split();
    spawn_framed(packet_stream, packet_sink, broker_tx);
}

/// TOOD(flxo): Move to dedicated module `io`?
pub fn spawn_framed<ST, SI>(packet_stream: ST, packet_sink: SI, broker_tx: Sender<BrokerMessage>)
where
    ST: Stream<Item = PacketResult> + Unpin + Send + Sync + 'static,
    SI: Sink<Packet, Error = EncodeError> + Unpin + Send + Sync + 'static,
{
    task::spawn(async move {
        let unconnected_client = UnconnectedClient::new(packet_stream, packet_sink, broker_tx);
        match unconnected_client.handshake().await {
            Ok(client) => client.run().await,
            Err(err) => warn!("Protocol error during connection handshake: {:?}", err),
        }
    });
}

/// TOOD(flxo): Move to dedicated module `io`?
async fn upgrade_ws_stream<S>(stream: S) -> Framed<S, WsMessageCodec>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + Sync + 'static,
{
    let mut upgrade_framed = Framed::new(stream, WsUpgraderCodec::new());

    let upgrade_msg = upgrade_framed.next().await;

    if let Some(Ok(websocket_key)) = upgrade_msg {
        let _ = upgrade_framed.send(websocket_key).await;
    }

    let old_parts = upgrade_framed.into_parts();
    let mut new_parts =
        Framed::new(old_parts.io, WsMessageCodec::with_masked_encode(false)).into_parts();
    new_parts.read_buf = old_parts.read_buf;
    new_parts.write_buf = old_parts.write_buf;

    Framed::from_parts(new_parts)
}

/// TOOD(flxo): Move to dedicated module `io`?
pub async fn spawn_websocket<S>(stream: S, broker_tx: Sender<BrokerMessage>)
where
    S: AsyncRead + AsyncWrite + Unpin + Send + Sync + 'static,
{
    let ws_framed = upgrade_ws_stream(stream).await;

    let (sink, ws_stream) = ws_framed.split();
    let sink = sink.with(|packet: Packet| {
        let mut payload_bytes = BytesMut::new();
        // TODO(bschwind) - Support MQTTv5 here. With a stateful Framed object we can store
        //                  the version on a successful Connect decode, but in this code structure
        //                  we can't pass state from the stream to the sink.
        encoder::encode_mqtt(&packet, &mut payload_bytes, ProtocolVersion::V311);

        async {
            let result: Result<Message, EncodeError> = Ok(Message::binary(payload_bytes.freeze()));
            result
        }
    });

    let read_buf = BytesMut::with_capacity(4096);

    let stream = stream::unfold(
        (ws_stream, read_buf, ProtocolVersion::V311),
        |(mut ws_stream, mut read_buf, mut protocol_version)| {
            async move {
                // Loop until we've built up enough data from the WebSocket stream
                // to decode a new MQTT packet
                loop {
                    // Try to read an MQTT packet from the read buffer
                    match mqtt_v5::decoder::decode_mqtt(&mut read_buf, protocol_version) {
                        Ok(Some(packet)) => {
                            if let Packet::Connect(packet) = &packet {
                                protocol_version = packet.protocol_version;
                            }

                            // If we got one, return it
                            return Some((Ok(packet), (ws_stream, read_buf, protocol_version)));
                        },
                        Err(e) => {
                            // If we had a decode error, propagate the error along the stream
                            return Some((Err(e), (ws_stream, read_buf, protocol_version)));
                        },
                        Ok(None) => {
                            // Otherwise we need more binary data from the WebSocket stream
                        },
                    }

                    let ws_frame = ws_stream.next().await;

                    match ws_frame {
                        Some(Ok(message)) => {
                            if message.opcode() == Opcode::Close {
                                return None;
                            }

                            if message.opcode() == Opcode::Ping {
                                trace!("Got a websocket ping");
                            }

                            if message.opcode() != Opcode::Binary {
                                // MQTT Control Packets MUST be sent in WebSocket binary data frames
                                return Some((
                                    Err(DecodeError::BadTransport),
                                    (ws_stream, read_buf, protocol_version),
                                ));
                            }

                            read_buf.extend_from_slice(&message.into_data());
                        },
                        Some(Err(e)) => {
                            debug!("Error while reading from WebSocket stream: {:?}", e);
                            // If we had a decode error in the WebSocket layer,
                            // propagate the it along the stream
                            return Some((
                                Err(DecodeError::BadTransport),
                                (ws_stream, read_buf, protocol_version),
                            ));
                        },
                        None => {
                            // The WebSocket stream is over, so we are too
                            return None;
                        },
                    }
                }
            }
        },
    );

    spawn_framed(Box::pin(stream), Box::pin(sink), broker_tx);
}

struct UnconnectedClient<ST: Stream<Item = PacketResult>, SI: Sink<Packet, Error = EncodeError>> {
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

        trace!("Received packet: {:?}", first_packet);

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
                        warn!("Error while reading frame: {:?}", err);
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
            let mut packets = match frame {
                ClientMessage::Packets(packets) => Either::Left(stream::iter(packets)),
                ClientMessage::Packet(packet) => Either::Right(stream::once(future::ready(packet))),
                ClientMessage::Disconnect(reason_code) => {
                    let disconnect_packet = DisconnectPacket {
                        reason_code,
                        session_expiry_interval: None,
                        reason_string: None,
                        user_properties: vec![],
                        server_reference: None,
                    };

                    if let Err(e) = sink.send(Packet::Disconnect(disconnect_packet)).await {
                        warn!("Failed to send disconnect packet to framed socket: {:?}", e);
                    }

                    info!("Broker told the client to disconnect");

                    return;
                },
            };

            // Process each packet in a dedicated timeout to be fair
            while let Some(packet) = packets.next().await {
                let send = sink.send(packet);
                match tokio::time::timeout(SINK_SEND_TIMEOUT, send).await {
                    Ok(Ok(())) => (),
                    Ok(Err(e)) => {
                        warn!("Failed to write to client client socket: {:?}", e);
                        return;
                    },
                    Err(_) => {
                        warn!("Timeout during client socket write. Disconnecting");
                        return;
                    },
                }
            }
        }
    }

    pub async fn run(self) {
        let task_rx = Self::handle_socket_reads(
            self.packet_stream,
            self.id.clone(),
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
        future::join(task_rx, task_tx).await;
        debug!("Client ID {} task exit", self.id);
    }
}
