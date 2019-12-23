use crate::types::{
    ConnectAckPacket, ConnectReason, DecodeError, Packet, SubscribeAckPacket, SubscribeAckReason,
};
use bytes::BytesMut;
use futures::{SinkExt, StreamExt};
use tokio::{
    net::{TcpListener, TcpStream},
    runtime::Runtime,
};
use tokio_util::codec::{Decoder, Encoder, Framed};

mod decoder;
mod encoder;
mod types;

pub struct MqttCodec;

impl MqttCodec {
    pub fn new() -> Self {
        MqttCodec {}
    }
}

impl Decoder for MqttCodec {
    type Error = DecodeError;
    type Item = Packet;

    fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        // TODO - Ideally we should keep a state machine to store the data we've read so far.
        decoder::decode_mqtt(buf)
    }
}

impl Encoder for MqttCodec {
    type Error = DecodeError;
    type Item = Packet;

    fn encode(&mut self, packet: Self::Item, bytes: &mut BytesMut) -> Result<(), DecodeError> {
        encoder::encode_mqtt(&packet, bytes)?;
        Ok(())
    }
}

async fn client_handler(stream: TcpStream) {
    println!("Handling a client");

    let mut framed_sock = Framed::new(stream, MqttCodec::new());

    while let Some(frame) = framed_sock.next().await {
        match frame {
            Ok(frame) => {
                println!("Got a frame: {:#?}", frame);

                if let Packet::Connect(_) = frame {
                    let connect_ack = ConnectAckPacket {
                        // Variable header
                        session_present: false,
                        reason: ConnectReason::Success,

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

                    framed_sock
                        .send(Packet::ConnectAck(connect_ack))
                        .await
                        .expect("Couldn't forward packet to framed socket");
                }

                if let Packet::Subscribe(packet) = frame {
                    let subscribe_ack = SubscribeAckPacket {
                        packet_id: packet.packet_id,
                        reason_string: None,
                        user_properties: vec![],
                        reason_codes: packet
                            .subscription_topics
                            .iter()
                            .map(|_| SubscribeAckReason::GrantedQoSOne)
                            .collect(),
                    };

                    framed_sock
                        .send(Packet::SubscribeAck(subscribe_ack))
                        .await
                        .expect("Couldn't forward packet to framed socket");
                }
            },
            Err(err) => {
                println!("Error while reading frame: {:?}", err);
                break;
            },
        }
    }

    println!("Client disconnected");
}

async fn server_loop() -> Result<(), Box<dyn std::error::Error>> {
    let bind_addr = "0.0.0.0:1883";
    let mut listener = TcpListener::bind(bind_addr).await?;

    println!("Listening on {}", bind_addr);

    loop {
        let (socket, addr) = listener.accept().await?;
        println!("Got a new socket from addr: {:?}", addr);

        let handler = client_handler(socket);

        tokio::spawn(handler);
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Creating a Runtime does the following:
    // * Spawn a background thread running a Reactor instance.
    // * Start a ThreadPool for executing futures.
    // * Run an instance of Timer per thread pool worker thread.
    let mut runtime = Runtime::new()?;

    let server_future = server_loop();

    runtime.block_on(server_future)?;

    Ok(())
}
