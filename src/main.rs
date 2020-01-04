use crate::{
    broker::Broker,
    client::UnconnectedClient,
    types::{DecodeError, Packet},
};
use bytes::BytesMut;
use tokio::{
    net::{TcpListener, TcpStream},
    runtime::Runtime,
    sync::mpsc::Sender,
};
use tokio_util::codec::{Decoder, Encoder, Framed};

mod broker;
mod client;
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
        encoder::encode_mqtt(&packet, bytes);
        Ok(())
    }
}

async fn client_handler(stream: TcpStream, mut _broker_tx: Sender<Packet>) {
    println!("Handling a client");

    let framed_sock = Framed::new(stream, MqttCodec::new());
    let unconnected_client = UnconnectedClient::new(framed_sock);

    let connected_client = match unconnected_client.handshake().await {
        Ok(connected_client) => connected_client,
        Err(err) => {
            println!("Protocol error during connection handshake: {:?}", err);
            return;
        },
    };

    // TODO - use connected_client for broker logic

    // while let Some(frame) = framed_sock.next().await {
    //     match frame {
    //         Ok(frame) => {
    //             println!("Got a frame: {:#?}", frame);

    //             if let Packet::Subscribe(packet) = &frame {
    //                 let subscribe_ack = SubscribeAckPacket {
    //                     packet_id: packet.packet_id,
    //                     reason_string: None,
    //                     user_properties: vec![],
    //                     reason_codes: packet
    //                         .subscription_topics
    //                         .iter()
    //                         .map(|_| SubscribeAckReason::GrantedQoSOne)
    //                         .collect(),
    //                 };

    //                 framed_sock
    //                     .send(Packet::SubscribeAck(subscribe_ack))
    //                     .await
    //                     .expect("Couldn't forward packet to framed socket");
    //             }

    //             broker_tx.send(frame).await;
    //         },
    //         Err(err) => {
    //             println!("Error while reading frame: {:?}", err);
    //             break;
    //         },
    //     }
    // }

    println!("Client disconnected");
}

async fn server_loop() -> Result<(), Box<dyn std::error::Error>> {
    let bind_addr = "0.0.0.0:1883";
    let mut listener = TcpListener::bind(bind_addr).await?;

    println!("Listening on {}", bind_addr);

    let broker = Broker::new();
    let broker_tx = broker.sender();

    tokio::spawn(broker.run());

    loop {
        let (socket, addr) = listener.accept().await?;
        println!("Got a new socket from addr: {:?}", addr);

        let handler = client_handler(socket, broker_tx.clone());

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
