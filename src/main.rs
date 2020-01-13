use crate::{
    broker::{Broker, BrokerMessage},
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

async fn client_handler(stream: TcpStream, broker_tx: Sender<BrokerMessage>) {
    println!("Handling a client");

    let framed_sock = Framed::new(stream, MqttCodec::new());
    let unconnected_client = UnconnectedClient::new(framed_sock, broker_tx);

    let connected_client = match unconnected_client.handshake().await {
        Ok(connected_client) => connected_client,
        Err(err) => {
            println!("Protocol error during connection handshake: {:?}", err);
            return;
        },
    };

    connected_client.run().await;

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
