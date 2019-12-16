use crate::types::{DecodeError, Packet};
use bytes::BytesMut;
use futures::StreamExt;
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
    type Item = ();

    fn encode(&mut self, _data: Self::Item, _bytes: &mut BytesMut) -> Result<(), DecodeError> {
        Ok(())
    }
}

async fn client_handler(stream: TcpStream) {
    println!("Handling a client");

    let mut framed_sock = Framed::new(stream, MqttCodec::new());

    while let Some(frame) = framed_sock.next().await {
        match frame {
            Ok(frame) => println!("Got a frame: {:#?}", frame),
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
