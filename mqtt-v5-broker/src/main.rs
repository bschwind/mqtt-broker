use crate::{
    broker::{Broker, BrokerMessage},
    client::UnconnectedClient,
};
use bytes::BytesMut;
use futures::{stream, SinkExt, StreamExt};
use mqtt_v5::{
    codec::MqttCodec,
    encoder,
    types::{DecodeError, EncodeError, Packet, ProtocolVersion},
    websocket::WsUpgraderCodec,
};
use tokio::{
    net::{TcpListener, TcpStream},
    runtime::Runtime,
    sync::mpsc::Sender,
};
use tokio_util::codec::Framed;
use websocket_codec::{Message, MessageCodec};

mod broker;
mod client;
mod tree;

async fn client_handler(stream: TcpStream, broker_tx: Sender<BrokerMessage>) {
    println!("Handling a client");

    let (sink, stream) = Framed::new(stream, MqttCodec::new()).split();
    let unconnected_client = UnconnectedClient::new(stream, sink, broker_tx);

    let connected_client = match unconnected_client.handshake().await {
        Ok(connected_client) => connected_client,
        Err(err) => {
            println!("Protocol error during connection handshake: {:?}", err);
            return;
        },
    };

    connected_client.run().await;
}

pub async fn ws_upgrade(stream: TcpStream) -> Framed<TcpStream, MessageCodec> {
    let mut upgrade_framed = Framed::new(stream, WsUpgraderCodec::new());

    let upgrade_msg = upgrade_framed.next().await;
    println!("upgrade_msg: {:?}", upgrade_msg);

    if let Some(Ok(websocket_key)) = upgrade_msg {
        // Write the HTTP response
        let response = format!(
            "HTTP/1.1 101 Switching Protocols\r\n\
            Upgrade: websocket\r\n\
            Connection: Upgrade\r\n\
            Sec-WebSocket-Protocol: mqtt\r\n\
            Sec-WebSocket-Accept: {}\r\n\r\n",
            websocket_key
        );

        let _ = upgrade_framed.send(response).await;
    }

    let old_parts = upgrade_framed.into_parts();
    let mut new_parts = Framed::new(old_parts.io, MessageCodec::new()).into_parts();
    new_parts.read_buf = old_parts.read_buf;
    new_parts.write_buf = old_parts.write_buf;

    Framed::from_parts(new_parts)
}

async fn websocket_client_handler(stream: TcpStream, broker_tx: Sender<BrokerMessage>) {
    println!("Handling a WebSocket client");

    let ws_framed = ws_upgrade(stream).await;

    let (sink, ws_stream) = ws_framed.split();
    let sink = sink.with(|packet: Packet| {
        let mut payload_bytes = BytesMut::new();
        encoder::encode_mqtt(&packet, &mut payload_bytes, ProtocolVersion::V500);

        async {
            let result: Result<Message, EncodeError> = Ok(Message::binary(payload_bytes.freeze()));
            result
        }
    });

    let read_buf = BytesMut::with_capacity(4096);

    let stream =
        stream::unfold((ws_stream, read_buf), |(mut ws_stream, mut read_buf)| async move {
            // Loop until we've built up enough data from the WebSocket stream
            // to decode a new MQTT packet
            loop {
                // Try to read an MQTT packet from the read buffer
                match mqtt_v5::decoder::decode_mqtt(&mut read_buf, ProtocolVersion::V500) {
                    Ok(Some(packet)) => {
                        // If we got one, return it
                        return Some((Ok(packet), (ws_stream, read_buf)));
                    },
                    Err(e) => {
                        // If we had a decode error, propagate the error along the stream
                        return Some((Err(e), (ws_stream, read_buf)));
                    },
                    Ok(None) => {
                        // Otherwise we need more binary data from the WebSocket stream
                    },
                }

                let ws_frame = ws_stream.next().await;

                match ws_frame {
                    Some(Ok(message)) => {
                        if message.opcode() != websocket_codec::Opcode::Binary {
                            // MQTT Control Packets MUST be sent in WebSocket binary data frames
                            return Some((Err(DecodeError::BadTransport), (ws_stream, read_buf)));
                        }

                        read_buf.extend_from_slice(&message.into_data());
                    },
                    Some(Err(e)) => {
                        println!("Error while reading from WebSocket stream: {:?}", e);
                        // If we had a decode error in the WebSocket layer,
                        // propagate the it along the stream
                        return Some((Err(DecodeError::BadTransport), (ws_stream, read_buf)));
                    },
                    None => {
                        // The WebSocket stream is over, so we are too
                        return None;
                    },
                }
            }
        });

    futures::pin_mut!(stream);
    // futures::pin_mut!(sink);
    let unconnected_client = UnconnectedClient::new(stream, sink, broker_tx);

    let connected_client = match unconnected_client.handshake().await {
        Ok(connected_client) => connected_client,
        Err(err) => {
            println!("Protocol error during connection handshake: {:?}", err);
            return;
        },
    };

    connected_client.run().await;
}

async fn server_loop(broker_tx: Sender<BrokerMessage>) {
    let bind_addr = "0.0.0.0:1883";
    let mut listener = TcpListener::bind(bind_addr).await.expect("Couldn't bind to port 1883");

    println!("Listening on {}", bind_addr);

    loop {
        let (socket, addr) =
            listener.accept().await.expect("Error in server_loop 'listener.accept()");
        println!("Got a new socket from addr: {:?}", addr);

        let handler = client_handler(socket, broker_tx.clone());

        tokio::spawn(handler);
    }
}

async fn websocket_server_loop(broker_tx: Sender<BrokerMessage>) {
    let bind_addr = "0.0.0.0:8080";
    let mut listener = TcpListener::bind(bind_addr).await.expect("Couldn't bind to port 8080");

    println!("Listening on {}", bind_addr);

    loop {
        let (socket, addr) =
            listener.accept().await.expect("Error in websocket_server_loop 'listener.accept()");
        println!("Got a new socket from addr: {:?}", addr);

        let handler = websocket_client_handler(socket, broker_tx.clone());

        tokio::spawn(handler);
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Creating a Runtime does the following:
    // * Spawn a background thread running a Reactor instance.
    // * Start a ThreadPool for executing futures.
    // * Run an instance of Timer per thread pool worker thread.
    let mut runtime = Runtime::new()?;

    let broker = Broker::new();
    let broker_tx = broker.sender();
    runtime.spawn(broker.run());

    let server_future = server_loop(broker_tx.clone());
    let websocket_future = websocket_server_loop(broker_tx);

    runtime.spawn(websocket_future);
    runtime.block_on(server_future);

    Ok(())
}
