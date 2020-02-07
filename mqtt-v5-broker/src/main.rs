use crate::{
    broker::{Broker, BrokerMessage},
    client::UnconnectedClient,
};
use futures::StreamExt;
use mqtt_v5::codec::MqttCodec;
use tokio::{
    net::{TcpListener, TcpStream},
    runtime::Runtime,
    sync::mpsc::Sender,
};
use tokio_util::codec::Framed;

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
