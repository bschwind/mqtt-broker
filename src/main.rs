use tokio::{
    net::{TcpListener, TcpStream},
    prelude::*,
    runtime::Runtime,
};

async fn client_handler(mut stream: TcpStream) {
    println!("Handling a client");

    let mut buf = [0; 1024];

    loop {
        let n = match stream.read(&mut buf).await {
            Ok(n) if n == 0 => {
                println!("Read 0 bytes, we're out");
                return;
            },
            Ok(n) => {
                println!("read {} bytes", n);
                n
            },
            Err(err) => {
                println!("received an error while reading: {:?}", err);
                return;
            },
        };

        println!("data: {:?}", &buf[0..n]);
    }
}

async fn server_loop() -> Result<(), Box<dyn std::error::Error>> {
    let bind_addr = "0.0.0.0:1883";
    let mut listener = TcpListener::bind(bind_addr).await?;

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
    let runtime = Runtime::new()?;

    let server_future = server_loop();

    runtime.block_on(server_future)?;

    Ok(())
}
