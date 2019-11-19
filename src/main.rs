use crate::types::{Packet, PacketType, ParseError};
use std::convert::TryFrom;
use tokio::{
    net::{TcpListener, TcpStream},
    prelude::*,
    runtime::Runtime,
};

mod types;

fn encode_variable_int(value: u32, buf: &mut [u8]) -> usize {
    let mut x = value;
    let mut byte_counter = 0;

    loop {
        let mut encoded_byte: u8 = (x % 128) as u8;
        x = x / 128;

        if x > 0 {
            encoded_byte = encoded_byte | 128;
        }

        buf[byte_counter] = encoded_byte;

        byte_counter += 1;

        if x <= 0 {
            break;
        }
    }

    byte_counter
}

fn decode_variable_int(bytes: &[u8]) -> Result<Option<(u32, usize)>, ParseError> {
    if bytes.len() < 1 {
        Ok(None)
    } else {
        let mut multiplier = 1;
        let mut value: u32 = 0;
        let mut iterator = bytes.iter();
        let mut consumed_bytes: usize = 0;

        loop {
            let encoded_byte;
            match iterator.next() {
                Some(n) => {
                    encoded_byte = n;
                },
                None => return Ok(None),
            }

            consumed_bytes += 1;

            value += ((encoded_byte & 0b01111111) as u32) * multiplier;
            multiplier *= 128;

            if multiplier > (128 * 128 * 128) {
                return Err(ParseError::InvalidRemainingLength);
            }

            if encoded_byte & 0b10000000 == 0b00000000 {
                break;
            }
        }

        Ok(Some((value, consumed_bytes)))
    }
}

fn parse_mqtt(bytes: &[u8]) -> Result<Option<Packet>, ParseError> {
    if bytes.len() < 2 {
        return Ok(None);
    }

    let first_byte = bytes[0];
    let bytes = &bytes[1..];
    let first_byte_val = (first_byte & 0b11110000) >> 4;
    let packet_type = PacketType::try_from(first_byte_val)?;

    println!("got packet type: {:?}", packet_type);

    let remaining_length_opt = decode_variable_int(&bytes)?;

    if remaining_length_opt.is_none() {
        return Ok(None);
    }

    let (remaining_length, consumed_bytes) = remaining_length_opt.unwrap();
    let bytes = &bytes[consumed_bytes..];

    println!("remaining_length is {}", remaining_length);

    println!("remaining bytes: len={}, data={:?}", bytes.len(), bytes);

    let packet = Packet::new(packet_type, bytes);

    Ok(Some(packet))
}

async fn client_handler(mut stream: TcpStream) {
    println!("Handling a client");

    let mut buf = [0; 1024];
    let mut buf_offset = 0;

    loop {
        let n = match stream.read(&mut buf[buf_offset..]).await {
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

        match parse_mqtt(&buf[0..(buf_offset + n)]) {
            Ok(None) => {
                buf_offset += n;
                continue;
            },
            Ok(Some(packet)) => {
                // Do stuff with packet
                println!("{:?}", packet);
                buf_offset = 0;
            },
            Err(err) => {
                println!("Error parsing MQTT packet: {:?}", err);
                buf_offset += n;
                continue;
            },
        }
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
