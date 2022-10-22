use std::{error::Error, str};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

/// I decided to go with an HTTP server that provides a simply REST API to access the event store.
/// The API:
///     1. '/get/{key}': Get the latest event corresponding to the key if it exists.
///     2. '/stream/{key}': Subscribe to this key (TODO:)
///     3. '/add/

const SERVER_PORT: u16 = 2048;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", SERVER_PORT)).await?;
    println!("Created a new stream");

    let result = stream.write(b"Hello World!").await;
    println!("wrote to stream; success={:?}", result.is_ok());

    println!("awaiting reply...");

    let mut reply = vec![0; 1024];
    let n = stream
        .read(&mut reply)
        .await
        .expect("Error reading from server");
    if n != 0 {
        println!(
            "Data: {}",
            str::from_utf8(&reply[0..n]).expect("failed to read server data")
        );
    }

    Ok(())
}
