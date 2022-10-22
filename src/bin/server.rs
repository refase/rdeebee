use std::str;

use anyhow::anyhow;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

use crate::rdeebee_server::RDeeBeeServer;

mod rdeebee_server;

const PORT: u16 = 2048;
const DEEBEE_FOLDER: &str = "/tmp/rdeebee";
const COMPACTION_SIZE: usize = 2048;

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    println!("{}", concat!(env!("OUT_DIR"), "/protos"));
    let addr = format!("127.0.0.1:{}", PORT);

    let rdb_srv = match RDeeBeeServer::new(COMPACTION_SIZE, DEEBEE_FOLDER.to_string()) {
        Some(rdb_srv) => rdb_srv,
        None => return Err(anyhow!("failed to create rdeebee")),
    };

    println!("Server started on: {}", &addr);

    let listener = TcpListener::bind(&addr).await?;

    loop {
        let (socket, _) = listener.accept().await?;
        tokio::spawn(async move {
            handle_client(socket).await;
        });
    }
}

async fn handle_client(mut socket: TcpStream) {
    let mut buf = vec![0; 1024];
    loop {
        let n = socket
            .read(&mut buf)
            .await
            .expect("failed to read from socket");
        if n == 0 {
            return;
        }

        println!(
            "Data: {}",
            str::from_utf8(&buf[0..n]).expect("failed to read client data")
        );
        let result = socket.write(&buf[0..n]).await;
        println!("wrote to stream; success={:?}", result.is_ok());
    }
}

#[cfg(test)]
mod test {}
