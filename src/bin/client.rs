use anyhow::Ok;
use clap::{arg, command, Parser, Subcommand};
use protobuf::{CodedInputStream, EnumOrUnknown, Message};
use rdeebee::wire_format::operation::{Operation, Request, Response};
use serde::Deserialize;
use std::str;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

const SERVER_PORT: u16 = 2048;

#[derive(Debug, Deserialize)]
#[allow(non_snake_case)]
struct SeqStruct {
    Sequence: u64,
}

#[derive(Debug, Subcommand)]
enum Action {
    Read,
    Write,
    Delete,
}

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[command(subcommand)]
    operation: Action,
    #[arg(short, long)]
    key: String,
    #[arg(short, long)]
    payload: Option<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", SERVER_PORT)).await?;
    println!("Created a new stream");

    let request = create_request(args.operation, &args.key, args.payload).await?;

    let request_bytes = request.write_length_delimited_to_bytes()?;

    let result = stream.write(&request_bytes).await;

    println!("wrote to stream; success={:?}", result.is_ok());

    println!("awaiting reply...");

    let mut reply = vec![0; 1024];
    let n = stream
        .read(&mut reply)
        .await
        .expect("Error reading from server");
    if n != 0 {
        let mut input_stream = CodedInputStream::from_bytes(&mut reply);
        let response: Response = input_stream.read_message().expect("failed to read back");
        println!("Response:");
        println!("\tResponse Key: {:#?}", response.key);
        println!(
            "\tResponse Operation: {:#?}",
            response.op.enum_value().unwrap()
        );
        println!("\tResponse Status: {:#?}", response.status);
        if !response.payload.is_empty() {
            let payload: String = bincode::deserialize(&response.payload).unwrap();
            println!("\tPayload: {}", payload);
        }
    }

    Ok(())
}

async fn create_request(
    action: Action,
    key: &str,
    payload: Option<String>,
) -> anyhow::Result<Request> {
    let mut request = Request::new();
    request.key = key.to_string();
    request.op = match action {
        Action::Read => EnumOrUnknown::new(Operation::Read),
        Action::Write => EnumOrUnknown::new(Operation::Write),
        Action::Delete => EnumOrUnknown::new(Operation::Delete),
    };
    let seq: SeqStruct = match action {
        Action::Write | Action::Delete => {
            let seq_str = reqwest::get("http://localhost:8080").await?.text().await?;
            println!("Sequence string: {}", &seq_str);
            serde_json::from_str(&seq_str)?
        },
        Action::Read => SeqStruct { Sequence: 0 },
    };

    request.seq = seq.Sequence;
    
    if let Some(payload) = payload {
        let payload = bincode::serialize(&payload)?;
        request.payload = payload;
    }
    Ok(request)
}
