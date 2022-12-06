use anyhow::Ok;
use clap::{arg, command, Parser, Subcommand};
use protobuf::{CodedInputStream, Message};

use rdeebee::wire_format::operation::Response;
use std::{env, str};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};
use tracing::Level;
use tracing_subscriber::FmtSubscriber;

mod rdeebee_client;
use rdeebee_client::*;

const SERVER_PORT: u16 = 2048;

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
    let trace_level = match env::var("TRACE_LEVEL")
        .expect("Trace level undefined")
        .as_str()
    {
        "TRACE" | "Trace" | "trace" => Level::TRACE,
        "INFO" | "Info" | "info" => Level::INFO,
        "DEBUG" | "Debug" | "debug" => Level::DEBUG,
        "WARN" | "Warn" | "warn" => Level::WARN,
        "ERROR" | "Error" | "error" => Level::ERROR,
        _ => Level::TRACE,
    };

    // Set up tracing.
    let subscriber = FmtSubscriber::builder()
        .with_max_level(trace_level)
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    let args = Args::parse();

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", SERVER_PORT)).await?;
    println!("Created a new stream");

    let mut sequencer = SequenceSvc::new().await;

    // let request = create_request(args.operation, &args.key, args.payload).await?;
    let request = sequencer
        .create_request(args.operation, &args.key, args.payload)
        .await?;

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
