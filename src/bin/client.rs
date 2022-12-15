use anyhow::Ok;
use clap::{arg, command, Parser, Subcommand};
use protobuf::{CodedInputStream, Message};

use rdeebee::wire_format::operation::Response;
use std::{env, net::Ipv4Addr, str};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};
use tracing::Level;
use tracing_subscriber::FmtSubscriber;

mod rdeebee_client;
use rdeebee_client::*;

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

    let server_ip: Ipv4Addr = env::var("SERVER_IP")
        .expect("Server IP undefined")
        .parse()
        .expect("Unable to parse IP");
    let server_port = env::var("SERVER_PORT")
        .expect("Server port undefined")
        .parse::<u64>()
        .expect("Invalid server port");

    let mut stream = TcpStream::connect(format!("{server_ip}:{server_port}")).await?;
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
        let mut input_stream = CodedInputStream::from_bytes(&reply);
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
