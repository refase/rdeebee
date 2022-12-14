use std::{
    borrow::{Borrow, BorrowMut},
    collections::VecDeque,
    env, str,
    sync::Arc,
};

use anyhow::anyhow;
use parking_lot::RwLock;
use protobuf::{CodedInputStream, EnumOrUnknown, Message};
use rdeebee::wire_format::operation::{Operation, Request, Response, Status};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
};
use tracing::{error, info, Level};
use tracing_subscriber::FmtSubscriber;

use crate::rdeebee_server::RDeeBeeServer;

mod rdeebee_server;

const PORT: u16 = 2048;
const DEEBEE_FOLDER: &str = "/tmp/rdeebee";
// const COMPACTION_SIZE: usize = 2048;
const COMPACTION_SIZE: usize = 500;
const QUEUE_CAPACITY: usize = 500;

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

    let addr = format!("127.0.0.1:{}", PORT);

    let rdb_srv = match RDeeBeeServer::new(COMPACTION_SIZE, DEEBEE_FOLDER.to_string()).await {
        Ok(rdb_srv) => rdb_srv,
        Err(e) => return Err(e),
    };

    // Start the cluster node
    let node = rdb_srv.get_node();
    std::thread::spawn(move || {
        info!("Starting cluster thread");
        let rt = tokio::runtime::Runtime::new().expect("Failed to start server runtime");
        let mut node = node.as_ref().borrow_mut().write();
        rt.block_on(async move {
            node.run_cluster_node().await.unwrap();
        })
    });

    match rdb_srv.is_leader() {
        true => info!("Node is leader"),
        false => info!("Node is non-leading member"),
    }

    // Recover the system.
    // Assume the directory is empty or doesn't exist for a new system.
    rdb_srv.recover()?;

    // TODO: can we do away with this locking system?
    // The other option is to use message passing.
    // Ideally we want a data structure that allows independent access to the two ends.
    let event_queue = Arc::new(RwLock::new(VecDeque::with_capacity(QUEUE_CAPACITY)));

    // We would have wanted to monitor the MemTable.
    // Check the size and see if compaction size is reached whenever new data is added.
    // But that is difficult without making MemTable public.
    // So as a workaround we poll the wal file.
    // Any new add, is also added to the wal file as well and can tell us to check the MemTable size for compaction.
    // Alternatively, we can send a message to the thread to check for compaction when an event is received.
    // Otherwise, we could spawn an async context when a new event arrives to check for compaction.
    // But that has two issues:
    //      1. It needs MemTable in `RDeeBee` to be wrapped in `Arc<RwLock>>` which is at least some
    //          performance hit!
    //      2. If one event arrives close to another and two async contexts are generated?
    //          What conflicts does that create?
    // TODO: which method is better?
    let (compaction_sender, compaction_receiver) = unbounded_channel::<bool>();
    let rdb_compaction = rdb_srv.clone();
    let compaction_handler = compaction_thread(rdb_compaction, compaction_receiver);

    // Start the thread to add events to database.
    let rdb_get = rdb_srv.clone();
    let event_queue_get = event_queue.clone();

    let (event_sender, event_receiver) = unbounded_channel::<bool>();

    let db_add_handler = add_events_to_db(rdb_get, event_queue_get, event_receiver);

    let listener = TcpListener::bind(&addr).await?;
    info!("Server started on: {}", &listener.local_addr().unwrap());

    let rdb_srv_clone = rdb_srv.clone();
    let main_thrd = main_task(
        listener,
        rdb_srv_clone,
        event_sender,
        compaction_sender,
        event_queue,
    );

    let results = tokio::join!(compaction_handler, db_add_handler, main_thrd);
    if let Err(e) = results.0 {
        return Err(anyhow!("{}", e));
    }
    if let Err(e) = results.2 {
        return Err(anyhow!("{}", e));
    }
    Ok(())
}

async fn main_task(
    listener: TcpListener,
    rdb: RDeeBeeServer,
    event_sender: UnboundedSender<bool>,
    compaction_sender: UnboundedSender<bool>,
    event_queue: Arc<RwLock<VecDeque<Request>>>,
) -> anyhow::Result<()> {
    loop {
        let (socket, _) = listener.accept().await?;
        let event_notifier = event_sender.clone();
        let rdb_clone = rdb.clone();
        let compaction_notifier = compaction_sender.clone();
        let event_queue = event_queue.clone();
        tokio::spawn(async move {
            handle_client(
                socket,
                rdb_clone,
                event_queue,
                event_notifier,
                compaction_notifier,
            )
            .await;
        });
    }
}

/// Add events to DB when notified about arrival of new event.
/// Uses the Write Lock.
async fn add_events_to_db(
    rdb: RDeeBeeServer,
    event_queue: Arc<RwLock<VecDeque<Request>>>,
    mut event_notifier_receiver: UnboundedReceiver<bool>,
) {
    while let Some(event_added) = event_notifier_receiver.recv().await {
        println!("Event notification received");
        if !event_added {
            continue;
        }
        match event_queue.as_ref().borrow_mut().try_write() {
            Some(mut queue_guard) => {
                while !queue_guard.is_empty() {
                    match queue_guard.pop_front() {
                        Some(event) => {
                            match event.op.enum_value() {
                                Ok(op) => match op {
                                    Operation::Write =>
                                    // TODO: retry???
                                    {
                                        if let Err(e) = rdb.add_event(event) {
                                            error!("failed to add event: {}", e);
                                        }
                                    }
                                    Operation::Delete =>
                                    // TODO: retry???
                                    {
                                        if let Err(e) = rdb.delete_event(event) {
                                            error!("failed to delete event: {}", e);
                                        }
                                    }
                                    Operation::Read => {}
                                },
                                Err(_) => error!("failed to get operation from event"),
                            }
                        }
                        None => error!("failed to get event from array"),
                    }
                }
            }
            None => error!("failed to get event from array",),
        }
        println!("Database MemTable size: {:#?}", rdb.get_memtable_size());
    }
}

/// Run a compaction thread in the background.
async fn compaction_thread(
    rdb: RDeeBeeServer,
    mut compaction_receiver: UnboundedReceiver<bool>,
) -> anyhow::Result<()> {
    while let Some(event_added) = compaction_receiver.recv().await {
        if !event_added {
            continue;
        }
        // If new events have arrived, check the size of the MemTable.
        // And compact the MemTable if needed.
        println!("size: {}", rdb.get_memtable_size().unwrap());
        if rdb.get_memtable_size().is_some() && rdb.get_memtable_size().unwrap() > COMPACTION_SIZE {
            println!("compacting");
            rdb.compact_memtable()?;
            rdb.compact_sstables()?;
        }
    }
    Ok(())
}

/// Handles each client and provides a response.
/// Uses the Read lock if a READ operation is received.
async fn handle_client(
    mut socket: TcpStream,
    rdb: RDeeBeeServer,
    event_queue: Arc<RwLock<VecDeque<Request>>>,
    event_notifier: UnboundedSender<bool>,
    compaction_notifier: UnboundedSender<bool>,
) {
    let mut buf = vec![0; 1024];
    let mut raw = Vec::new();

    loop {
        let n = socket
            .read(&mut buf)
            .await
            .expect("failed to read from socket");

        for elem in &buf {
            raw.push(*elem);
        }
        if n < 1024 {
            break;
        }
    }

    let request: Request;

    // Ensure we coded input stream goes out of scope before the next await is hit.
    {
        let mut input_stream = CodedInputStream::from_bytes(&raw);
        request = match input_stream.read_message() {
            Ok(request) => request,
            Err(e) => {
                println!("error reading request: {}", e);
                return;
            }
        };
    }

    // build the response here
    let mut response = Response::new();
    response.key = request.key.clone();
    response.op = request.op;

    match request.op.enum_value() {
        Ok(op) => match op {
            Operation::Delete | Operation::Write => {
                let mut event_added = false;
                // Do we want a retry logic instead of failing the request?
                match event_queue.as_ref().borrow_mut().try_write() {
                    Some(mut arr_write_guard) => {
                        arr_write_guard.push_back(request);
                        info!("added event");
                        event_added = true;
                        response.status = EnumOrUnknown::new(Status::Ok);
                    }
                    None => {
                        error!("failed to get array lock for key: {}", response.key.clone());
                        response.status = EnumOrUnknown::new(Status::Server_Error);
                    }
                };

                println!(
                    "Event Queue Length: {}",
                    event_queue.as_ref().borrow().read().len()
                );

                match event_notifier.send(event_added) {
                    Ok(_) => println!("sending"),
                    Err(e) => println!("didn't send: {}", e),
                }

                match compaction_notifier.send(event_added) {
                    Ok(_) => println!("sending"),
                    Err(e) => println!("didn't send: {}", e),
                }

                send_response(socket, response).await;
            }
            Operation::Read => {
                match rdb.get_event(&request.key) {
                    Some(response) => send_response(socket, response).await,
                    None => {
                        response.status = EnumOrUnknown::new(Status::Server_Error);
                        send_response(socket, response).await;
                    }
                };
            }
        },
        Err(e) => {
            error!("error getting operation: {}", e);
            response.status = EnumOrUnknown::new(Status::Server_Error);
            send_response(socket, response).await;
        }
    }
}

async fn send_response(mut socket: TcpStream, response: Response) {
    let response_bytes = response.write_length_delimited_to_bytes().unwrap();
    let result = socket.write(&response_bytes).await.unwrap();
    println!("wrote to stream; success={}", result);
}
