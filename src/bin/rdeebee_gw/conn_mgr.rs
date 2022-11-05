use std::{io, sync::Arc};

use protobuf::{CodedInputStream, Message};
use rdeebee::{
    wire_format::operation::{Request, Response, Status},
    ConsulRegister,
};
use rs_consul::{ConsulError, ServiceNode};
use tokio::{
    io::AsyncWriteExt,
    net::{tcp::OwnedReadHalf, TcpStream},
};

use crate::{CONSUL_SERVER, CONSUL_TCP_PORT};

#[derive(Clone)]
pub(crate) struct ConnMgr {
    consul: Arc<ConsulRegister>,
}

impl ConnMgr {
    pub(crate) fn new() -> Self {
        // let pod_ip = env::var("POD_IP").expect("Pod IP not defined");
        let pod_ip = "127.0.01".to_string();
        Self {
            consul: Arc::new(ConsulRegister::new(
                &format!("{}:{}", CONSUL_SERVER, CONSUL_TCP_PORT),
                pod_ip,
            )),
        }
    }

    pub async fn get_leaders(&self) -> Result<Vec<ServiceNode>, ConsulError> {
        self.consul.get_leaders().await
    }

    pub async fn get_node(&self) -> Result<ServiceNode, ConsulError> {
        self.consul.get_node().await
    }

    pub(crate) async fn read(&self, request: Request) -> Result<Vec<Response>, anyhow::Error> {
        let leaders = self.get_leaders().await?;
        let mut responses = Vec::new();
        for leader in leaders {
            let response = self.send_request(request.clone(), Some(leader)).await?;
            match response.status.enum_value() {
                Ok(status) => match status {
                    Status::Ok => responses.push(response),
                    _ => continue,
                },
                Err(err) => return Err(anyhow::anyhow!(err)),
            }
        }
        Ok(responses)
    }

    pub(crate) async fn send_request(
        &self,
        request: Request,
        node: Option<ServiceNode>,
    ) -> anyhow::Result<Response> {
        let node = match node {
            Some(node) => node,
            None => self.get_node().await.unwrap(),
        };
        let addr = format!("{}:{}", node.node.address, node.service.port);
        let stream = TcpStream::connect(addr).await.unwrap();
        let request_bytes = request.write_length_delimited_to_bytes().unwrap();

        let (stream_rx, mut stream_tx) = stream.into_split();
        let result = stream_tx.write(&request_bytes).await;
        println!("wrote to stream; success={:?}", result.is_ok());
        println!("awaiting reply...");
        let response = self.get_response(stream_rx).await?;
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
        Ok(response)
    }

    async fn get_response(&self, stream: OwnedReadHalf) -> anyhow::Result<Response> {
        let mut response = Response::new();

        println!("Reading response");
        loop {
            stream.readable().await?;
            let mut reply = Vec::with_capacity(4096);
            match stream.try_read_buf(&mut reply) {
                Ok(0) => {
                    break;
                }
                Ok(_n) => {
                    let mut input_stream = CodedInputStream::from_bytes(&mut reply);
                    response = input_stream.read_message()?;
                }
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                    continue;
                }
                Err(e) => {
                    return Err(e.into());
                }
            }
        }
        Ok(response)
    }
}
