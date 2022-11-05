use actix_web::{error, web, Error, HttpRequest, HttpResponse};
use futures::StreamExt;
use protobuf::EnumOrUnknown;
use rdeebee::wire_format::operation::{Operation, Request};
use reqwest::header::{self};

use serde::{Deserialize, Serialize};

use super::conn_mgr::ConnMgr;

#[derive(Clone)]
pub(crate) struct RDeeBeeClientPool {
    // pool: Client,
    conn_mgr: ConnMgr,
}

impl RDeeBeeClientPool {
    pub(crate) fn new() -> Self {
        let mut headers = header::HeaderMap::new();
        headers.insert("X-Forwarded-For", header::HeaderValue::from_static(""));
        Self {
            conn_mgr: ConnMgr::new(),
        }
    }
}

#[derive(Debug, Deserialize, PartialEq)]
enum Action {
    Read,
    Write,
    Delete,
}

#[derive(Debug, Deserialize)]
pub(crate) struct Info {
    action: Action,
    key: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct DbObj {
    payload: Vec<u8>,
}

const MAX_SIZE: usize = 262_144; // max payload size is 256k

pub(crate) async fn connection_handler(
    request: HttpRequest,
    client: web::Data<RDeeBeeClientPool>,
    info: web::Query<Info>,
    mut payload: web::Payload,
) -> Result<HttpResponse, Error> {
    let query = request.query_string().to_string();
    println!("Query:{}", query);

    match info.action {
        Action::Delete | Action::Write => {
            let mut request = Request::new();
            if info.action == Action::Delete {
                request.op = EnumOrUnknown::new(Operation::Delete);
            } else {
                request.op = EnumOrUnknown::new(Operation::Write);
            }
            request.key = info.key.clone();

            let mut body = web::BytesMut::new();
            while let Some(chunk) = payload.next().await {
                let chunk = chunk?;
                // limit max size of in-memory payload
                if body.len() + chunk.len() > MAX_SIZE {
                    return Err(error::ErrorBadRequest("overflow"));
                }
                body.extend_from_slice(&chunk);
            }

            request.payload = body.to_vec();

            match client.conn_mgr.send_request(request, None).await {
                Ok(response) => Ok(HttpResponse::Ok().body(format!("{}", response))),
                Err(e) => Err(error::ErrorBadRequest(e)),
            }
        }
        Action::Read => {
            let mut request = Request::new();
            request.op = EnumOrUnknown::new(Operation::Read);
            request.key = info.key.clone();
            match client.conn_mgr.read(request).await {
                Ok(response) => Ok(HttpResponse::Ok().body(format!("{:#?}", response))),
                Err(e) => Err(error::ErrorBadRequest(e)),
            }
        }
    }
}
