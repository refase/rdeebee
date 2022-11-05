use std::time::Duration;

use actix_web::{http::header::HeaderMap, web, HttpRequest, HttpResponse, Responder};
use reqwest::{Client, ClientBuilder, Method, Request, Url};

use crate::{CONSUL_SERVER, SERVER_PORT};

use super::utils::respond;

#[derive(Clone)]
pub(crate) struct OutClient {
    outclient: Client,
}

impl OutClient {
    pub(crate) fn new() -> Self {
        Self {
            outclient: ClientBuilder::new()
                .http2_adaptive_window(true)
                .tcp_keepalive(Duration::new(150, 0))
                .tcp_nodelay(true) // disable Nagle
                // .connect_timeout(Duration::new(150, 0))
                .connection_verbose(true)
                .build()
                .expect("Failed creating out client pool"),
        }
    }
}

pub(crate) async fn handle_outgoing(
    request: HttpRequest,
    client: web::Data<OutClient>,
) -> impl Responder {
    let mut headermap = HeaderMap::new();
    for (key, val) in request.headers() {
        headermap.insert(key.clone(), val.clone());
    }

    let uri = match request.query_string() {
        "" => return HttpResponse::BadRequest().body("No query string found"),
        _ => Url::parse(&format!(
            "{}:{}?{}",
            CONSUL_SERVER,
            SERVER_PORT,
            request.query_string()
        ))
        .unwrap(),
    };

    let req = Request::new(Method::GET, uri);

    let handle = tokio::spawn(client.outclient.execute(req));

    let res = match handle.await {
        Ok(res) => match res {
            Ok(res) => res,
            Err(e) => {
                return HttpResponse::InternalServerError()
                    .body(format!("Error requesting path: {}", e))
            }
        },
        Err(e) => {
            return HttpResponse::InternalServerError().body(format!("Error running task: {}", e))
        }
    };

    respond(res).await
}
