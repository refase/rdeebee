use std::io;

use actix_web::{
    web::{self, Data},
    App, HttpServer,
};
use rdeebee_gw::{connection_handler, RDeeBeeClientPool};

mod rdeebee_gw;

// const SERVER_PORT: u16 = 2048;
// const CONSUL_SERVER: &str = "consul-server.consul.svc.cluster.local";
const CONSUL_SERVER: &str = "localhost";
const CONSUL_TCP_PORT: u16 = 8500;
const GWPORT: u16 = 4096;

#[actix_web::main]
async fn main() -> io::Result<()> {
    std::env::set_var("RUST_LOG", "debug");
    std::env::set_var("RUST_BACKTRACE", "1");
    env_logger::init();
    let client = Data::new(RDeeBeeClientPool::new());

    println!("creating server");
    HttpServer::new(move || {
        App::new()
            .default_service(web::route().to(connection_handler))
            .app_data(client.clone())
    })
    .bind(format!("localhost:{}", GWPORT))?
    .run()
    .await
}
