use std::env;
use std::error::Error;

use burst_message_relay::config::ServerConfig;
use burst_message_relay::server::server::Server;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    env::set_var("RUST_BACKTRACE", "full");
    env_logger::init();
    let addr = "127.0.0.1:8000";

    let mut server = Server::new(addr, ServerConfig::default()).await;

    let sender = server.start_manager().await;

    loop {
        let manager_sender = sender.clone();

        server.start_client(manager_sender).await;
    }

    //Ok(())
}
