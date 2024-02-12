use std::error::Error;

use burst_message_relay::client::{connection_pool::ConnectionPool, client::Client};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {

    let connection_pool = Arc::new(ConnectionPool::new("127.0.0.1:8000", 1));
    connection_pool.initialize_conns().await;

    let mut client = Client::new(connection_pool.clone());

    let random_bytes = vec![1; 1024 * 1024];

    let code = client.send(0, &random_bytes).await;
    println!("Send Code: {:?}", code);

    let buf = client.recv(0).await;
    println!("Data: {:?}", buf.len());

    connection_pool.close().await;

    Ok(())
}