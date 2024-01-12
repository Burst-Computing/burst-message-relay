use std::error::Error;

use burst_message_relay::{client::{connection_pool::ConnectionPool, client::Client}, config::ClientConfig};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {

    let connection_pool = Arc::new(ConnectionPool::new("127.0.0.1:8000", 5));
    connection_pool.initialize_conns().await;

    let mut client_1 = Client::new(connection_pool.clone(), ClientConfig::default());

    let queues = vec![0, 1];

    let code = client_1.init_queues(&queues).await;

    println!("code: {:?}", code);

    let mut client_2 = Client::new(connection_pool.clone(), ClientConfig::default());

    let task = tokio::spawn(async move {
        let random_bytes = vec![1; 268435456 * 2];

        let code = client_1.send(0, &random_bytes).await;

        println!("Thread 1: Send Code: {:?}", code);

    });

    let task2 = tokio::spawn(async move {
        let buf = client_2.recv(0).await;

        println!("Thread 2: Receive data: {:?}", buf.len());

    });

    let _ = task.await;
    let _ = task2.await;

    connection_pool.close().await;

    Ok(())
}