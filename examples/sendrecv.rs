use tcp_version::{client::client::Client, config::ClientConfig};

#[tokio::main]
pub async fn main() -> std::io::Result<()> {
    let mut client = Client::new("127.0.0.1:8000", ClientConfig::default());
    client.connect().await;

    let queues = vec![0, 1];

    let code = client.init_queues(&queues).await;

    println!("Init_queues Code: {:?}", code);

    client.close().await;

    let mut client_1 = Client::new("127.0.0.1:8000", ClientConfig::default());
    client_1.connect().await;

    let mut client_2 = Client::new("127.0.0.1:8000", ClientConfig::default());
    client_2.connect().await;

    let task = tokio::spawn(async move {
        let random_bytes = vec![1; 268435456 * 2];

        let code = client_1.send(0, &random_bytes).await;

        println!("Thread 1: Send Code: {:?}", code);

        client_1.close().await;
    });

    let task2 = tokio::spawn(async move {
        let buf = client_2.recv(0).await;

        println!("Thread 2: Receive data: {:?}", buf.len());

        client_2.close().await;
    });

    let _ = task.await;
    let _ = task2.await;

    Ok(())
}
