use tcp_version::client::client::Client;
use tcp_version::config::ClientConfig;

#[tokio::main]
pub async fn main() -> std::io::Result<()> {
    let mut client = Client::new("127.0.0.1:8000", ClientConfig::default());
    client.connect().await;

    let queues = vec![0];

    let code = client.init_queues(&queues).await;

    println!("Init_queues Code: {:?}", code);

    client.close().await;

    let mut client = Client::new("127.0.0.1:8000", ClientConfig::default());
    client.connect().await;

    let task = tokio::spawn(async move {
        let random_bytes = vec![1; 268435456 * 2];
        let d: &[&[u8]] = &[&[1, 2, 3, 4], &random_bytes];

        let code = client.send_refs(0, d).await;

        println!("Thread 1: Send Code: {:?}", code);

        let buf = client.recv(0).await;

        println!("Thread 1: Receive: {:?}", buf.len());

        client.close().await;
    });

    let _ = task.await;

    Ok(())
}
