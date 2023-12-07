use burst_message_relay::{client::client::Client, config::ClientConfig};

#[tokio::main]
pub async fn main() -> std::io::Result<()> {
    let mut client = Client::new("127.0.0.1:8000", ClientConfig::default());
    client.connect().await;

    let queues = vec![0, 1];
    let code = client.init_queues(&queues).await;

    println!("Init code: {:?}", code);

    let code = client.create_bc_group(String::from("0"), 2).await;

    println!("Broadcast Code: {:?}", code);

    client.close().await;

    let mut client_1 = Client::new("127.0.0.1:8000", ClientConfig::default());
    client_1.connect().await;

    let mut client_2 = Client::new("127.0.0.1:8000", ClientConfig::default());
    client_2.connect().await;

    let task = tokio::spawn(async move {

        for _ in 0..15 {
            let random_bytes = vec![1; 268435456 * 2];
            let d: &[&[u8]] = &[&[1, 2, 3, 4], &random_bytes];

            let code = client_1
                .broadcast_root_refs(String::from("0"), &d)
                .await;

            println!("Thread 1: Broadcast_root code: {:?}", code);

            let buf = client_1.broadcast(String::from("0")).await;

            println!("Thread 1: Broadcast data: {:?}", buf.len());
        }

        client_1.close().await;
    });

    let task2 = tokio::spawn(async move {
        for _ in 0..15 {
            let buf = client_2.broadcast(String::from("0")).await;

            println!("Thread 2: Broadcast data: {:?}", buf.len());
        }

        client_2.close().await;
    });

    let _ = task.await;
    let _ = task2.await; 

    Ok(())
}
