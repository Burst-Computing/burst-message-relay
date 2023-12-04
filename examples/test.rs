use std::thread;
use std::time::{Instant, SystemTime};
use burst_message_relay::client::client::Client;
use burst_message_relay::config::ClientConfig;

const PAYLOAD: usize = 1024 * 1024 * 1; // 1 MB
const REPEAT: u32 = 256;
const BURST_SIZE: u32 = 64;

#[tokio::main]
pub async fn main() -> std::io::Result<()> {
    // Cliente -> init
    // TCP manager -> declare queues
    let mut client = Client::new("127.0.0.1:8000", ClientConfig::default());
    client.connect().await;

    let queues = [
        0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24,
        25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47,
        48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63
    ];

    let code = client.init_queues(&queues).await;
    println!("Queues Code: {:?}", code);

    client.close().await;

    let t1 = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap();
    println!("start: {}", t1.as_millis() as f64 / 1000.0);

    let mut handles = Vec::with_capacity(BURST_SIZE as usize);

    for i in 0..BURST_SIZE {
        let thread = thread::spawn(move || {
            println!("thread start: id={}", i);
            let tokio_runtime = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .unwrap();
            let result =
                tokio_runtime.block_on(async { worker(BURST_SIZE, i, PAYLOAD, REPEAT).await });
            println!("thread end: id={}", i);
            result
        });
        handles.push(thread);
    }

    let mut total_throughput = 0.0;

    for handle in handles {
        total_throughput += handle.join().unwrap();
    }

    println!("Total throughput: {} MB/s", total_throughput);

    let t2 = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap();
    println!("end: {}", t2.as_millis() as f64 / 1000.0);

    println!("Total time: {} s", (t2 - t1).as_millis() as f64 / 1000.0);

    Ok(())
}

async fn worker(burst_size: u32, worker_id: u32, payload: usize, repeat: u32) -> f64 {
    let mut client = Client::new("127.0.0.1:8000", ClientConfig::default());
    client.connect().await;

    println!("Worker {:?} started", worker_id);

    let throughput;
    let size_mb;
    // Receive
    if worker_id < (burst_size / 2) {
        let mut total_size = 0;

        let t0 = Instant::now();

        let msg = client.recv(worker_id as u32).await;
        total_size += msg.len();

        println!("Worker {} - started receiving", worker_id);
        for _ in 0..repeat - 1 {
            let msg = client.recv(worker_id as u32).await;
            total_size += msg.len();
        }

        println!("total_size: {:?}", total_size);
        let t = t0.elapsed();
        size_mb = total_size as f64 / 1024.0 / 1024.0;
        throughput = size_mb as f64 / (t.as_millis() as f64 / 1000.0);

        println!(
            "Worker {} - recv {} MB ({} messages) in {} s ({} MB/s)",
            worker_id,
            size_mb,
            repeat,
            t.as_millis() as f64 / 1000.0,
            throughput
        );
    }
    // Send
    else {
        let data = vec![b'x'; payload];
        let target = worker_id % (burst_size / 2);
        println!("Worker {} Sending to {}", worker_id, target);
        let t0 = Instant::now();
        for _ in 0..repeat {
            client.send(target as u32, &data.clone()).await;
        }
        let t = t0.elapsed();
        let total_size = data.len() * repeat as usize;
        size_mb = total_size as f64 / 1024.0 / 1024.0;
        throughput = size_mb as f64 / (t.as_millis() as f64 / 1000.0);
        println!(
            "Worker {} - sent {} MB ({} messages) in {} s ({} MB/s)",
            worker_id,
            size_mb,
            repeat,
            t.as_millis() as f64 / 1000.0,
            throughput
        );
    }

    client.close().await;

    println!("Worker {:?} finished", worker_id);

    throughput
}
