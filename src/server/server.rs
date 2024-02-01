use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio::sync::broadcast::{Receiver as BroadcastReceiver, Sender as BroadcastSender};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::Mutex;

use crate::server::worker;

use dashmap::DashMap;
use std::collections::HashMap;
use std::sync::Arc;

pub struct ServerDataStructures {
    send_store: Arc<DashMap<u32, Arc<Sender<Arc<Vec<u8>>>>>>,
    recv_store: Arc<DashMap<u32, Arc<Mutex<Receiver<Arc<Vec<u8>>>>>>>,
    broadcast_send_store: Arc<DashMap<String, Arc<BroadcastSender<Arc<Vec<u8>>>>>>,
    broadcast_recv_store:
        Arc<DashMap<String, DashMap<u32, Arc<Mutex<BroadcastReceiver<Arc<Vec<u8>>>>>>>>,
    bgroup_counter: Arc<Mutex<HashMap<String, u32>>>,
}

pub struct Server {
    tcp_socket: TcpListener,
    client_id: u32,
    data_structures: Arc<ServerDataStructures>,
}

impl Server {
    pub async fn new(addr: &str) -> Server {
        let tcp_addr = String::from(addr);
        let listener = Self::create_tcp(tcp_addr.clone());

        Server {
            tcp_socket: listener.await,
            client_id: 0,
            data_structures: Arc::new(ServerDataStructures {
                send_store: Arc::new(DashMap::new()),
                recv_store: Arc::new(DashMap::new()),
                broadcast_send_store: Arc::new(DashMap::new()),
                broadcast_recv_store: Arc::new(DashMap::new()),
                bgroup_counter: Arc::new(Mutex::new(HashMap::new())),
            }),
        }
    }

    async fn create_tcp(addr: String) -> TcpListener {
        println!("Server: Listening on: {}", addr);
        TcpListener::bind(&addr).await.unwrap()
    }

    pub async fn start_worker(&mut self) {
        // Accept connection request
        match self.accept().await {
            Some(tcp_stream) => {
                let id = self.client_id.clone();
                let data_struct = self.data_structures.clone();

                // Start Client thread
                tokio::spawn(async move {
                    //println!("Client thread spawned");
                    worker::worker_task(
                        id,
                        tcp_stream,
                        data_struct.send_store.clone(),
                        data_struct.recv_store.clone(),
                        data_struct.broadcast_send_store.clone(),
                        data_struct.broadcast_recv_store.clone(),
                        data_struct.bgroup_counter.clone(),
                    )
                    .await
                    .expect("Client Connection Failed");
                });

                self.client_id += 1;
            }
            None => {
                println!("No tcp_stream");
            }
        }
    }

    async fn accept(&mut self) -> Option<TcpStream> {
        match self.tcp_socket.accept().await {
            Ok((stream, addr)) => {
                println!("Server: Accepted request from {:?}", addr);
                Some(stream)
            }
            Err(err) => {
                println!("Err: {:?}", err);
                None
            }
        }
    }
}
