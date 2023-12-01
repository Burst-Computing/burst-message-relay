use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::sync::mpsc::Sender;

use crate::config::ServerConfig;
use crate::server::client;
use crate::server::enums::ToManager;
use crate::server::manager;

pub struct Server {
    tcp_socket: TcpListener,
    client_id: u32,
    config: ServerConfig,
}

impl Server {
    pub async fn new(addr: &str, config: ServerConfig) -> Server {
        let tcp_addr = String::from(addr);
        let listener = Self::create_tcp(tcp_addr.clone());

        Server {
            tcp_socket: listener.await,
            client_id: 0,
            config: config,
        }
    }

    async fn create_tcp(addr: String) -> TcpListener {
        println!("Server: Listening on: {}", addr);
        TcpListener::bind(&addr).await.unwrap()
    }

    pub async fn start_manager(&self) -> Sender<ToManager> {
        // Create channel to communicate Clients Threads between Manager Server
        let (manager_sender, manager_receiver) = mpsc::channel(1024);

        // Start Manager thread
        tokio::spawn(async move {
            manager::start(manager_receiver)
                .await
                .expect("Manager Connection Failed");
        });

        // Return Producer Channel
        manager_sender
    }

    pub async fn start_client(&mut self, manager_sender: Sender<ToManager>) {
        // Accept connection request
        match self.accept().await {
            Some(tcp_stream) => {
                let id = self.client_id.clone();
                let server_config = self.config.clone();

                // Start Client thread
                tokio::spawn(async move {
                    //println!("Client thread spawned");
                    client::process_task(id, tcp_stream, manager_sender, server_config)
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
