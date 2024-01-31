use simple_pool::ResourcePool;
use simple_pool::ResourcePoolGuard;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

use crate::protocol::{ClientOperation, ServerResponse};

pub struct ConnectionPool {
    conns: ResourcePool<TcpStream>,
    end_point: String,
    max_connections: u32,
}

impl ConnectionPool {
    pub fn new(end_point: &str, max_connections: u32) -> ConnectionPool {
        ConnectionPool {
            conns: ResourcePool::new(),
            end_point: String::from(end_point),
            max_connections,
        }
    }

    pub async fn initialize_conns(&self) {
        for _ in 0..self.max_connections {
            self.conns.append(self.connect().await);
        }
    }

    async fn connect(&self) -> TcpStream {
        println!("Endpoint: {:?}", self.end_point);
        match TcpStream::connect(&self.end_point).await {
            Ok(connection) => {
                connection.set_nodelay(true).unwrap();
                return connection;
            }
            Err(_) => panic!(),
        }
    }

    pub async fn get_connection(&self) -> ResourcePoolGuard<TcpStream> {
        return self.conns.get().await;
    }

    pub async fn close(&self) {
        for _ in 0..self.max_connections {
            let mut conn = self.conns.get().await;
            conn.write_u32(ClientOperation::Close as u32).await.unwrap();
            conn.flush().await.unwrap();

            let sv_code = conn.read_u32().await.unwrap();

            if sv_code == ServerResponse::Close as u32 {
                conn.forget_resource();
            }

            drop(conn);
        }
    }
}
