use simple_pool::ResourcePool;
use simple_pool::ResourcePoolGuard;
use tokio::net::TcpStream;

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

}