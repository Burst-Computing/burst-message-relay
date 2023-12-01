pub const DEFAULT_SERVER_SEND_BUFFER_CAPACITY: usize = 1048576;
pub const DEFAULT_CLIENT_SEND_BUFFER_CAPACITY: usize = 1048576;
pub const DEFAULT_CLIENT_RECEIVE_BUFFER_CAPACITY: usize = 1048576;

#[derive(Clone, Copy)]
pub struct ServerConfig {
    pub send_buffer_capacity: usize,
}

impl Default for ServerConfig {
    fn default() -> ServerConfig {
        ServerConfig {
            send_buffer_capacity: DEFAULT_SERVER_SEND_BUFFER_CAPACITY,
        }
    }
}

#[derive(Clone, Copy)]
pub struct ClientConfig {
    pub send_buffer_capacity: usize,
    pub receive_buffer_capacity: usize,
}

impl Default for ClientConfig {
    fn default() -> ClientConfig {
        ClientConfig {
            send_buffer_capacity: DEFAULT_CLIENT_SEND_BUFFER_CAPACITY,
            receive_buffer_capacity: DEFAULT_CLIENT_RECEIVE_BUFFER_CAPACITY,
        }
    }
}
