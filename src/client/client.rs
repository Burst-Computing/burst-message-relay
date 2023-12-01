use log::debug;

use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

use crate::config::ClientConfig;
use crate::protocol::{ClientOperation, ServerResponse};

pub struct Client {
    socket_addr: String,
    tcp_stream: Option<TcpStream>,
    config: ClientConfig,
}

impl Client {
    pub fn new(addr: &str, config: ClientConfig) -> Client {
        Client {
            socket_addr: String::from(addr),
            tcp_stream: None,
            config: config,
        }
    }

    pub async fn connect(&mut self) -> i32 {
        match TcpStream::connect(&self.socket_addr).await {
            Ok(connection) => {
                connection.set_nodelay(true).unwrap();
                self.tcp_stream = Some(connection);
            }
            Err(_) => panic!(),
        }

        1
    }

    pub async fn init_queues(&mut self, data: &[u32]) -> u32 {
        self.init_operation(ClientOperation::InitQueue, None, None, data)
            .await
    }

    pub async fn create_bc_group(&mut self, group_name: String, data: &[u32]) -> u32 {
        self.init_operation(
            ClientOperation::CreateBcGroup,
            None,
            Some(group_name.as_bytes()),
            data,
        )
        .await
    }

    pub async fn send(&mut self, queue_id: u32, data: &[u8]) -> u32 {
        self.write_tcp(ClientOperation::Send, Some(queue_id), None, data)
            .await
    }

    pub async fn send_refs(&mut self, queue_id: u32, data: &[&[u8]]) -> u32 {
        self.write_tcp_refs(ClientOperation::Send, Some(queue_id), None, data)
            .await
    }

    pub async fn recv(&mut self, queue_id: u32) -> Vec<u8> {
        self.read_tcp(ClientOperation::Receive, Some(queue_id), None)
            .await
    }

    pub async fn broadcast_root(&mut self, group_name: String, data: &[u8]) -> u32 {
        self.write_tcp(
            ClientOperation::BroadcastRoot,
            None,
            Some(group_name.as_bytes()),
            data,
        )
        .await
    }

    pub async fn broadcast(&mut self, group_name: String) -> Vec<u8> {
        self.read_tcp(
            ClientOperation::Broadcast,
            None,
            Some(group_name.as_bytes()),
        )
        .await
    }

    pub async fn close(&mut self) -> ServerResponse {
        let stream_option = &mut self.tcp_stream;

        let stream = stream_option.as_mut().unwrap();

        Self::identify_operation(stream, ClientOperation::Close, None, None).await
    }

    async fn identify_operation(
        stream: &mut TcpStream,
        op_id: ClientOperation,
        queue_id: Option<u32>,
        group_name: Option<&[u8]>,
    ) -> ServerResponse {
        stream.write_u32(op_id as u32).await.unwrap();
        stream.flush().await.unwrap();

        if op_id == ClientOperation::Send || op_id == ClientOperation::Receive {
            stream.write_u32(queue_id.unwrap()).await.unwrap();
            stream.flush().await.unwrap();
        } else if op_id == ClientOperation::CreateBcGroup
            || op_id == ClientOperation::BroadcastRoot
            || op_id == ClientOperation::Broadcast
        {
            stream.write_all(group_name.unwrap()).await.unwrap();
            stream.flush().await.unwrap();
        }

        let sv_code = stream.read_u32().await.unwrap();

        debug!(
            "Client - Identify Operation: Server returned {:?} for operation ID: {:?}",
            sv_code, op_id
        );

        sv_code.into()
    }

    async fn init_operation(
        &mut self,
        protocol: ClientOperation,
        group_id: Option<u32>,
        group_name: Option<&[u8]>,
        queues: &[u32],
    ) -> u32 {
        let mut response: u32 = 999999;

        let stream_option = &mut self.tcp_stream;

        let stream = stream_option.as_mut().unwrap();

        if Self::identify_operation(stream, protocol, group_id, group_name).await
            == ServerResponse::Accepted
        {
            // Header
            let len = queues.len() as u32;

            stream.write_u32(len).await.unwrap();

            for slice in queues {
                stream.write_u32(*slice).await.unwrap();
            }
            stream.flush().await.unwrap();

            response = stream.read_u32().await.unwrap();
        }

        response
    }

    async fn write_tcp(
        &mut self,
        protocol: ClientOperation,
        queue_id: Option<u32>,
        group_name: Option<&[u8]>,
        data: &[u8],
    ) -> u32 {
        let mut response: u32 = 999999;

        let stream_option = &mut self.tcp_stream;

        let stream = stream_option.as_mut().unwrap();

        if Self::identify_operation(stream, protocol, queue_id, group_name).await
            == ServerResponse::Accepted
        {
            // Header
            let len;
            if protocol == ClientOperation::InitQueue || protocol == ClientOperation::CreateBcGroup
            {
                len = (data.len() / 4) as u32;
            } else {
                len = data.len() as u32;
            }

            stream.write_u32(len).await.unwrap();

            let iterations = data.len() / self.config.send_buffer_capacity;

            if iterations > 0 {
                for i in 0..iterations {
                    stream
                        .write_all(
                            &data[self.config.send_buffer_capacity * i
                                ..self.config.send_buffer_capacity * (i + 1)],
                        )
                        .await
                        .unwrap();
                }

                stream
                    .write_all(&data[self.config.send_buffer_capacity * iterations..data.len()])
                    .await
                    .unwrap();
            } else {
                stream.write_all(&data).await.unwrap();
            }
            stream.flush().await.unwrap();

            response = stream.read_u32().await.unwrap();
        }

        response
    }

    async fn write_tcp_refs(
        &mut self,
        protocol: ClientOperation,
        queue_id: Option<u32>,
        group_name: Option<&[u8]>,
        data: &[&[u8]],
    ) -> u32 {
        let mut response: u32 = 999999;

        let stream_option = &mut self.tcp_stream;

        let stream = stream_option.as_mut().unwrap();

        if Self::identify_operation(stream, protocol, queue_id, group_name).await
            == ServerResponse::Accepted
        {
            // Header
            let mut len = 0;
            for slice in data {
                len += slice.len() as u32;
            }

            //Send total len
            stream.write_u32(len).await.unwrap();

            for slice in data {
                let iterations = slice.len() / self.config.send_buffer_capacity;

                if iterations > 0 {
                    for i in 0..iterations {
                        let index = i as usize;

                        stream
                            .write_all(
                                &slice[self.config.send_buffer_capacity * index
                                    ..self.config.send_buffer_capacity * (index + 1)],
                            )
                            .await
                            .unwrap();
                    }

                    let iter = iterations as usize;
                    stream
                        .write_all(&slice[self.config.send_buffer_capacity * iter..slice.len()])
                        .await
                        .unwrap();
                } else {
                    stream.write_all(slice).await.unwrap();
                }
            }
            stream.flush().await.unwrap();

            response = stream.read_u32().await.unwrap();
        }

        response
    }

    async fn read_tcp(
        &mut self,
        protocol: ClientOperation,
        queue_id: Option<u32>,
        group_name: Option<&[u8]>,
    ) -> Vec<u8> {
        let mut data: Vec<u8> = Vec::new();

        let stream_option = &mut self.tcp_stream;

        let stream = stream_option.as_mut().unwrap();

        if Self::identify_operation(stream, protocol, queue_id, group_name).await
            == ServerResponse::Accepted
        {
            let mut n_bytes_read = 0;
            let total_bytes = stream.read_u32().await.unwrap();

            loop {
                let mut buffer = Vec::<u8>::with_capacity(self.config.receive_buffer_capacity);

                match stream.read_buf(&mut buffer).await {
                    Ok(0) => {
                        continue;
                    }
                    Ok(n) => {
                        debug!(
                            "Client - Read Tcp Operation: Reading Buffer: n_bytes {:?}",
                            n
                        );
                        n_bytes_read += n;

                        data.extend_from_slice(&buffer);

                        if n_bytes_read == total_bytes.try_into().unwrap() {
                            break;
                        }
                    }
                    Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        continue;
                    }
                    Err(e) => {
                        debug!("Client - Read Tcp Operation: Error: {:?}", e);
                    }
                }
            }

            stream
                .write_u32(ServerResponse::Accepted as u32)
                .await
                .unwrap();
            stream.flush().await.unwrap();
        }

        data
    }
}
