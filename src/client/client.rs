use log::debug;

use simple_pool::ResourcePoolGuard;
use std::sync::Arc;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

use crate::client::connection_pool::ConnectionPool;
use crate::config::ClientConfig;
use crate::protocol::{ClientOperation, ServerResponse};

pub struct Client {
    connection_pool: Arc<ConnectionPool>,
    config: ClientConfig,
}

impl Client {
    pub fn new(connection_pool: Arc<ConnectionPool>, config: ClientConfig) -> Client {
        Client {
            connection_pool,
            config,
        }
    }

    pub async fn init_queues(&mut self, data: &[u32]) -> u32 {
        self.init_queues_operation(ClientOperation::InitQueue, data)
            .await
    }

    pub async fn create_bc_group(&mut self, group_name: String, n_queues: u32) -> u32 {
        self.create_bc_group_operation(
            ClientOperation::CreateBcGroup,
            group_name.parse().unwrap(),
            n_queues,
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

    pub async fn broadcast_root(&mut self, group_name: &str, data: &[u8]) -> u32 {
        self.write_tcp(
            ClientOperation::BroadcastRoot,
            None,
            Some(group_name.parse().unwrap()),
            data,
        )
        .await
    }

    pub async fn broadcast_root_refs(&mut self, group_name: &str, data: &[&[u8]]) -> u32 {
        self.write_tcp_refs(
            ClientOperation::BroadcastRoot,
            None,
            Some(group_name.parse().unwrap()),
            data,
        )
        .await
    }

    pub async fn broadcast(&mut self, group_name: &str) -> Vec<u8> {
        self.read_tcp(
            ClientOperation::Broadcast,
            None,
            Some(group_name.parse().unwrap()),
        )
        .await
    }

    async fn identify_operation(
        stream: &mut ResourcePoolGuard<TcpStream>,
        op_id: ClientOperation,
    ) -> ServerResponse {
        stream.write_u32(op_id as u32).await.unwrap();
        stream.flush().await.unwrap();

        let sv_code = stream.read_u32().await.unwrap();

        debug!(
            "Client - Identify Operation: Server returned {:?} for operation ID: {:?}",
            sv_code, op_id
        );

        sv_code.into()
    }

    async fn init_queues_operation(&mut self, protocol: ClientOperation, queues: &[u32]) -> u32 {
        let mut response: u32 = 999999;

        let mut stream = self.connection_pool.get_connection().await;

        if Self::identify_operation(&mut stream, protocol).await == ServerResponse::Accepted {
            // Header
            let len = queues.len() as u32;

            stream.write_u32(len).await.unwrap();
            stream.flush().await.unwrap();

            for slice in queues {
                stream.write_u32(*slice).await.unwrap();
                stream.flush().await.unwrap();
            }

            response = stream.read_u32().await.unwrap();
        }

        drop(stream);

        response
    }

    async fn create_bc_group_operation(
        &mut self,
        protocol: ClientOperation,
        group_name: u32,
        n_queues: u32,
    ) -> u32 {
        let mut response: u32 = 999999;

        let mut stream = self.connection_pool.get_connection().await;

        if Self::identify_operation(&mut stream, protocol).await == ServerResponse::Accepted {
            // Send group name
            stream.write_u32(group_name).await.unwrap();
            stream.flush().await.unwrap();

            // N_queues
            stream.write_u32(n_queues).await.unwrap();
            stream.flush().await.unwrap();

            response = stream.read_u32().await.unwrap();
        }

        drop(stream);

        response
    }

    async fn check_queue(
        queue_id: Option<u32>,
        group_name: Option<u32>,
        stream: &mut ResourcePoolGuard<TcpStream>,
    ) -> ServerResponse {
        match queue_id {
            Some(id) => {
                // Send queue id
                stream.write_u32(id).await.unwrap();
                stream.flush().await.unwrap();
            }
            None => {
                // Send group name
                stream.write_u32(group_name.unwrap()).await.unwrap();
                stream.flush().await.unwrap();
            }
        }

        // Get response
        let sv_code = stream.read_u32().await.unwrap();

        sv_code.into()
    }

    async fn write_tcp(
        &mut self,
        protocol: ClientOperation,
        queue_id: Option<u32>,
        group_name: Option<u32>,
        data: &[u8],
    ) -> u32 {
        let mut response: u32 = 999999;

        let mut stream = self.connection_pool.get_connection().await;

        if Self::identify_operation(&mut stream, protocol).await == ServerResponse::Accepted {
            match Self::check_queue(queue_id, group_name, &mut stream).await {
                ServerResponse::Denied => {
                    if protocol == ClientOperation::Receive {
                        debug!("Client - {:?}: Queue {:?} not found", protocol, queue_id);
                    } else {
                        debug!(
                            "Client - {:?}: Group Name {:?} not found",
                            protocol, queue_id
                        );
                    }
                    return ServerResponse::Denied as u32;
                }
                ServerResponse::Accepted => {
                    // Header
                    let len;
                    if protocol == ClientOperation::InitQueue
                        || protocol == ClientOperation::CreateBcGroup
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
                            .write_all(
                                &data[self.config.send_buffer_capacity * iterations..data.len()],
                            )
                            .await
                            .unwrap();
                    } else {
                        stream.write_all(&data).await.unwrap();
                    }
                    stream.flush().await.unwrap();

                    response = stream.read_u32().await.unwrap();
                }
                ServerResponse::Close => {
                    unreachable!();
                }
                ServerResponse::Error => {
                    unreachable!();
                }
            }
        }
        drop(stream);

        response
    }

    async fn write_tcp_refs(
        &mut self,
        protocol: ClientOperation,
        queue_id: Option<u32>,
        group_name: Option<u32>,
        data: &[&[u8]],
    ) -> u32 {
        let mut response: u32 = 999999;

        let mut stream = self.connection_pool.get_connection().await;

        if Self::identify_operation(&mut stream, protocol).await == ServerResponse::Accepted {
            match Self::check_queue(queue_id, group_name, &mut stream).await {
                ServerResponse::Denied => {
                    if protocol == ClientOperation::Receive {
                        debug!("Client - {:?}: Queue {:?} not found", protocol, queue_id);
                    } else {
                        debug!(
                            "Client - {:?}: Group Name {:?} not found",
                            protocol, queue_id
                        );
                    }
                    return ServerResponse::Denied as u32;
                }
                ServerResponse::Accepted => {
                    // Header
                    let mut len = 0;
                    for slice in data {
                        len += slice.len() as u32;
                    }

                    //Send total len
                    stream.write_u32(len).await.unwrap();
                    stream.flush().await.unwrap();

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
                                stream.flush().await.unwrap();
                            }

                            let iter = iterations as usize;
                            stream
                                .write_all(
                                    &slice[self.config.send_buffer_capacity * iter..slice.len()],
                                )
                                .await
                                .unwrap();
                            stream.flush().await.unwrap();
                        } else {
                            stream.write_all(slice).await.unwrap();
                            stream.flush().await.unwrap();
                        }
                    }

                    response = stream.read_u32().await.unwrap();
                }
                ServerResponse::Close => {
                    unreachable!();
                }
                ServerResponse::Error => {
                    unreachable!();
                }
            }
        }

        drop(stream);

        response
    }

    async fn read_tcp(
        &mut self,
        protocol: ClientOperation,
        queue_id: Option<u32>,
        group_name: Option<u32>,
    ) -> Vec<u8> {
        let mut buffer = vec![0; 0];

        let mut stream = self.connection_pool.get_connection().await;

        if Self::identify_operation(&mut stream, protocol).await == ServerResponse::Accepted {
            match Self::check_queue(queue_id, group_name, &mut stream).await {
                ServerResponse::Denied => {
                    if protocol == ClientOperation::Receive {
                        debug!("Client - {:?}: Queue {:?} not found", protocol, queue_id);
                    } else {
                        debug!(
                            "Client - {:?}: Group Name {:?} not found",
                            protocol, queue_id
                        );
                    }

                    return buffer;
                }
                ServerResponse::Accepted => {
                    // Get header
                    let total_bytes = stream.read_u32().await.unwrap();

                    buffer = vec![0; total_bytes as usize];

                    match stream.read_exact(&mut buffer).await {
                        Ok(n) => {
                            debug!("Client read {:?} bytes", n);
                        }
                        Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                            //debug!("Err: TCP -> SV (Write))");
                        }
                        Err(e) => {
                            debug!("Error: {:?}", e);
                        }
                    }

                    stream
                        .write_u32(ServerResponse::Accepted as u32)
                        .await
                        .unwrap();
                    stream.flush().await.unwrap();
                }
                ServerResponse::Close => {
                    unreachable!();
                }
                ServerResponse::Error => {
                    unreachable!();
                }
            }
        }

        drop(stream);

        buffer
    }
}
