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
        self.init_queues_operation(ClientOperation::InitQueue, None, None, data)
            .await
    }

    pub async fn create_bc_group(&mut self, group_name: String, n_queues: u32) -> u32 {
        self.create_bc_group_operation(
            ClientOperation::CreateBcGroup,
            None,
            Some(group_name.as_bytes()),
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

    /* pub async fn broadcast_root(&mut self, group_name: &str, data: &[u8]) -> u32 {
        self.write_tcp(
            ClientOperation::BroadcastRoot,
            None,
            Some(group_name.as_bytes()),
            data,
        )
        .await
    } */

    pub async fn broadcast_root_refs(&mut self, group_name: &str, data: &[&[u8]]) -> u32 {
        self.write_tcp_refs(
            ClientOperation::BroadcastRoot,
            None,
            Some(group_name.as_bytes()),
            data,
        )
        .await
    }

    /* pub async fn broadcast(&mut self, group_name: &str) -> Vec<u8> {
        self.read_tcp(
            ClientOperation::Broadcast,
            None,
            Some(group_name.as_bytes()),
        )
        .await
    } */

    async fn identify_operation(
        stream: &mut ResourcePoolGuard<TcpStream>,
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

    async fn init_queues_operation(
        &mut self,
        protocol: ClientOperation,
        group_id: Option<u32>,
        group_name: Option<&[u8]>,
        queues: &[u32],
    ) -> u32 {
        let mut response: u32 = 999999;

        let mut stream = self.connection_pool.get_connection().await;

        if Self::identify_operation(&mut stream, protocol, group_id, group_name).await
            == ServerResponse::Accepted
        {
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
        group_id: Option<u32>,
        group_name: Option<&[u8]>,
        n_queues: u32,
    ) -> u32 {
        let mut response: u32 = 999999;

        let mut stream = self.connection_pool.get_connection().await;

        if Self::identify_operation(&mut stream, protocol, group_id, group_name).await
            == ServerResponse::Accepted
        {
            // N_queues
            stream.write_u32(n_queues).await.unwrap();
            stream.flush().await.unwrap();

            response = stream.read_u32().await.unwrap();
        }

        drop(stream);

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

        let mut stream = self.connection_pool.get_connection().await;

        let protocol_bytes: &[u8] = &(protocol as u32).to_be_bytes();
        let queue_id_bytes: &[u8] = &queue_id.unwrap().to_be_bytes();
        let header: &[u8] = &(data.len() as u32).to_be_bytes();

        let all_data = [protocol_bytes, queue_id_bytes, header, data].concat();

        stream.write_all(&all_data).await.unwrap();
        stream.flush().await.unwrap();

        // Header
        /* let len;
        if protocol == ClientOperation::InitQueue || protocol == ClientOperation::CreateBcGroup {
            len = (data.len() / 4) as u32;
        } else {
            len = data.len() as u32;
        } */

        /* stream.write_u32(len).await.unwrap();

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
        stream.flush().await.unwrap();*/

        response = stream.read_u32().await.unwrap();

        drop(stream);

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

        let mut stream = self.connection_pool.get_connection().await;

        if Self::identify_operation(&mut stream, protocol, queue_id, group_name).await
            == ServerResponse::Accepted
        {
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
                        .write_all(&slice[self.config.send_buffer_capacity * iter..slice.len()])
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

        drop(stream);

        response
    }

    async fn read_tcp(
        &mut self,
        protocol: ClientOperation,
        queue_id: Option<u32>,
        group_name: Option<&[u8]>,
    ) -> Vec<u8> {
        let mut stream = self.connection_pool.get_connection().await;

        let protocol_bytes: &[u8] = &(protocol as u32).to_be_bytes();
        let queue_id_bytes: &[u8] = &queue_id.unwrap().to_be_bytes();

        let all_data = [protocol_bytes, queue_id_bytes].concat();

        stream.write_all(&all_data).await.unwrap();
        stream.flush().await.unwrap();

        // Get header
        let mut buffer: Vec<u8> = vec![0; 4 as usize];
        stream.read_exact(&mut buffer).await.unwrap();
        let total_bytes = u32::from_be_bytes(buffer[0..4].try_into().unwrap());

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

        drop(stream);

        buffer
    }
}
