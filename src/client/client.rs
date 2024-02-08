use log::debug;

use std::sync::Arc;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;

use crate::client::connection_pool::ConnectionPool;
use crate::protocol::{ClientOperation, ServerResponse};

pub struct Client {
    connection_pool: Arc<ConnectionPool>,
}

impl Client {
    pub fn new(connection_pool: Arc<ConnectionPool>) -> Client {
        Client {
            connection_pool,
        }
    }

    pub async fn create_bc_group(&mut self, group_name: &str, n_queues: u32) -> u32 {
        self.create_bc_group_operation(
            ClientOperation::CreateBcGroup,
            group_name.parse::<u32>().unwrap(),
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
            Some(group_name.parse::<u32>().unwrap()),
            data,
        )
        .await
    }

    pub async fn broadcast_root_refs(&mut self, group_name: &str, data: &[&[u8]]) -> u32 {
        self.write_tcp_refs(
            ClientOperation::BroadcastRoot,
            None,
            Some(group_name.parse::<u32>().unwrap()),
            data,
        )
        .await
    }

    pub async fn broadcast(&mut self, group_name: &str) -> Vec<u8> {
        self.read_tcp(
            ClientOperation::Broadcast,
            None,
            Some(group_name.parse::<u32>().unwrap()),
        )
        .await
    }

    async fn create_bc_group_operation(
        &mut self,
        protocol: ClientOperation,
        group_name: u32,
        n_queues: u32,
    ) -> u32 {
        let mut stream = self.connection_pool.get_connection().await;

        let protocol_bytes: &[u8] = &(protocol as u32).to_be_bytes();
        let group_bytes: &[u8] = &group_name.to_be_bytes();
        let n_queues: &[u8] = &n_queues.to_be_bytes();

        let headers = [protocol_bytes, group_bytes, n_queues].concat();

        stream.write_all(&headers).await.unwrap();
        stream.flush().await.unwrap();

        let response = stream.read_u32().await.unwrap();

        drop(stream);

        response
    }

    async fn write_tcp(
        &mut self,
        protocol: ClientOperation,
        queue_id: Option<u32>,
        group_name: Option<u32>,
        data: &[u8],
    ) -> u32 {
        let mut stream = self.connection_pool.get_connection().await;

        let protocol_bytes: &[u8] = &(protocol as u32).to_be_bytes();

        let headers: Vec<u8>;
        if protocol == ClientOperation::Send {
            headers = [
                protocol_bytes,
                &queue_id.unwrap().to_be_bytes(),
                &(data.len() as u32).to_be_bytes(),
            ]
            .concat();
        } else {
            headers = [
                protocol_bytes,
                &group_name.unwrap().to_be_bytes(),
                &(data.len() as u32).to_be_bytes(),
            ]
            .concat();
        }

        stream.write_all(&headers).await.unwrap();
        stream.write_all(&data).await.unwrap();
        stream.flush().await.unwrap();

        let response = stream.read_u32().await.unwrap();

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
        let mut stream = self.connection_pool.get_connection().await;

        let protocol_bytes: &[u8] = &(protocol as u32).to_be_bytes();

        // Header
        let mut len = 0;
        for slice in data {
            len += slice.len() as u32;
        }

        let headers: Vec<u8>;
        if protocol == ClientOperation::Send {
            headers = [
                protocol_bytes,
                &queue_id.unwrap().to_be_bytes(),
                &len.to_be_bytes(),
            ]
            .concat();
        } else {
            headers = [
                protocol_bytes,
                &group_name.unwrap().to_be_bytes(),
                &len.to_be_bytes(),
            ]
            .concat();
        }

        //Send total len
        stream.write_all(&headers).await.unwrap();

        for slice in data {
            stream.write_all(&slice).await.unwrap();
        }

        stream.flush().await.unwrap();

        let response = stream.read_u32().await.unwrap();

        drop(stream);

        response
    }

    async fn read_tcp(
        &mut self,
        protocol: ClientOperation,
        queue_id: Option<u32>,
        group_name: Option<u32>,
    ) -> Vec<u8> {
        let mut stream = self.connection_pool.get_connection().await;

        let protocol_bytes: &[u8] = &(protocol as u32).to_be_bytes();

        let all_data: Vec<u8>;
        if protocol == ClientOperation::Receive {
            all_data = [protocol_bytes, &queue_id.unwrap().to_be_bytes()].concat();
        } else {
            all_data = [protocol_bytes, &group_name.unwrap().to_be_bytes()].concat();
        }

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

        stream
            .write_u32(ServerResponse::Accepted as u32)
            .await
            .unwrap();
        stream.flush().await.unwrap();

        drop(stream);

        buffer
    }
}
