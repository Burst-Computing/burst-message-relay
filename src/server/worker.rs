use log::debug;

use std::error::Error;
use std::sync::Arc;

use crate::protocol::{ClientOperation, ServerResponse};
use crate::server::enums::{FromManager, ToManager};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::sync::Mutex;

pub async fn worker_task(
    client_id: u32,
    mut stream: TcpStream,
    send_to_manager: Sender<ToManager>,
) -> Result<(), Box<dyn Error>> {
    debug!(
        "Client {:?} - Main thread: Start Client Connection",
        client_id
    );

    //Establish 1-1 channel to communicate with Manager
    let (manager_sender, mut receive_from_manager) = mpsc::channel(1024 * 1024);

    // Register new client -> Message to Manager
    let new_client = ToManager::Register(client_id, manager_sender.clone());
    send_to_manager.send(new_client).await?;

    let mut alive = protocol_from_manager(&mut receive_from_manager, client_id, &mut stream).await;

    while alive {
        debug!("Client {:?} - Main thread: Checking Operation", client_id);

        let (operation_id, queue_id, group_name) = identify_operation(client_id, &mut stream).await;

        // Sv_code -> TO DO

        match operation_id {
            ClientOperation::CreateBcGroup => {
                debug!(
                    "Client {:?} - Main thread: Starting Create_Bc_Group Operation",
                    client_id
                );

                init_operation(
                    &mut receive_from_manager,
                    &send_to_manager,
                    client_id,
                    &mut stream,
                    group_name,
                )
                .await;
            }

            ClientOperation::Send => {
                debug!(
                    "Client {:?} - Main thread: Starting Send Operation",
                    client_id
                );

                let send_request = ToManager::SendRequest(client_id, queue_id.unwrap());
                send_to_manager.send(send_request).await?;

                alive =
                    protocol_from_manager(&mut receive_from_manager, client_id, &mut stream).await;
            }

            ClientOperation::Receive => {
                debug!(
                    "Client {:?} - Main thread: Starting Receive Operation",
                    client_id
                );

                let receive_request = ToManager::ReceiveRequest(client_id, queue_id.unwrap());
                send_to_manager.send(receive_request).await?;

                alive =
                    protocol_from_manager(&mut receive_from_manager, client_id, &mut stream).await;
            }

            ClientOperation::BroadcastRoot => {
                debug!(
                    "Client {:?} - Main thread: Starting Broadcast Root Operation",
                    client_id
                );

                let receive_request =
                    ToManager::BroadcastRootRequest(client_id, group_name.unwrap());
                send_to_manager.send(receive_request).await?;

                alive =
                    protocol_from_manager(&mut receive_from_manager, client_id, &mut stream).await;
            }

            ClientOperation::Broadcast => {
                debug!(
                    "Client {:?} - Main thread: Starting Broadcast Operation",
                    client_id
                );

                let receive_request = ToManager::BroadcastRequest(client_id, group_name.unwrap());
                send_to_manager.send(receive_request).await?;

                alive =
                    protocol_from_manager(&mut receive_from_manager, client_id, &mut stream).await;
            }

            ClientOperation::Close => {
                // Finishing Client Main -> Manager
                let close_client = ToManager::Close(client_id);
                send_to_manager.send(close_client).await?;

                alive =
                    protocol_from_manager(&mut receive_from_manager, client_id, &mut stream).await;
            }

            ClientOperation::Error => {
                unreachable!();
            }
        }
    }

    Ok(())
}

async fn protocol_from_manager(
    receiver: &mut Receiver<FromManager>,
    client_id: u32,
    stream: &mut TcpStream,
) -> bool {
    match receiver.recv().await {
        Some(FromManager::Accept(sv_code)) => {
            if sv_code == ServerResponse::Denied {
                debug!(
                    "Client {:?} - Main thread: Client already exists. Closing Client",
                    client_id
                );
                return false;
            }
        }
        Some(FromManager::CreateBroadcastGroupResponse(sv_code)) => {
            if sv_code == ServerResponse::Denied {
                debug!(
                    "Client {:?} - Main thread: Empty queue store or queue not found",
                    client_id
                );
            } else {
                stream.write_u32(sv_code as u32).await.unwrap();
                stream.flush().await.unwrap();
            }
        }
        Some(FromManager::SendResponse(sv_code, queue)) => {
            if sv_code == ServerResponse::Denied {
                debug!(
                    "Client {:?} - Main Thread: Destination Client queue not found",
                    client_id
                );
            } else {
                send_operation(client_id, stream, queue.unwrap())
                    .await
                    .unwrap();
            }
        }
        Some(FromManager::ReceiveResponse(sv_code, queue)) => {
            if sv_code == ServerResponse::Denied {
                debug!(
                    "Client {:?} - Main Thread: Client queue not found",
                    client_id
                );
            } else {
                receive_operation(client_id, stream, queue.unwrap().clone())
                    .await
                    .unwrap();
            }
        }
        Some(FromManager::BroadcastRootResponse(sv_code, list_queues)) => {
            if sv_code == ServerResponse::Denied {
                debug!("Client {:?} - Main Thread: Broadcast Root Error", client_id);
            } else {
                send_operation(client_id, stream, list_queues.unwrap())
                    .await
                    .unwrap();
            }
        }
        Some(FromManager::BroadcastResponse(sv_code, queue)) => {
            if sv_code == ServerResponse::Denied {
                debug!("Client {:?} - Main Thread: Not enough queues", client_id);
            } else {
                receive_operation(client_id, stream, queue.unwrap().clone())
                    .await
                    .unwrap();
            }
        }
        Some(FromManager::Close()) => {
            debug!("Client {:?} - Main thread: Closing Client", client_id);

            stream
                .write_u32(ServerResponse::Close as u32)
                .await
                .unwrap();
            stream.flush().await.unwrap();

            return false;
        }
        None => {}
    }

    return true;
}

async fn identify_operation(
    client_id: u32,
    stream: &mut TcpStream,
) -> (ClientOperation, Option<u32>, Option<u32>) {
    // Identify operation from Client
    let mut buffer: Vec<u8> = vec![0; 4 as usize];
    stream.read_exact(&mut buffer).await.unwrap();
    let op_id: ClientOperation = u32::from_be_bytes(buffer[0..4].try_into().unwrap()).into();

    let mut queue_id = None;
    let mut group_name_op = None;

    if op_id == ClientOperation::Send || op_id == ClientOperation::Receive {
        stream.read_exact(&mut buffer).await.unwrap();
        queue_id = Some(u32::from_be_bytes(buffer[0..4].try_into().unwrap()));
    } else if op_id == ClientOperation::CreateBcGroup
        || op_id == ClientOperation::BroadcastRoot
        || op_id == ClientOperation::Broadcast
    {
        stream.read_exact(&mut buffer).await.unwrap();
        group_name_op = Some(u32::from_be_bytes(buffer[0..4].try_into().unwrap()));
    }

    debug!(
        "Client {:?} - Main thread: Operation ID: {:?}",
        client_id, op_id
    );

    (op_id.into(), queue_id, group_name_op)
}

async fn init_operation(
    receiver: &mut Receiver<FromManager>,
    sender: &Sender<ToManager>,
    client_id: u32,
    stream: &mut TcpStream,
    group_name: Option<u32>,
) -> bool {
    //Read from tcp
    let mut buffer: Vec<u8> = vec![0; 4 as usize];
    stream.read_exact(&mut buffer).await.unwrap();
    let header = u32::from_be_bytes(buffer[0..4].try_into().unwrap());

    //Send to Manager
    let create_bc_operation =
        ToManager::CreateBroadcastGroup(client_id, group_name.unwrap(), header);
    sender.send(create_bc_operation).await.unwrap();

    //Read from Manager
    protocol_from_manager(receiver, client_id, stream).await
}

async fn init_operation(
    receiver: &mut Receiver<FromManager>,
    sender: &Sender<ToManager>,
    client_id: u32,
    stream: &mut TcpStream,
    operation_id: ClientOperation,
    group_name: Option<String>,
) -> bool {
    //Read from tcp
    let header = stream.read_u32().await.unwrap();

    //Send to Manager
    if operation_id == ClientOperation::InitQueue {
        let mut queues: Vec<u32> = Vec::with_capacity(header.try_into().unwrap());

        //Header contains number of queues
        let mut n = 0;
        while n < header {
            queues.push(stream.read_u32().await.unwrap());
            n += 1;
        }

        let init_operation = ToManager::InitQueues(client_id, queues);
        sender.send(init_operation).await.unwrap();
    } else if operation_id == ClientOperation::CreateBcGroup {
        let create_bc_operation =
            ToManager::CreateBroadcastGroup(client_id, group_name.unwrap(), header);
        sender.send(create_bc_operation).await.unwrap();
    } else {
        debug!("Unreachable!");
    }

    //Read from Manager
    protocol_from_manager(receiver, client_id, stream).await
}

async fn send_operation(
    client_id: u32,
    stream: &mut TcpStream,
    queue: Arc<Sender<Arc<Vec<u8>>>>,
) -> Result<(), Box<dyn Error>> {
    // Get header
    let mut buffer: Vec<u8> = vec![0; 4 as usize];
    stream.read_exact(&mut buffer).await.unwrap();
    let total_bytes = u32::from_be_bytes(buffer[0..4].try_into().unwrap());

    let mut buffer: Vec<u8> = vec![0; total_bytes as usize];

    match stream.read_exact(&mut buffer).await {
        Ok(_) => {
            let _ = queue.send(Arc::new(buffer)).await;
        }
        Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
            //debug!("Err: TCP -> SV (Write))");
        }
        Err(e) => {
            return Err(e.into());
        }
    }

    debug!(
        "Client {:?} - Main thread: Send: bytes: {:?}",
        client_id, total_bytes
    );

    debug!(
        "Client {:?} - Main thread: Send Operation completed",
        client_id
    );

    stream
        .write_u32(ServerResponse::Accepted as u32)
        .await
        .unwrap();
    stream.flush().await.unwrap();

    Ok(())
}

async fn receive_operation(
    client_id: u32,
    stream: &mut TcpStream,
    queue: Arc<Mutex<Receiver<Arc<Vec<u8>>>>>,
) -> Result<(), Box<dyn Error>> {
    let message = queue.lock().await.recv().await.unwrap();

    //Send header
    stream
        .write_all(&(message.len() as u32).to_be_bytes())
        .await?;

    // Send data to Client
    stream.write_all(&message).await.unwrap();
    stream.flush().await.unwrap();

    debug!(
        "Client {:?} - Main thread: Receive Operation completed",
        client_id
    );

    // Get response from Client
    let conf_code = stream.read_u32().await.unwrap();

    debug!(
        "Client {:?} - Main thread: Operation response: {:?}",
        client_id, conf_code
    );

    Ok(())
}