use deadqueue::unlimited::Queue;

use log::debug;

use std::collections::HashMap;
use std::error::Error;
use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc::{self, Receiver, Sender};

use crate::config::ServerConfig;
use crate::protocol::{ClientOperation, ServerResponse};
use crate::server::enums::{FromManager, ToManager};
use crate::server::message::Message;

use tokio::sync::Mutex;

pub async fn process_task(
    client_id: u32,
    mut stream: TcpStream,
    send_to_manager: Sender<ToManager>,
    config: ServerConfig,
) -> Result<(), Box<dyn Error>> {
    debug!(
        "Client {:?} - Main thread: Start Client Connection",
        client_id
    );

    //Establish 1-1 channel to communicate with Manager
    let (manager_sender, mut receive_from_manager) = mpsc::channel(1024);

    // Register new client -> Message to Manager
    let new_client = ToManager::Register(client_id, manager_sender.clone());
    send_to_manager.send(new_client).await?;

    let hashmap_queues: Arc<Mutex<HashMap<u32, Vec<Message>>>> = Arc::new(Mutex::new(HashMap::new()));

    let mut alive = protocol_from_manager(
        &mut receive_from_manager,
        client_id,
        &mut stream,
        None,
        None
    )
    .await;

    while alive {
        debug!("Client {:?} - Main thread: Checking Operation", client_id);

        let (_sv_code, operation_id, queue_id, group_name) =
            identify_operation(client_id, &mut stream).await;

        // Sv_code -> TO DO

        match operation_id {
            ClientOperation::InitQueue => {
                debug!(
                    "Client {:?} - Main thread: Starting Init_Queue Operation",
                    client_id
                );

                init_operation(
                    &mut receive_from_manager,
                    &send_to_manager,
                    client_id,
                    &mut stream,
                    operation_id,
                    None,
                )
                .await;
            }

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
                    operation_id,
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

                alive = protocol_from_manager(
                    &mut receive_from_manager,
                    client_id,
                    &mut stream,
                    Some(config.clone()),
                    None
                )
                .await;
            }

            ClientOperation::Receive => {
                debug!(
                    "Client {:?} - Main thread: Starting Receive Operation",
                    client_id
                );

                let receive_request = ToManager::ReceiveRequest(client_id, queue_id.unwrap());
                send_to_manager.send(receive_request).await?;

                alive = protocol_from_manager(
                    &mut receive_from_manager,
                    client_id,
                    &mut stream,
                    None,
                    Some(hashmap_queues.clone())
                )
                .await;
            }

            ClientOperation::BroadcastRoot => {
                debug!(
                    "Client {:?} - Main thread: Starting Broadcast Root Operation",
                    client_id
                );

                let receive_request =
                    ToManager::BroadcastRootRequest(client_id, group_name.unwrap());
                send_to_manager.send(receive_request).await?;

                alive = protocol_from_manager(
                    &mut receive_from_manager,
                    client_id,
                    &mut stream,
                    Some(config.clone()),
                    None
                )
                .await;
            }

            ClientOperation::Broadcast => {
                debug!(
                    "Client {:?} - Main thread: Starting Broadcast Operation",
                    client_id
                );

                let receive_request = ToManager::BroadcastRequest(client_id, group_name.unwrap());
                send_to_manager.send(receive_request).await?;

                alive = protocol_from_manager(
                    &mut receive_from_manager,
                    client_id,
                    &mut stream,
                    None,
                    Some(hashmap_queues.clone()),
                )
                .await;
            }

            ClientOperation::Close => {
                // Finishing Client Main -> Manager
                let close_client = ToManager::Close(client_id);
                send_to_manager.send(close_client).await?;

                alive = protocol_from_manager(
                    &mut receive_from_manager,
                    client_id,
                    &mut stream,
                    None,
                    None
                )
                .await;
            }

            ClientOperation::Error => {
                unreachable!();
            }
        }
    }

    Ok(())
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
    protocol_from_manager(receiver, client_id, stream, None, None).await
}

async fn protocol_from_manager(
    receiver: &mut Receiver<FromManager>,
    client_id: u32,
    stream: &mut TcpStream,
    config: Option<ServerConfig>,
    hashmap_queues: Option<Arc<Mutex<HashMap<u32, Vec<Message>>>>>,
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
        Some(FromManager::InitQueuesResponse(sv_code)) => {
            stream.write_u32(sv_code as u32).await.unwrap();
            stream.flush().await.unwrap();
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
        Some(FromManager::SendResponse(sv_code, id, queue)) => {
            if sv_code == ServerResponse::Denied {
                debug!(
                    "Client {:?} - Main Thread: Destination Client queue not found",
                    client_id
                );
            } else {
                send_operation(id, client_id, stream, queue.unwrap(), config.unwrap())
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
                receive_operation(
                    client_id,
                    ClientOperation::Send,
                    stream,
                    queue.unwrap().clone(),
                    hashmap_queues.unwrap()
                )
                .await
                .unwrap();
            }
        }
        Some(FromManager::BroadcastRootResponse(sv_code, id, list_queues)) => {
            if sv_code == ServerResponse::Denied {
                debug!("Client {:?} - Main Thread: Broadcast Root Error", client_id);
            } else {
                broadcast_root_operation(id, client_id, stream, list_queues.unwrap(), config.unwrap())
                    .await
                    .unwrap();
            }
        }
        Some(FromManager::BroadcastResponse(sv_code, queue)) => {
            if sv_code == ServerResponse::Denied {
                debug!("Client {:?} - Main Thread: Not enough queues", client_id);
            } else {
                receive_operation(
                    client_id,
                    ClientOperation::BroadcastRoot,
                    stream,
                    queue.unwrap(),
                    hashmap_queues.unwrap()
                )
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
) -> (ServerResponse, ClientOperation, Option<u32>, Option<String>) {
    // Identify operation from Client
    let op_id: ClientOperation = stream.read_u32().await.unwrap().into();

    let mut queue_id = None;
    let mut group_name_op = None;
    if op_id == ClientOperation::Send || op_id == ClientOperation::Receive {
        queue_id = Some(stream.read_u32().await.unwrap());
    } else if op_id == ClientOperation::CreateBcGroup
        || op_id == ClientOperation::BroadcastRoot
        || op_id == ClientOperation::Broadcast
    {
        let mut group_name = Vec::new();
        stream.read_buf(&mut group_name).await.unwrap();
        group_name_op = Some(String::from_utf8(group_name).unwrap());
    }

    debug!(
        "Client {:?} - Main thread: Operation ID: {:?}",
        client_id, op_id
    );

    let mut sv_code = ServerResponse::Denied;

    // Check operation
    if op_id == ClientOperation::InitQueue
        || op_id == ClientOperation::CreateBcGroup
        || op_id == ClientOperation::Send
        || op_id == ClientOperation::Receive
        || op_id == ClientOperation::BroadcastRoot
        || op_id == ClientOperation::Broadcast
        || op_id == ClientOperation::Close
    {
        sv_code = ServerResponse::Accepted;
    }

    // Send sv_code to Client
    if op_id != ClientOperation::Close {
        stream.write_u32(sv_code as u32).await.unwrap();
        stream.flush().await.unwrap();
    }

    (sv_code.into(), op_id.into(), queue_id, group_name_op)
}

async fn send_operation(
    id: u32,
    client_id: u32,
    stream: &mut TcpStream,
    queue: Arc<Queue<Message>>,
    config: ServerConfig,
) -> Result<(), Box<dyn Error>> {
    let mut n_bytes_read = 0;
    let mut chunk_id = 0;
    let mut last_chunk = false;

    // Get header
    let total_bytes = stream.read_u32().await.unwrap();

    loop {
        let mut buffer = Vec::<u8>::with_capacity(config.send_buffer_capacity);

        match stream.read_buf(&mut buffer).await {
            Ok(0) => {
                continue;
            }
            Ok(n) => {
                n_bytes_read += n;

                if n_bytes_read == total_bytes.try_into().unwrap() {
                    last_chunk = true;
                }

                let message = Message::new(
                    id,
                    client_id,
                    ClientOperation::Send,
                    chunk_id,
                    last_chunk,
                    total_bytes,
                    buffer,
                );

                queue.push(message.clone());

                chunk_id += 1;

                if last_chunk {
                    break;
                }
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                //debug!("Err: TCP -> SV (Write))");
                continue;
            }
            Err(e) => {
                return Err(e.into());
            }
        }
    }

    debug!(
        "Client {:?} - Main thread: Send: bytes: {:?}",
        client_id, n_bytes_read
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
    op_id: ClientOperation,
    stream: &mut TcpStream,
    queue: Arc<Queue<Message>>,
    hashmap_queues: Arc<Mutex<HashMap<u32, Vec<Message>>>>,
) -> Result<(), Box<dyn Error>> {

    let mut start = false;
    let mut message_id = 0;
    let mut chunk = 0;

    let mut finish = false;

    let mut aux_queue = Vec::new();

    let mut hashmap = hashmap_queues.lock().await;

    loop {

        let message;

        if !hashmap.is_empty() {

            if !start {
                let (_, queue) = hashmap.clone().into_iter().next().unwrap();
                aux_queue = queue;
            }

            if !aux_queue.is_empty() {
                message = aux_queue.remove(0);
            } else {
                message = queue.pop().await;
            }

        } else {
            message = queue.pop().await;
        }

        if !start {
            message_id = message.id;

            stream.write_all(&message.all_mess_len.to_be_bytes()).await?;
            stream.flush().await.unwrap();

            start = true;
        }

        if message.chunk_id == chunk && message.op_id == op_id && message.id == message_id {
            // Send data to Client
            stream.write_all(&message.bytes).await?;
            stream.flush().await.unwrap();

            chunk += 1;

            if message.last_chunk {
                finish = true;
            }

        } else if message.id == message_id {

            aux_queue.push(message);

        } else {
            if !hashmap.contains_key(&message.id) {
                let mut new_queue = Vec::new();
                let id = message.id;
                new_queue.push(message);
                hashmap.insert(id, new_queue);
            } else {
                let aux_queue = hashmap.get_mut(&message.id).unwrap();
                aux_queue.push(message);
            }
        }

        if finish {
            break;
        }
    }

    hashmap.remove(&message_id);
    drop(hashmap);

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

async fn broadcast_root_operation(
    id: u32,
    client_id: u32,
    stream: &mut TcpStream,
    list_queues: Vec<Arc<Queue<Message>>>,
    config: ServerConfig,
) -> Result<(), Box<dyn Error>> {
    let mut n_bytes_read = 0;
    let mut chunk_id = 0;
    let mut last_chunk = false;

    // Get header
    let total_bytes = stream.read_u32().await.unwrap();

    loop {
        let mut buffer = Vec::<u8>::with_capacity(config.send_buffer_capacity);

        match stream.read_buf(&mut buffer).await {
            Ok(0) => {
                continue;
            }
            Ok(n) => {
                n_bytes_read += n;

                if n_bytes_read == total_bytes.try_into().unwrap() {
                    last_chunk = true;
                }

                let message = Message::new(
                    id,
                    client_id,
                    ClientOperation::BroadcastRoot,
                    chunk_id,
                    last_chunk,
                    total_bytes,
                    buffer,
                );
                for queue in list_queues.clone().into_iter() {
                    queue.push(message.clone());
                }

                chunk_id += 1;

                if last_chunk {
                    break;
                }
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                //debug!("Err: TCP -> SV (Write))");
                continue;
            }
            Err(e) => {
                return Err(e.into());
            }
        }
    }

    debug!(
        "Client {:?} - Main thread: Send: bytes: {:?}",
        client_id, n_bytes_read
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
