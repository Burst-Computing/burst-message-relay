use log::debug;

use std::error::Error;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use crate::protocol::{ClientOperation, ServerResponse};
use dashmap::DashMap;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::sync::Mutex;
use tokio::sync::broadcast::{self, Receiver as BroadcastReceiver, Sender as BroadcastSender};

pub async fn worker_task(
    client_id: u32,
    mut stream: TcpStream,
    send_store: Arc<DashMap<u32, Arc<Sender<Arc<Vec<u8>>>>>>,
    recv_store: Arc<DashMap<u32, Arc<Mutex<Receiver<Arc<Vec<u8>>>>>>>,
    broadcast_send_store: Arc<DashMap<String, Arc<BroadcastSender<Arc<Vec<u8>>>>>>,
    broadcast_recv_store: Arc<DashMap<String, DashMap<u32, Arc<Mutex<BroadcastReceiver<Arc<Vec<u8>>>>>>>>,
    bgroup_counter: Arc<DashMap<String, Mutex<u32>>>,
) -> Result<(), Box<dyn Error>> {
    debug!(
        "Client {:?} - Main thread: Start Client Connection",
        client_id
    );

    let mut alive = true;

    while alive {
        debug!("Client {:?} - Main thread: Checking Operation", client_id);

        let (_sv_code, operation_id) =
            identify_operation(client_id, &mut stream).await;

        // Sv_code -> TO DO

        match operation_id {
            ClientOperation::InitQueue => {
                debug!(
                    "Client {:?} - Main thread: Starting Init Queues Operation",
                    client_id
                );

                alive = init_queues(
                    client_id,
                    &mut stream,
                    send_store.clone(),
                    recv_store.clone(),
                )
                .await;
            }

            ClientOperation::CreateBcGroup => {
                debug!(
                    "Client {:?} - Main thread: Starting Create Broadcast Group Operation",
                    client_id
                );

                alive = create_bc_group(
                    client_id,
                    &mut stream,
                    broadcast_send_store.clone(),
                    broadcast_recv_store.clone(),
                    bgroup_counter.clone(),
                )
                .await;
            }

            ClientOperation::Send => {
                debug!(
                    "Client {:?} - Main thread: Starting Send Operation",
                    client_id
                );

                alive = send_operation(
                    client_id,
                    &mut stream,
                    send_store.clone(),
                )
                .await;
            }

            ClientOperation::Receive => {
                debug!(
                    "Client {:?} - Main thread: Starting Receive Operation",
                    client_id
                );

                alive = receive_operation(
                    client_id,
                    &mut stream,
                    recv_store.clone(),
                )
                .await;
            }

            ClientOperation::BroadcastRoot => {
                debug!(
                    "Client {:?} - Main thread: Starting Broadcast Root Operation",
                    client_id
                );

                alive = broadcast_root_operation(
                    client_id,
                    &mut stream,
                    broadcast_send_store.clone(),
                )
                .await;
            }

            ClientOperation::Broadcast => {
                debug!(
                    "Client {:?} - Main thread: Starting Broadcast Operation",
                    client_id
                );

                alive = broadcast_operation(
                    client_id,
                    &mut stream,
                    broadcast_recv_store.clone(),
                    bgroup_counter.clone(),
                )
                .await;
            }

            ClientOperation::Close => {
                // Finishing Client Main
                debug!("Client {:?} - Main thread: Closing Client", client_id);

                stream
                    .write_u32(ServerResponse::Close as u32)
                    .await
                    .unwrap();
                stream.flush().await.unwrap();

                alive = false;
            }

            ClientOperation::Error => {
                debug!(
                    "Client {:?} - Main thread: Client Operation Error",
                    client_id
                );
            }
        }
    }

    Ok(())
}

async fn identify_operation(
    client_id: u32,
    stream: &mut TcpStream,
) -> (ServerResponse, ClientOperation) {

    // Identify operation from Client
    let op_id: ClientOperation = stream.read_u32().await.unwrap().into();

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

    (sv_code.into(), op_id.into())
}

async fn init_queues(
    client_id: u32,
    stream: &mut TcpStream,
    send_store: Arc<DashMap<u32, Arc<Sender<Arc<Vec<u8>>>>>>,
    recv_store: Arc<DashMap<u32, Arc<Mutex<Receiver<Arc<Vec<u8>>>>>>>,
) -> bool {

    let mut sv_code = ServerResponse::Denied;

    //Read from tcp
    let header = stream.read_u32().await.unwrap();

    //Header contains number of queues
    let mut n = 0;
    while n < header {
        // Initialize queues
        let queue_id = stream.read_u32().await.unwrap();

        if send_store.contains_key(&queue_id) {
            debug!(
                "Client {:?} - Main thread: Queue id {:?} is already registered",
                client_id, queue_id
            );
        } else {
            let (send_channel, recv_channel) = mpsc::channel(1024 * 1024);
            send_store.insert(queue_id, Arc::new(send_channel));
            recv_store.insert(queue_id, Arc::new(Mutex::new(recv_channel)));
            sv_code = ServerResponse::Accepted;
        }

        n += 1;
    }

    stream
        .write_u32(sv_code as u32)
        .await
        .unwrap();
    stream.flush().await.unwrap();

    debug!(
        "Client {:?} - Main thread: Init Queues completed",
        client_id
    );

    return true;
}

async fn create_bc_group(
    client_id: u32,
    stream: &mut TcpStream,
    broadcast_send_store: Arc<DashMap<String, Arc<BroadcastSender<Arc<Vec<u8>>>>>>,
    broadcast_recv_store: Arc<DashMap<String, DashMap<u32, Arc<Mutex<BroadcastReceiver<Arc<Vec<u8>>>>>>>>,
    bgroup_counter: Arc<DashMap<String, Mutex<u32>>>
) -> bool {

    let mut sv_code = ServerResponse::Denied;

    // Read group name
 
    let group_name = stream.read_u32().await.unwrap();

    // Read number of queues
    let n_queues = stream.read_u32().await.unwrap();

    let gp_name = group_name.to_string().clone();

    if broadcast_send_store.contains_key(&gp_name) {
        debug!(
            "Client {:?} - Main thread: Broadcast group {:?} is already registered",
            client_id, gp_name
        );
    } else {

        let bc_hashmap: DashMap<u32, Arc<Mutex<BroadcastReceiver<Arc<Vec<u8>>>>>> =
            DashMap::new();

        let (tx, _) = broadcast::channel(1024*1024);

        for i in 0..n_queues {
            bc_hashmap.insert(i, Arc::new(Mutex::new(tx.subscribe())));
        }

        broadcast_recv_store.insert(gp_name.clone(), bc_hashmap);
        broadcast_send_store.insert(gp_name.clone(), Arc::new(tx));
        bgroup_counter.insert(gp_name, Mutex::new(0));

        sv_code = ServerResponse::Accepted;
    }

    stream
        .write_u32(sv_code as u32)
        .await
        .unwrap();
    stream.flush().await.unwrap();

    debug!(
        "Client {:?} - Main thread: Create Broadcast Group completed",
        client_id
    );

    return true;
}

async fn send_operation(
    client_id: u32,
    stream: &mut TcpStream,
    send_store: Arc<DashMap<u32, Arc<Sender<Arc<Vec<u8>>>>>>,
) -> bool {

    // Get queue_id
    let queue_id = stream.read_u32().await.unwrap();

    // Get queue
    match send_store.get(&queue_id) {
        Some(queue) => {

            // Queue exists
            stream
                .write_u32(ServerResponse::Accepted as u32)
                .await
                .unwrap();
            stream.flush().await.unwrap(); 

            // Get header
            let total_bytes = stream.read_u32().await.unwrap();

            let mut buffer: Vec<u8> = vec![0; total_bytes as usize];

            match stream.read_exact(&mut buffer).await {
                Ok(_) => {
                    let _ = queue.send(Arc::new(buffer)).await;
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    //debug!("Err: TCP -> SV (Write))");
                }
                Err(e) => {
                    debug!("{:?}", e);
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
        }
        None => {
            stream
                .write_u32(ServerResponse::Denied as u32)
                .await
                .unwrap();
            stream.flush().await.unwrap(); 

            debug!(
                "Client {:?} - Main thread: Send Operation denied - Queue {:?} not found",
                client_id, queue_id
            );

        }
    }

    return true;
}

async fn receive_operation(
    client_id: u32,
    stream: &mut TcpStream,
    recv_store: Arc<DashMap<u32, Arc<Mutex<Receiver<Arc<Vec<u8>>>>>>>,
) -> bool {

    // Get queue_id
    let queue_id = stream.read_u32().await.unwrap();

    match recv_store.get(&queue_id) {
        Some(queue) => {

            // Queue exists
            stream
                .write_u32(ServerResponse::Accepted as u32)
                .await
                .unwrap();
            stream.flush().await.unwrap(); 


            let message = queue.lock().await.recv().await.unwrap();

            //Send header
            stream.write_u32(message.len() as u32).await.unwrap();
            stream.flush().await.unwrap();

            // Send data to Client
            stream.write_all(&message).await.unwrap();

            debug!(
                "Client {:?} - Main thread: Receive Operation completed",
                client_id
            );

            // Get response from Client
            let conf_code = stream.read_u32().await.unwrap();

            debug!(
                "Client {:?} - Main thread: Receive Operation response: {:?}",
                client_id, conf_code
            );
        }
        None => {
            stream
                .write_u32(ServerResponse::Denied as u32)
                .await
                .unwrap();
            stream.flush().await.unwrap(); 

            debug!(
                "Client {:?} - Main thread: Receive Operation denied - Queue {:?} not found",
                client_id, queue_id
            );
        }
    }

    return true;
}

async fn broadcast_root_operation(
    client_id: u32,
    stream: &mut TcpStream,
    broadcast_send_store: Arc<DashMap<String, Arc<BroadcastSender<Arc<Vec<u8>>>>>>,
) -> bool {

    // Read group name
    let group_name = stream.read_u32().await.unwrap();
    let group_name_str = group_name.to_string();

    match broadcast_send_store.get(&group_name_str) {
        Some(queue) => {

            // Group name exists
            stream
                .write_u32(ServerResponse::Accepted as u32)
                .await
                .unwrap();
            stream.flush().await.unwrap(); 

            // Get header
            let total_bytes = stream.read_u32().await.unwrap();

            let mut buffer: Vec<u8> = vec![0; total_bytes as usize];

            match stream.read_exact(&mut buffer).await {
                Ok(_) => {
                    let _ = queue.send(Arc::new(buffer));
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    //debug!("Err: TCP -> SV (Write))");
                }
                Err(e) => {
                    debug!("{:?}", e);
                }
            }

            debug!(
                "Client {:?} - Main thread: Send: bytes: {:?}",
                client_id, total_bytes
            );

            debug!(
                "Client {:?} - Main thread: Broadcast Root Operation completed",
                client_id
            );

            stream
                .write_u32(ServerResponse::Accepted as u32)
                .await
                .unwrap();
            stream.flush().await.unwrap();
        }
        None => {
            stream
                .write_u32(ServerResponse::Denied as u32)
                .await
                .unwrap();
            stream.flush().await.unwrap(); 

            debug!(
                "Client {:?} - Main thread: Broadcast Root Operation denied - Group Name {:?} not found",
                client_id, group_name_str
            );
        }
    }

    return true;
}

async fn broadcast_operation(
    client_id: u32,
    stream: &mut TcpStream,
    broadcast_recv_store: Arc<DashMap<String, DashMap<u32, Arc<Mutex<BroadcastReceiver<Arc<Vec<u8>>>>>>>>,
    bgroup_counter: Arc<DashMap<String, Mutex<u32>>>,
) -> bool {

    // Read group name
    let group_name = stream.read_u32().await.unwrap();
    let group_name_str = group_name.to_string();

    match broadcast_recv_store.get(&group_name_str) {
        Some(hashmap_group) => {

            // Group name exists
            stream
                .write_u32(ServerResponse::Accepted as u32)
                .await
                .unwrap();
            stream.flush().await.unwrap(); 

            let mutex_counter = bgroup_counter.get_mut(&group_name_str).unwrap();

            let mut counter = mutex_counter.lock().await;
            let actual_counter = *counter;
            *counter += 1;

            if *counter == hashmap_group.len() as u32 {
                *counter = 0;
            }
            drop(counter);

            match hashmap_group.get(&actual_counter) {
                Some(queue) => {

                    let message = queue.lock().await.recv().await.unwrap();

                    //Send header
                    stream.write_u32(message.len() as u32).await.unwrap();
                    stream.flush().await.unwrap();

                    // Send data to Client
                    stream.write_all(&message).await.unwrap();

                    debug!(
                        "Client {:?} - Main thread: Broadcast Operation completed",
                        client_id
                    );

                    // Get response from Client
                    let conf_code = stream.read_u32().await.unwrap();

                    debug!(
                        "Client {:?} - Main thread: Broadcast Operation response: {:?}",
                        client_id, conf_code
                    );

                }
                None => {
                    debug!("Client {:?} - Main thread: Error: The number of requests is greater than the number of queues!", client_id);
                    //atomic_counter.fetch_sub(actual_counter, Ordering::SeqCst);
                }
            }
        }
        None => {
            stream
                .write_u32(ServerResponse::Denied as u32)
                .await
                .unwrap();
            stream.flush().await.unwrap(); 

            debug!(
                "Client {:?} - Main thread: Broadcast Operation denied - Group Name {:?} not found",
                client_id, group_name_str
            );
        }
    }

    

    return true;
}
