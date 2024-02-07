use log::debug;

use std::collections::HashMap;
use std::error::Error;
use std::sync::Arc;

use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::sync::Mutex;

use crate::protocol::ServerResponse;
use crate::server::enums::{FromManager, ToManager};
use crate::server::message::Message;

pub async fn start(mut receiver: Receiver<ToManager>) -> Result<(), Box<dyn Error>> {
    // Creating HashMap to store clients and queues
    let mut clients_store: HashMap<u32, Sender<FromManager>> = HashMap::new();
    let mut send_store: HashMap<u32, Arc<Sender<Arc<Message>>>> = HashMap::new();
    let mut recv_store: HashMap<u32, Arc<Mutex<Receiver<Arc<Message>>>>> = HashMap::new();
    let mut broadcast_send_store: HashMap<String, Arc<Sender<Arc<Vec<u8>>>>> = HashMap::new();
    let mut broadcast_recv_store: HashMap<String, Arc<Mutex<Receiver<Arc<Vec<u8>>>>>> =
        HashMap::new();

    loop {
        match receiver.recv().await {
            Some(ToManager::Register(client_id, client_channel)) => {
                debug!("Manager Thread: Register [Client: ID: {:?}]", client_id);

                let cc_copy = client_channel.clone();

                // Register client

                if clients_store.contains_key(&client_id) {
                    debug!("Manager Thread: Client is already registered");
                    cc_copy
                        .send(FromManager::Accept(ServerResponse::Denied))
                        .await?;
                } else {
                    clients_store.insert(client_id, client_channel);
                }

                // Send Message: Accept -> Manager To Client
                let acc = FromManager::Accept(ServerResponse::Accepted);
                cc_copy.send(acc).await?;
            }
            Some(ToManager::InitQueues(client_id, queues)) => {
                let client_channel: &Sender<FromManager> = clients_store.get(&client_id).unwrap();

                for name in queues {
                    if send_store.contains_key(&name) {
                        debug!("Manager Thread: Queue id is already registered");
                    } else {
                        let (send, recv) = mpsc::channel(1024 * 1024);
                        send_store.insert(name, Arc::new(send));
                        recv_store.insert(name, Arc::new(Mutex::new(recv)));
                    }
                }

                client_channel
                    .send(FromManager::InitQueuesResponse(ServerResponse::Accepted))
                    .await?;
            }
            Some(ToManager::CreateBroadcastGroup(client_id, group_name, _n_queues)) => {
                let client_channel: &Sender<FromManager> = clients_store.get(&client_id).unwrap();

                if broadcast_send_store.contains_key(&group_name) {
                    debug!("Manager Thread: Broadcast Group is already registered");
                } else {
                    let (send, recv) = mpsc::channel(1024 * 1024);
                    broadcast_send_store.insert(group_name.clone(), Arc::new(send));
                    broadcast_recv_store.insert(group_name, Arc::new(Mutex::new(recv)));
                }

                client_channel
                    .send(FromManager::CreateBroadcastGroupResponse(
                        ServerResponse::Accepted,
                    ))
                    .await?;
            }
            Some(ToManager::SendRequest(client_id, queue_id)) => {
                let client_channel = clients_store.get(&client_id).unwrap();

                let mut exists = true;
                let mut queue: Option<Arc<Sender<Arc<Message>>>> = None;

                if !send_store.contains_key(&queue_id) {
                    let (send_channel, recv_channel) = mpsc::channel(1024 * 1024);
                    let send_queue = Arc::new(send_channel);
                    send_store.insert(queue_id.clone(), send_queue.clone());
                    recv_store.insert(queue_id.clone(), Arc::new(Mutex::new(recv_channel)));
                    exists = false;
                    queue = Some(send_queue);
                }

                // Get queue
                if exists {
                    queue = Some(send_store.get(&queue_id).unwrap().clone());
                }

                client_channel
                    .send(FromManager::SendResponse(
                        ServerResponse::Accepted,
                        Some(queue.unwrap().clone()),
                    ))
                    .await?;
            }
            Some(ToManager::ReceiveRequest(client_id, queue_id)) => {
                let client_channel = clients_store.get(&client_id).unwrap();

                let mut exists = true;
                let mut queue: Option<Arc<Mutex<Receiver<Arc<Message>>>>> = None;

                if !send_store.contains_key(&queue_id) {
                    let (send_channel, recv_channel) = mpsc::channel(1024 * 1024);
                    let recv_queue = Arc::new(Mutex::new(recv_channel));
                    send_store.insert(queue_id.clone(), Arc::new(send_channel));
                    recv_store.insert(queue_id.clone(), recv_queue.clone());
                    exists = false;
                    queue = Some(recv_queue);
                }

                // Get queue
                if exists {
                    queue = Some(recv_store.get(&queue_id).unwrap().clone());
                }

                client_channel
                    .send(FromManager::ReceiveResponse(
                        ServerResponse::Accepted,
                        Some(queue.unwrap().clone()),
                    ))
                    .await?;

            }
            Some(ToManager::BroadcastRootRequest(client_id, group_name)) => {
                let client_channel = clients_store.get(&client_id).unwrap();

                match broadcast_send_store.get(&group_name) {
                    Some(queue) => {
                        client_channel
                            .send(FromManager::BroadcastRootResponse(
                                ServerResponse::Accepted,
                                Some(queue.clone()),
                            ))
                            .await?;
                    }
                    None => {
                        client_channel
                            .send(FromManager::BroadcastRootResponse(
                                ServerResponse::Denied,
                                None,
                            ))
                            .await?;
                    }
                }
            }
            Some(ToManager::BroadcastRequest(client_id, group_id)) => {
                let client_channel = clients_store.get(&client_id).unwrap();

                match broadcast_recv_store.get(&group_id) {
                    Some(queue) => {
                        client_channel
                            .send(FromManager::BroadcastResponse(
                                ServerResponse::Accepted,
                                Some(queue.clone()),
                            ))
                            .await?;
                    }
                    None => {
                        client_channel
                            .send(FromManager::BroadcastResponse(ServerResponse::Denied, None))
                            .await?;
                    }
                }
            }
            Some(ToManager::Close(client_id)) => {
                debug!("Manager Thread: Removing Client {:?} ", client_id);
                let client_channel = clients_store.remove(&client_id).unwrap();
                client_channel.send(FromManager::Close()).await?;
            }
            None => {
                debug!("Manager:Receiver empty");
            }
        }
    }
    //Ok(())
}
