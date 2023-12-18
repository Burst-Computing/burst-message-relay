use log::debug;

use std::collections::HashMap;
use std::error::Error;
use std::sync::Arc;

use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::Mutex;

use crate::protocol::ServerResponse;
use crate::server::enums::{FromManager, ToManager};
use crate::server::message::Message;

type TaskQueue = deadqueue::unlimited::Queue<Message>;

pub async fn start(mut receiver: Receiver<ToManager>) -> Result<(), Box<dyn Error>> {
    // Creating HashMap to store clients and queues
    let mut clients_store: HashMap<u32, Sender<FromManager>> = HashMap::new();
    let mut queues_store: HashMap<u32, Arc<deadqueue::unlimited::Queue<Message>>> = HashMap::new();
    let mut broadcast_groups: HashMap<String, Arc<deadqueue::unlimited::Queue<Message>>> = HashMap::new();

    let send_counter: Mutex<u32> = Mutex::new(0);
    let broadcast_counter: Mutex<u32> = Mutex::new(0);

    loop {
        match receiver.recv().await {
            Some(ToManager::Register(client_id, client_channel)) => {
                debug!("Manager Thread: Register [Client: ID: {:?}]", client_id);

                let cc_copy = client_channel.clone();

                // Register client

                if find_key(client_id, &clients_store) {
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
                    if find_key(name, &queues_store) {
                        debug!("Manager Thread: Queue id is already registered");
                    } else {
                        queues_store.insert(name, Arc::new(TaskQueue::new()));
                    }
                }

                client_channel
                    .send(FromManager::InitQueuesResponse(ServerResponse::Accepted))
                    .await?;
            }
            Some(ToManager::CreateBroadcastGroup(client_id, group_name, _)) => {
                let client_channel: &Sender<FromManager> = clients_store.get(&client_id).unwrap();

                if broadcast_groups.contains_key(&group_name) {
                    debug!("Manager Thread: Broadcast Group is already registered");
                } else {

                    broadcast_groups.insert(group_name, Arc::new(TaskQueue::new()));
                }

                client_channel
                    .send(FromManager::CreateBroadcastGroupResponse(
                        ServerResponse::Accepted,
                    ))
                    .await?;
            }
            Some(ToManager::SendRequest(client_id, queue_id)) => {
                let client_channel = clients_store.get(&client_id).unwrap();

                match queues_store.get(&queue_id) {
                    Some(queue) => {
                        let mut send_id = send_counter.lock().await;

                        client_channel
                            .send(FromManager::SendResponse(
                                ServerResponse::Accepted,
                                *send_id,
                                Some(queue.clone()),
                            ))
                            .await?;

                        *send_id += 1;
                        drop(send_id);
                    }
                    None => {
                        client_channel
                            .send(FromManager::SendResponse(ServerResponse::Denied, 0, None))
                            .await?;
                    }
                }
            }
            Some(ToManager::ReceiveRequest(client_id, queue_id)) => {
                let client_channel = clients_store.get(&client_id).unwrap();

                match queues_store.get(&queue_id) {
                    Some(queue) => {
                        client_channel
                            .send(FromManager::ReceiveResponse(
                                ServerResponse::Accepted,
                                Some(queue.clone()),
                            ))
                            .await?;
                    }
                    None => {
                        client_channel
                            .send(FromManager::ReceiveResponse(ServerResponse::Denied, None))
                            .await?;
                    }
                }
            }
            Some(ToManager::BroadcastRootRequest(client_id, group_name)) => {
                let client_channel = clients_store.get(&client_id).unwrap();

                match broadcast_groups.get(&group_name) {
                    Some(bc_group) => {

                        let mut bc_id = broadcast_counter.lock().await;
                        client_channel
                            .send(FromManager::BroadcastRootResponse(
                                ServerResponse::Accepted,
                                *bc_id,
                                Some(bc_group.clone())
                            ))
                            .await?;

                        *bc_id += 1;
                        drop(bc_id);
                    }
                    None => {
                        client_channel
                            .send(FromManager::BroadcastRootResponse(
                                ServerResponse::Denied,
                                0,
                                None,
                            ))
                            .await?;
                    }
                }
            }
            Some(ToManager::BroadcastRequest(client_id, group_id)) => {
                let client_channel = clients_store.get(&client_id).unwrap();

                match broadcast_groups.get(&group_id) {
                    Some(hashmap_group) => {

                        client_channel
                                    .send(FromManager::BroadcastResponse(
                                        ServerResponse::Accepted,
                                        Some(hashmap_group.clone()),
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

fn find_key<K: std::cmp::PartialEq<u32>, V>(key_to_find: u32, hashmap: &HashMap<K, V>) -> bool {
    if hashmap.is_empty() {
        return false;
    } else {
        for (key, _value) in hashmap {
            if *key == key_to_find {
                return true;
            }
        }

        return false;
    }
}
