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
    let mut broadcast_groups: HashMap<
        String,
        HashMap<u32, Arc<deadqueue::unlimited::Queue<Message>>>,
    > = HashMap::new();
    let bgroup_counter: Mutex<HashMap<String, u32>> = Mutex::new(HashMap::new());

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
            Some(ToManager::CreateBroadcastGroup(client_id, group_name, n_queues)) => {
                let client_channel: &Sender<FromManager> = clients_store.get(&client_id).unwrap();

                if broadcast_groups.contains_key(&group_name){
                    debug!("Manager Thread: Broadcast Group is already registered");
                } else {
                    let mut bc_hashmap: HashMap<u32, Arc<deadqueue::unlimited::Queue<Message>>> =
                    HashMap::new();

                    for i in 0..n_queues {
                        bc_hashmap.insert(i, Arc::new(TaskQueue::new()));
                    }

                    broadcast_groups.insert(group_name.clone(), bc_hashmap);
                    let mut bgroup_hashmap = bgroup_counter.lock().await;
                    bgroup_hashmap.insert(group_name, 0);
                    drop(bgroup_hashmap);
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
                        client_channel
                            .send(FromManager::SendResponse(
                                ServerResponse::Accepted,
                                Some(queue.clone()),
                            ))
                            .await?;
                    }
                    None => {
                        client_channel
                            .send(FromManager::SendResponse(ServerResponse::Denied, None))
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

                let mut all_queues: Vec<Arc<deadqueue::unlimited::Queue<Message>>> = Vec::new();

                match broadcast_groups.get(&group_name) {
                    Some(bc_group) => {
                        for (_key, value) in bc_group {
                            all_queues.push(value.clone());
                        }

                        client_channel
                            .send(FromManager::BroadcastRootResponse(
                                ServerResponse::Accepted,
                                Some(all_queues),
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

                match broadcast_groups.get(&group_id) {
                    Some(hashmap_group) => {
                        let mut bgroup_hashmap = bgroup_counter.lock().await;
                        let counter = bgroup_hashmap.get_mut(&group_id).unwrap();
                        let actual_counter = *counter;
                        *counter += 1;
                        if *counter == hashmap_group.len() as u32 {
                            *counter = 0;
                        }
                        drop(bgroup_hashmap);

                        match hashmap_group.get(&actual_counter) {
                            Some(queue) => {
                                client_channel
                                    .send(FromManager::BroadcastResponse(
                                        ServerResponse::Accepted,
                                        Some(queue.clone()),
                                    ))
                                    .await?;
                            }
                            None => {
                                debug!("Manager Thread: Error: The number of requests is greater than the number of queues!");
                                client_channel
                                    .send(FromManager::BroadcastResponse(
                                        ServerResponse::Denied,
                                        None,
                                    ))
                                    .await?;
                                let mut bgroup_hashmap = bgroup_counter.lock().await;
                                *bgroup_hashmap.get_mut(&group_id).unwrap() = 0;
                                drop(bgroup_hashmap);
                            }
                        }
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
