use std::sync::Arc;

use crate::protocol::ServerResponse;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::Mutex;

pub enum ToManager {
    Register(u32, Sender<FromManager>),
    InitQueues(u32, Vec<u32>),
    CreateBroadcastGroup(u32, String, u32),
    SendRequest(u32, u32),
    ReceiveRequest(u32, u32),
    BroadcastRootRequest(u32, String),
    BroadcastRequest(u32, String),
    Close(u32),
}

pub enum FromManager {
    Accept(ServerResponse),
    InitQueuesResponse(ServerResponse),
    CreateBroadcastGroupResponse(ServerResponse),
    SendResponse(ServerResponse, Option<Arc<Sender<Arc<Vec<u8>>>>>),
    ReceiveResponse(ServerResponse, Option<Arc<Mutex<Receiver<Arc<Vec<u8>>>>>>),
    BroadcastRootResponse(ServerResponse, Option<Arc<Sender<Arc<Vec<u8>>>>>),
    BroadcastResponse(ServerResponse, Option<Arc<Mutex<Receiver<Arc<Vec<u8>>>>>>),
    Close(),
}
