use deadqueue::unlimited::Queue;

use std::sync::Arc;

use tokio::sync::mpsc::Sender;

use crate::protocol::ServerResponse;
use crate::server::message::Message;

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
    SendResponse(ServerResponse, Option<Arc<Queue<Message>>>),
    ReceiveResponse(ServerResponse, Option<Arc<Queue<Message>>>),
    BroadcastRootResponse(ServerResponse, Option<Vec<Arc<Queue<Message>>>>),
    BroadcastResponse(ServerResponse, Option<Arc<Queue<Message>>>),
    Close(),
}
