use std::fmt::Debug;

use crate::protocol::ClientOperation;

#[derive(Clone)]
pub struct Message {
    pub id: u32,
    pub sender_id: u32,
    pub op_id: ClientOperation,
    pub chunk_id: u32,
    pub last_chunk: bool,
    pub all_mess_len: u32,
    pub bytes: Vec<u8>,
}

impl Message {
    pub fn new(
        id: u32,
        sender_id: u32,
        op_id: ClientOperation,
        chunk_id: u32,
        last_chunk: bool,
        all_mess_len: u32,
        bytes: Vec<u8>,
    ) -> Message {
        Message {
            id: id,
            sender_id: sender_id,
            op_id: op_id,
            chunk_id: chunk_id,
            last_chunk: last_chunk,
            all_mess_len: all_mess_len,
            bytes: bytes,
        }
    }
}

impl Debug for Message {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Message")
            .field("id", &self.id)
            .field("sender_id", &self.sender_id)
            .field("op_id", &self.op_id)
            .field("chunk_id", &self.chunk_id)
            .field("last_chunk", &self.last_chunk)
            .field("all_mess_len", &self.all_mess_len)
            .field("bytes", &self.bytes.len())
            .finish()
    }
}
