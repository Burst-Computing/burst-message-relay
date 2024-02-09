use std::fmt::Display;

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum ClientOperation {
    CreateBcGroup,
    Send,
    Receive,
    BroadcastRoot,
    Broadcast,
    Close,
    Error,
}

impl Display for ClientOperation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl From<u32> for ClientOperation {
    fn from(n: u32) -> Self {
        match n {
            0 => ClientOperation::CreateBcGroup,
            1 => ClientOperation::Send,
            2 => ClientOperation::Receive,
            3 => ClientOperation::BroadcastRoot,
            4 => ClientOperation::Broadcast,
            5 => ClientOperation::Close,
            _ => ClientOperation::Error,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum ServerResponse {
    Denied,
    Accepted,
    Close,
    Error,
}

impl Display for ServerResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl From<u32> for ServerResponse {
    fn from(n: u32) -> Self {
        match n {
            0 => ServerResponse::Denied,
            1 => ServerResponse::Accepted,
            2 => ServerResponse::Close,
            _ => ServerResponse::Error,
        }
    }
}
