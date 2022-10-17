use std::{
    fmt::{Debug, Display},
    time::SystemTime,
};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) enum Action {
    READ,
    WRITE,
    UPDATE,
    DELETE,
}

type Payload = Option<Vec<u8>>;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct Event {
    timestamp: SystemTime,
    transaction_id: Uuid,
    action: Action,
    payload: Payload,
}

impl Event {
    pub fn new(action: Action) -> Self {
        let uuid = Uuid::new_v4();
        Self {
            timestamp: SystemTime::now(),
            transaction_id: uuid,
            action,
            payload: None,
        }
    }

    pub(crate) fn id(&self) -> Uuid {
        self.transaction_id
    }

    pub(crate) fn set_id(&mut self, id: Uuid) {
        self.transaction_id = id;
    }

    pub(crate) fn set_payload(&mut self, payload: Payload) {
        self.payload = payload;
    }

    pub(crate) fn timestamp(&self) -> SystemTime {
        self.timestamp
    }

    pub(crate) fn action(&self) -> &Action {
        &self.action
    }

    pub(crate) fn payload(&self) -> Payload {
        self.payload.clone()
    }
}

impl Display for Event {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&format!(
            "Time: {:#?}, ID: {}, Action: {:#?}",
            self.timestamp, self.transaction_id, self.action
        ))
    }
}
