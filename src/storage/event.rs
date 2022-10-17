use std::{
    cell::RefCell,
    fmt::{Debug, Display},
    rc::Rc,
    time::SystemTime,
};

use uuid::Uuid;

#[derive(Debug, Clone)]
pub(crate) enum Action {
    READ,
    WRITE,
    UPDATE,
    DELETE,
}

type Payload = Option<Rc<RefCell<Vec<u8>>>>;

#[derive(Debug, Clone)]
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

    pub(crate) fn set_id(&mut self, id: u64) {
        let id_u = Uuid::from_u128(id as u128);
        self.transaction_id = id_u;
    }

    pub(crate) fn timestamp(&self) -> SystemTime {
        self.timestamp
    }

    pub(crate) fn action(&self) -> &Action {
        &self.action
    }
}

impl Display for Event {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&format!(
            "ID: {}, Action: {:#?}",
            self.transaction_id, self.action
        ))
    }
}
