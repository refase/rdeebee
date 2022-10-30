use std::{
    fmt::{Debug, Display},
    mem,
    time::SystemTime,
};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) enum Action {
    Read,
    Write,
    Delete,
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
    pub(crate) fn new(action: Action) -> Self {
        Self {
            timestamp: SystemTime::now(),
            transaction_id: Uuid::new_v4(),
            action,
            payload: None,
        }
    }

    pub(crate) fn with_id(id: Uuid, action: Action) -> Self {
        Self {
            timestamp: SystemTime::now(),
            transaction_id: id,
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

    pub(crate) fn action(&self) -> &Action {
        &self.action
    }

    pub(crate) fn payload(&self) -> Payload {
        self.payload.clone()
    }

    pub(crate) fn size(&self) -> usize {
        let systime_alignment = mem::align_of::<SystemTime>();
        let id_alignment = mem::align_of::<Uuid>();
        let action_alignment = mem::align_of::<Action>();
        let payload_alignment = mem::align_of::<Vec<u8>>();
        let mut systime_sz = mem::size_of::<SystemTime>();
        let mut id_sz = mem::size_of::<Uuid>();
        let mut action_sz = mem::size_of::<Action>();
        let mut payload_sz = match &self.payload {
            Some(v) => v.len(),
            None => payload_alignment,
        };

        systime_sz = match systime_sz % systime_alignment {
            0 => systime_sz,
            n => systime_sz + (systime_alignment - n),
        };

        id_sz = match id_sz % id_alignment {
            0 => id_sz,
            n => id_sz + (id_alignment - n),
        };

        action_sz = match action_sz % action_alignment {
            0 => action_sz,
            n => action_sz + (action_alignment - n),
        };

        payload_sz = match payload_sz % payload_alignment {
            0 => payload_sz,
            n => payload_sz + (payload_alignment - n),
        };

        systime_sz + id_sz + action_sz + payload_sz
    }
}

impl Display for Event {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.payload() {
            Some(payload) => {
                let payload_str = bincode::deserialize::<&str>(&payload).unwrap();
                f.write_str(&format!(
                    "Time: {:#?}, ID: {}, Action: {:#?}\nPayload: {}",
                    self.timestamp, self.transaction_id, self.action, payload_str
                ))
            }
            None => f.write_str(&format!(
                "Time: {:#?}, ID: {}, Action: {:#?}",
                self.timestamp, self.transaction_id, self.action
            )),
        }
    }
}

#[cfg(test)]
mod test {
    use super::{Action, Event};

    #[test]
    fn event_size() {
        let event1 = Event::new(Action::Read);
        println!("Event1 size: {}", event1.size());
        let mut event2 = Event::new(Action::Read);
        event2.set_payload(Some(bincode::serialize("This is payload").unwrap()));
        println!("Event2 size: {}", event2.size());
    }
}
