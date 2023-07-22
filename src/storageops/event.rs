use std::{fmt::Display, mem, time::SystemTime};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Action defines the actions that can be taken.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) enum Action {
    Read,
    Write(u8),
    Delete,
}

/// All payload within events will be serialized to a Vec<u8>.
/// This allows for variable sized payloads.
/// Payloads are only allowed in events that are of Write Action type.
type Payload = Option<Vec<u8>>;

/// Event is the structure that the engine understands.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct Event {
    seq: u64,         // Sequence ID of the event (within some flow).
    tid: Uuid,        // Transaction ID of the event.
    action: Action,   // The action type.
    payload: Payload, // The payload, if action type is Write.
}

impl Event {
    pub(crate) fn new(action: Action, seq: u64) -> Self {
        Self {
            seq,
            tid: Uuid::new_v4(),
            action,
            payload: None,
        }
    }

    pub(crate) fn transaction_id(&self) -> Uuid {
        self.tid
    }

    pub(crate) fn set_payload(&mut self, payload: Payload) {
        // Payload can be set only if Action type is Write.
        // And the payload can be set only the first time.
        // Any further attempt to set payload fails without error.
        if self.action == Action::Write(0) {
            self.action = Action::Write(1);
            self.payload = payload;
        }
    }

    pub(crate) fn action(&self) -> Action {
        self.action.clone()
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
                    self.seq, self.tid, self.action, payload_str
                ))
            }
            None => f.write_str(&format!(
                "Time: {:#?}, ID: {}, Action: {:#?}",
                self.seq, self.tid, self.action
            )),
        }
    }
}
