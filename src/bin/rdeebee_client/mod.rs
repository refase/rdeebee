use std::{env, str};

use anyhow::anyhow;
use etcd_client::{Client, LockOptions};
use protobuf::EnumOrUnknown;
use rdeebee::wire_format::operation::{Operation, Request};
use tracing::{debug, info};

use crate::Action;

pub(crate) struct SequenceSvc {
    client: Client,
    counter_key: String,
    lock_key: String,
}

impl SequenceSvc {
    pub(crate) async fn new() -> Self {
        let etcd = env::var("ETCD").expect("etcd host:port undefined");
        let counter_key = env::var("COUNTER_KEY").expect("Counter key undefined");
        let lock_key = env::var("LOCK_KEY").expect("Lock key undefined");
        let client = Client::connect([etcd], None)
            .await
            .expect("failed to create client");
        Self {
            client,
            counter_key,
            lock_key,
        }
    }

    pub(crate) async fn create_request(
        &mut self,
        action: Action,
        key: &str,
        payload: Option<String>,
    ) -> anyhow::Result<Request> {
        let mut request = Request::new();
        request.key = key.to_string();
        request.op = match action {
            Action::Read => EnumOrUnknown::new(Operation::Read),
            Action::Write => EnumOrUnknown::new(Operation::Write),
            Action::Delete => EnumOrUnknown::new(Operation::Delete),
        };

        // If lock not released in 10 seconds,
        // node is probably dead.
        let lease = self
            .client
            .lease_grant(10, None)
            .await
            .expect("lease failed");
        let lease_id = lease.id();

        let lock_options = LockOptions::new().with_lease(lease_id);
        let resp = self
            .client
            .lock(self.lock_key.clone(), Some(lock_options))
            .await
            .expect("failed to get lock");
        debug!("Locked key resp: {:#?}", resp);

        let lock_key = str::from_utf8(resp.key()).expect("failed to get lock key");

        let resp = self
            .client
            .get(self.counter_key.clone(), None)
            .await
            .expect("failed to get counter");

        self.client
            .unlock(lock_key)
            .await
            .expect("failed unlock key");

        let kv = resp.kvs();

        if kv.len() == 0 {
            self.client
                .put(self.counter_key.clone(), "1", None)
                .await
                .expect("failed to put value");
            info!("Initialized counter: {:#?}", resp);
            return Err(anyhow!("No such counter key"));
        }

        let seq = match action {
            Action::Delete | Action::Write => match kv[0].value_str() {
                Ok(val) => match val.parse::<u64>() {
                    Ok(val) => val,
                    Err(e) => return Err(anyhow!("{e}")),
                },
                Err(e) => return Err(anyhow!("{e}")),
            },
            Action::Read => 0,
        };

        request.seq = seq;

        if let Some(payload) = payload {
            let payload = bincode::serialize(&payload)?;
            request.payload = payload;
        }
        Ok(request)
    }
}
