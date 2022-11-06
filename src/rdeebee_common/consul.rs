use std::collections::HashMap;

use rand::Rng;
use rs_consul::{
    Config, Consul, GetServiceNodesRequest, RegisterEntityPayload, RegisterEntityService,
    ServiceNode,
};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ConsulErrors {
    #[error(transparent)]
    RequestError(#[from] reqwest::Error),
    #[error(transparent)]
    ConsulError(#[from] rs_consul::ConsulError),
}

pub struct ConsulRegister {
    consul_client: Consul,
    config: Config,
    pod_ip: String,
}

impl ConsulRegister {
    pub fn new(consul_svc: &str, pod_ip: String) -> Self {
        let config = Config {
            address: consul_svc.to_owned(),
            token: None,
        };
        let client = Consul::new(config.clone());

        Self {
            consul_client: client,
            config,
            pod_ip,
        }
    }

    fn build_url(&self, request: GetServiceNodesRequest<'_>) -> String {
        let mut url = String::new();
        url.push_str(&format!(
            "http://{}/v1/health/service/{}",
            self.config.address, request.service
        ));
        url.push_str(&format!("?passing={}", request.passing));
        if let Some(near) = request.near {
            url.push_str(&format!("&near={}", near));
        }
        if let Some(filter) = request.filter {
            url.push_str(&format!("&filter={}", filter));
        }
        url
    }

    pub async fn register(&self, svc: &str) -> Result<(), rs_consul::ConsulError> {
        println!("register");
        let service = RegisterEntityService {
            ID: None,
            Service: svc.to_owned(),
            Tags: vec![],
            TaggedAddresses: HashMap::new(),
            Meta: HashMap::new(),
            Port: Some(2048),
            Namespace: None,
        };
        let node = format!("node-{}", self.pod_ip.clone());
        let mut meta = HashMap::new();
        meta.insert("leader".to_string(), "false".to_string());

        let payload = RegisterEntityPayload {
            ID: None,
            Node: node,
            Address: self.pod_ip.clone(),
            Datacenter: None,
            TaggedAddresses: HashMap::new(),
            NodeMeta: meta,
            Service: Some(service),
            Check: None,
            SkipNodeUpdate: None,
        };
        self.consul_client.register_entity(&payload).await
    }

    pub async fn get_node(&self) -> Result<ServiceNode, ConsulErrors> {
        let request = GetServiceNodesRequest {
            service: "Database",
            near: None,
            passing: true,
            filter: None,
        };

        let response = reqwest::get(self.build_url(request)).await?;
        let bytes = response.bytes().await?;
        let nodes = serde_json::from_slice::<Vec<ServiceNode>>(&bytes)
            .map_err(rs_consul::ConsulError::ResponseDeserializationFailed)?;

        let mut rng = rand::thread_rng();
        let rnd: usize = rng.gen();
        let index = rnd % nodes.len();
        Ok(nodes[index].clone())
    }

    pub async fn get_leaders(&self) -> Result<Vec<ServiceNode>, ConsulErrors> {
        let request = GetServiceNodesRequest {
            service: "Database",
            near: None,
            passing: true,
            filter: Some("Node.Meta.leader==false"),
        };

        let response = reqwest::get(self.build_url(request)).await?;
        let bytes = response.bytes().await?;

        Ok(serde_json::from_slice::<Vec<ServiceNode>>(&bytes)
            .map_err(rs_consul::ConsulError::ResponseDeserializationFailed)?)
    }
}
