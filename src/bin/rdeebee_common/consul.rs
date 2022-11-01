use std::{collections::HashMap, env};

use rand::Rng;
use rs_consul::{
    Config, Consul, ConsulError, GetServiceNodesRequest, RegisterEntityPayload,
    RegisterEntityService, ServiceNode,
};

pub(crate) struct ConsulRegister {
    consul_client: Consul,
    pod_ip: String,
}

impl ConsulRegister {
    pub(crate) fn new(consul_svc: &str) -> Self {
        let config = Config {
            address: consul_svc.to_owned(),
            token: None,
        };
        let client = Consul::new(config);
        let pod_ip = env::var("POD_IP").expect("Pod IP not defined");
        Self {
            consul_client: client,
            pod_ip,
        }
    }
    pub(crate) async fn register(&self, svc: &str) -> Result<(), ConsulError> {
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

    pub(crate) async fn get_node(&self) -> Result<ServiceNode, ConsulError> {
        let request = GetServiceNodesRequest {
            service: "Database",
            near: None,
            passing: true,
            filter: None,
        };

        let nodes = self
            .consul_client
            .get_service_nodes(request, None)
            .await?
            .response;
        let mut rng = rand::thread_rng();
        let rnd: usize = rng.gen();
        let index = rnd % nodes.len();
        Ok(nodes[index].clone())
    }

    pub(crate) async fn get_leaders(&self) -> Result<Vec<ServiceNode>, ConsulError> {
        let request = GetServiceNodesRequest {
            service: "Database",
            near: None,
            passing: true,
            filter: Some("Node.Meta.leader==false"),
        };

        Ok(self
            .consul_client
            .get_service_nodes(request, None)
            .await?
            .response)
    }
}
