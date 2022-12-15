#!/bin/bash
sudo apt install -y etcd-client

# Get the service port
PORT=$(kubectl get svc etcd-client -o go-template='{{range.spec.ports}}{{if .nodePort}}{{.nodePort}}{{"\n"}}{{end}}{{end}}')

# Set up the failover ID
# kubectl exec -it etcd-0 -- etcdctl set failover_id "1"
ETCDCTL_API=3 etcdctl put failover_id 1 --endpoints=localhost:$PORT
# Set up the ID key
ETCDCTL_API=3 etcdctl put id_key 1 --endpoints=localhost:$PORT