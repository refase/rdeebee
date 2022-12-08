#!/bin/bash
# Set up the failover ID
kubectl exec -it etcd-0 -- etcdctl set failover_id "1"
# Set up the ID key
kubectl exec -it etcd-0 -- etcdctl set id_key "1"