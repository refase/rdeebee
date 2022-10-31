#!/bin/bash
kubectl apply -f configs/sc.yaml
kubectl apply -f configs/pv.yaml

helm repo add hashicorp https://helm.releases.hashicorp.com
helm install consul hashicorp/consul --create-namespace --namespace consul --values configs/valus.yaml