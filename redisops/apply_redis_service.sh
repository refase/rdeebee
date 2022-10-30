kubectl apply -f config/sc.yaml
sleep 2
kubectl apply -f config/pv.yaml
sleep 2
kubectl apply -f config/configmap.yaml
sleep 2
kubectl apply -f config/statefulset.yaml
sleep 2
kubectl apply -f config/redis-service.yaml