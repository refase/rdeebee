kubectl apply -f sc.yaml
sleep 2
kubectl apply -f pv.yaml
sleep 2
kubectl apply -f configmap.yaml
sleep 2
kubectl apply -f statefulset.yaml
sleep 2
kubectl apply -f redis-service.yaml