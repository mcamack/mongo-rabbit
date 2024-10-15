
helm install my-rabbitmq bitnami/rabbitmq -f values.yaml

kubectl port-forward svc/my-rabbitmq 15672:15672

http://localhost:15672/#/