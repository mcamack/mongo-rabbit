helm install my-mongodb bitnami/mongodb -f values.yaml
k get secret/my-mongodb-ca -o jsonpath='{.data.mongodb-ca-key}' | base64 --decode > ./ca.key
k get secret/my-mongodb-ca -o jsonpath='{.data.mongodb-ca-cert}' | base64 --decode > ./ca.crt
cat ca.key ca.crt > client.pem

from client:
tlsCAFile='ca.crt'
tlsCertificateKeyFile='client.pem'
tlsAllowInvalidCertificates=False
tlsAllowInvalidHostnames=True

Create TLS Secrets:
What docs say on how to create "tls" secret:
```
kubectl create secret tls "mongodb-tls"  --cert="mongodb-0.crt" --key="mongodb-0.key"
kubectl patch secret "mongodb-tls" -p="{\"data\":{\"ca.crt\": \"$(cat ca.crt | base64 -w0 )\"}}"
```

Your client certificate and private key must be in the same .pem file.
cat key.pem cert.pem > combined.pem

generated-key2.pem is the combined pem:
```
-----BEGIN CERTIFICATE-----
MI......bZ
-----END CERTIFICATE-----
-----BEGIN PRIVATE KEY-----
MI......CQ
-----END PRIVATE KEY-----
```



WARNING: Kubernetes configuration file is group-readable. This is insecure. Location: /home/argyle/.kube/config
WARNING: Kubernetes configuration file is world-readable. This is insecure. Location: /home/argyle/.kube/config
NAME: my-mongodb
LAST DEPLOYED: Thu Sep 12 22:14:53 2024
NAMESPACE: rabbitmq
STATUS: deployed
REVISION: 1
TEST SUITE: None
NOTES:
CHART NAME: mongodb
CHART VERSION: 15.6.23
APP VERSION: 7.0.14

** Please be patient while the chart is being deployed **

MongoDB&reg; can be accessed on the following DNS name(s) and ports from within your cluster:

    my-mongodb-0.my-mongodb-headless.rabbitmq.svc.cluster.local:27017

To get the root password run:

    export MONGODB_ROOT_PASSWORD=$(kubectl get secret --namespace rabbitmq my-mongodb -o jsonpath="{.data.mongodb-root-password}" | base64 -d)

To connect to your database, create a MongoDB&reg; client container:

    kubectl run --namespace rabbitmq my-mongodb-client --rm --tty -i --restart='Never' --env="MONGODB_ROOT_PASSWORD=$MONGODB_ROOT_PASSWORD" --image docker.io/bitnami/mongodb:7.0.14-debian-12-r0 --command -- bash

Then, run the following command:
    mongosh admin --host "my-mongodb-0.my-mongodb-headless.rabbitmq.svc.cluster.local:27017" --authenticationDatabase admin -u $MONGODB_ROOT_USER -p $MONGODB_ROOT_PASSWORD

To connect to your database nodes from outside, you need to add both primary and secondary nodes hostnames/IPs to your Mongo client. To obtain them, follow the instructions below:

  NOTE: It may take a few minutes for the LoadBalancer IPs to be available.
        Watch the status with: 'kubectl get svc --namespace rabbitmq -l "app.kubernetes.io/name=mongodb,app.kubernetes.io/instance=my-mongodb,app.kubernetes.io/component=mongodb,pod" -w'

    MongoDB&reg; nodes domain: You will have a different external IP for each MongoDB&reg; node. You can get the list of external IPs using the command below:

        echo "$(kubectl get svc --namespace rabbitmq -l "app.kubernetes.io/name=mongodb,app.kubernetes.io/instance=my-mongodb,app.kubernetes.io/component=mongodb,pod" -o jsonpath='{.items[*].status.loadBalancer.ingress[0].ip}' | tr ' ' '\n')"

    MongoDB&reg; nodes port: 27017

WARNING: There are "resources" sections in the chart not set. Using "resourcesPreset" is not recommended for production. For production installations, please set the following values according to your workload needs:
  - arbiter.resources
  - externalAccess.autoDiscovery.resources
  - resources
+info https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/