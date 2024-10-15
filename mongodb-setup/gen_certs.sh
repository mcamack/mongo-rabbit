# Create the Certificate Authority (CA)
## Generate the root private key
openssl genrsa -out ca.key 4096
## Generate the root certificate
openssl req -x509 -new -nodes -key ca.key -sha256 -days 3650 -out ca.crt -subj "/C=US/ST=YourState/L=YourCity/O=YourOrg/OU=YourOrgUnit/CN=localhost"


# Generate the Server Certificate for MongoDB
# Generate the server private key
openssl genrsa -out mongodb.key 4096
# Generate a certificate signing request (CSR) for the server certificate
openssl req -new -key mongodb.key -out mongodb.csr -subj "/C=US/ST=YourState/L=YourCity/O=YourOrg/OU=YourOrgUnit/CN=localhost"
# Generate the server certificate signed by the CA
openssl x509 -req -in mongodb.csr -CA ca.crt -CAkey ca.key -CAcreateserial -out mongodb.crt -days 3650 -sha256

# Client Secrets
# Generate the client private key
openssl genrsa -out client.key 4096
# Generate a CSR for the client certificate
openssl req -new -key client.key -out client.csr -subj "/C=US/ST=YourState/L=YourCity/O=YourOrg/OU=YourOrgUnit/CN=localhost"
# Generate the client certificate signed by the CA
openssl x509 -req -in client.csr -CA ca.crt -CAkey ca.key -CAcreateserial -out client.crt -days 3650 -sha256


cat mongodb.crt mongodb.key > mongodb.pem
cat client.crt client.key > mongodb-client.pem
kubectl create secret tls "mongodb-tls"  --cert="mongodb.crt" --key="mongodb.key"
# kubectl patch secret "mongodb-tls" -p="{\"data\":{\"ca.crt\": \"$(cat ca.crt | base64 -w0 )\"}}"
kubectl patch secret "mongodb-tls" -p="{\"data\":{\"mongodb-ca-cert\": \"$(cat ca.crt | base64 -w0 )\"}}"
kubectl patch secret "mongodb-tls" -p="{\"data\":{\"mongodb-ca-key\": \"$(cat ca.key | base64 -w0 )\"}}"
