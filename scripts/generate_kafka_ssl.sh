#!/bin/bash

# Create directory for certificates
mkdir -p ssl
cd ssl

# 1. Create Certificate Authority (CA)
echo "Generating CA..."
openssl req -new -x509 -keyout ca-key -out ca-cert -days 365 -nodes \
  -subj "//CN=Fraud-Detection-CA"

# 2. Create Kafka Broker Keystore
echo "Generating Broker Keystore..."
keytool -keystore kafka.server.keystore.jks -alias kafka-server \
  -validity 365 -genkey -keyalg RSA -storepass kafka-secret -keypass kafka-secret \
  -dname "CN=kafka,O=FraudDetection,C=US"

# 3. Create Certificate Signing Request (CSR)
echo "Generating CSR..."
keytool -keystore kafka.server.keystore.jks -alias kafka-server \
  -certreq -file cert-file -storepass kafka-secret

# 4. Sign the Certificate with CA
echo "Signing Certificate..."
openssl x509 -req -CA ca-cert -CAkey ca-key -in cert-file \
  -out cert-signed -days 365 -CAcreateserial -passin pass:kafka-secret

# 5. Import CA and Signed Cert into Keystore
echo "Importing Certificates into Keystore..."
keytool -keystore kafka.server.keystore.jks -alias CARoot \
  -import -file ca-cert -storepass kafka-secret -noprompt

keytool -keystore kafka.server.keystore.jks -alias kafka-server \
  -import -file cert-signed -storepass kafka-secret -noprompt

# 6. Create Truststore (Client needs this to trust the Broker)
echo "Creating Truststore..."
keytool -keystore kafka.server.truststore.jks -alias CARoot \
  -import -file ca-cert -storepass kafka-secret -noprompt

# 7. Extract keys for Python clients (Producer/Consumer)
echo "Extracting keys for Python clients..."
# Export key from keystore to PKCS12
keytool -importkeystore -srckeystore kafka.server.keystore.jks \
  -destkeystore client.p12 -deststoretype PKCS12 \
  -srcstorepass kafka-secret -deststorepass kafka-secret \
  -srcalias kafka-server

# Convert PKCS12 to PEM (Key and Cert)
openssl pkcs12 -in client.p12 -nokeys -out client-cert.pem -passin pass:kafka-secret
openssl pkcs12 -in client.p12 -nocerts -nodes -out client-key.pem -passin pass:kafka-secret

# Clean up intermediate files
rm cert-file cert-signed client.p12 .srl

echo "✅ SSL Certificates generated successfully in ./ssl/"
