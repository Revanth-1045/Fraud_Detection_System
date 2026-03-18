$opensslPath = "C:\Program Files\Git\usr\bin\openssl.exe"
$password = "kafka-secret"

# Create ssl directory if not exists
New-Item -ItemType Directory -Force -Path "ssl" | Out-Null
Set-Location "ssl"

Write-Host "🔐 Generating CA..."
& $opensslPath req -new -x509 -keyout ca-key -out ca-cert -days 365 -nodes -subj "//CN=Fraud-Detection-CA"

Write-Host "🔑 Generating Broker Keystore..."
keytool -genkey -keystore kafka.server.keystore.jks -alias kafka-server -validity 365 -keyalg RSA -storepass $password -keypass $password -dname "CN=kafka,O=FraudDetection,C=US"

Write-Host "📝 Generating CSR..."
keytool -keystore kafka.server.keystore.jks -alias kafka-server -certreq -file cert-file -storepass $password

Write-Host "✍️ Signing Certificate..."
& $opensslPath x509 -req -CA ca-cert -CAkey ca-key -in cert-file -out cert-signed -days 365 -CAcreateserial -passin pass:$password

Write-Host "📥 Importing Certificates..."
keytool -keystore kafka.server.keystore.jks -alias CARoot -import -file ca-cert -storepass $password -noprompt
keytool -keystore kafka.server.keystore.jks -alias kafka-server -import -file cert-signed -storepass $password -noprompt

Write-Host "🛡️ Creating Truststore..."
keytool -keystore kafka.server.truststore.jks -alias CARoot -import -file ca-cert -storepass $password -noprompt

Write-Host "🐍 Extracting keys for Python clients..."
keytool -importkeystore -srckeystore kafka.server.keystore.jks -destkeystore client.p12 -deststoretype PKCS12 -srcstorepass $password -deststorepass $password -srcalias kafka-server

& $opensslPath pkcs12 -in client.p12 -nokeys -out client-cert.pem -passin pass:$password
& $opensslPath pkcs12 -in client.p12 -nocerts -nodes -out client-key.pem -passin pass:$password

# Cleanup
Remove-Item cert-file, cert-signed, client.p12, .srl -ErrorAction SilentlyContinue

Write-Host "✅ SSL Certificates generated successfully in ./ssl/"
