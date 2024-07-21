#!/bin/bash

# Variables
KEYSTORE_PASSWORD="jamisonVolvoMercedes724"
KEYSTORE_NAME="dremio.jks"
CERTIFICATE_NAME="dremio.crt"
NAMESPACE="data-lakehouse"

# Generate a Keystore and a Self-Signed Certificate
keytool -genkeypair -alias dremio -keyalg RSA -keysize 2048 -storetype JKS -keystore $KEYSTORE_NAME -validity 365 -storepass $KEYSTORE_PASSWORD <<EOF
John Doe
IT
ExampleCorp
Worcester
MA
US
yes
EOF

# Export the Certificate (Optional)
keytool -exportcert -alias dremio -file $CERTIFICATE_NAME -keystore $KEYSTORE_NAME -storepass $KEYSTORE_PASSWORD

# Create Kubernetes Secrets for the Keystore
kubectl create secret generic dremio-keystore-password --from-literal=password=$KEYSTORE_PASSWORD -n $NAMESPACE
kubectl create secret generic dremio-tls-secret --from-file=dremio.jks=$KEYSTORE_NAME -n $NAMESPACE

# Verify the Secrets
kubectl get secrets -n $NAMESPACE
kubectl describe secret dremio-keystore-password -n $NAMESPACE
kubectl describe secret dremio-tls-secret -n $NAMESPACE
