#!/bin/bash

# Ensure jq and curl are installed
if ! command -v jq &> /dev/null; then
    echo "jq could not be found, installing..."
    sudo apt-get update
    sudo apt-get install -y jq
fi

if ! command -v curl &> /dev/null; then
    echo "curl could not be found, installing..."
    sudo apt-get update
    sudo apt-get install -y curl
fi

# Fetch the latest sealed-secrets version using GitHub API
KUBESEAL_VERSION=$(curl -s https://api.github.com/repos/bitnami-labs/sealed-secrets/tags | jq -r '.[0].name' | cut -c 2-)

# Check if the version was fetched successfully
if [ -z "$KUBESEAL_VERSION" ]; then
    echo "Failed to fetch the latest KUBESEAL_VERSION"
    exit 1
fi

# Download and install kubeseal
curl -OL "https://github.com/bitnami-labs/sealed-secrets/releases/download/v${KUBESEAL_VERSION}/kubeseal-${KUBESEAL_VERSION}-linux-amd64.tar.gz"
tar -xvzf kubeseal-${KUBESEAL_VERSION}-linux-amd64.tar.gz kubeseal
sudo install -m 755 kubeseal /usr/local/bin/kubeseal

# Clean up
rm kubeseal-${KUBESEAL_VERSION}-linux-amd64.tar.gz
rm kubeseal

echo "kubeseal version ${KUBESEAL_VERSION} installed successfully."