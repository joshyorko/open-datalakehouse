#!/bin/bash

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to print colorized output
print_status() {
  local color=$1
  local message=$2
  echo -e "${color}${message}${NC}"
}

# Function to exit gracefully
exit_gracefully() {
  print_status "${RED}" "Exiting gracefully..."
  exit 1
}

# Function to check if all pods in a namespace are running and ready
check_pods_ready() {
  local namespace=$1
  local timeout=${2:-300} # Default timeout of 5 minutes
  local start_time=$(date +%s)

  while true; do
    local not_ready_pods=$(kubectl get pods -n "$namespace" --no-headers 2>/dev/null | awk '$3 != "Running" || $2 != "1/1" {print $1}')
    if [ -z "$not_ready_pods" ]; then
      print_status "${GREEN}" "✔ All pods in the $namespace namespace are running and ready."
      return 0
    else
      local current_time=$(date +%s)
      local elapsed_time=$((current_time - start_time))
      if [ $elapsed_time -ge $timeout ]; then
        print_status "${RED}" "❌ Timeout reached. Some pods in $namespace are still not ready."
        return 1
      fi
      print_status "${YELLOW}" "⏳ Waiting for pods in $namespace to be ready... (${elapsed_time}s elapsed)"
      sleep 10
    fi
  done
}

# Function to install Minikube
install_minikube() {
  print_status "${YELLOW}" "⏳ Minikube is not installed. Installing Minikube..."
  curl -LO https://storage.googleapis.com/minikube/releases/latest/minikube-linux-amd64
  sudo install minikube-linux-amd64 /usr/local/bin/minikube
  if [ $? -ne 0 ]; then
    print_status "${RED}" "❌ Failed to install Minikube."
    exit_gracefully
  fi
  print_status "${GREEN}" "✔ Minikube installed successfully."
}

# Function to display summary of deployed services
display_summary() {
  print_status "${GREEN}" "\n📊 Data Lakehouse Deployment Summary:"
  echo "----------------------------------------"
  kubectl get services -n data-lakehouse
  echo "----------------------------------------"
  print_status "${YELLOW}" "To access these services, you may need to set up port-forwarding or use a LoadBalancer."
}

# Main script starts here
print_status "${GREEN}" "🚀 Starting Data Lakehouse Setup"

# Check if kubectl is installed
if ! command -v kubectl &> /dev/null; then
  print_status "${RED}" "❌ kubectl is not installed. Please install it and try again."
  exit_gracefully
fi

# Prompt for Minikube usage
print_status "${YELLOW}" "Do you want to use Minikube? (yes/no)"
read -p "Enter your choice: " use_minikube

if [[ "$use_minikube" == "yes" ]]; then
  # Check if Minikube is installed, and install it if not
  if ! command -v minikube &> /dev/null; then
    install_minikube
  fi

  print_status "${YELLOW}" "Starting Minikube with high availability..."
  minikube start --ha --driver=docker --container-runtime=containerd
  
  if [ $? -ne 0 ]; then
    print_status "${RED}" "❌ Failed to start Minikube."
    exit_gracefully
  fi
  print_status "${GREEN}" "✔ Minikube started successfully."
else
  # Check for existing Kubernetes context
  current_context=$(kubectl config current-context 2>/dev/null)
  
  if [ -z "$current_context" ]; then
    print_status "${RED}" "❌ No current Kubernetes context detected and Minikube usage declined."
    exit_gracefully
  else
    print_status "${GREEN}" "✔ Using the current Kubernetes context: $current_context"
  fi
fi

# Set up Longhorn for storage
print_status "${YELLOW}" "⏳ Setting up Longhorn for storage..."
kubectl create ns longhorn-system
helm repo add longhorn https://charts.longhorn.io
helm repo update
helm install longhorn longhorn/longhorn --namespace longhorn-system
check_pods_ready "longhorn-system"

# Set up ArgoCD
print_status "${YELLOW}" "⏳ Setting up ArgoCD..."
kubectl create ns argocd
kubectl apply -n argocd -f https://raw.githubusercontent.com/argoproj/argo-cd/stable/manifests/install.yaml
check_pods_ready "argocd"

# Deploy the ArgoCD application of applications
print_status "${YELLOW}" "⏳ Deploying the ArgoCD application of applications..."
kubectl apply -n argocd -f https://raw.githubusercontent.com/joshyorko/open-datalakehouse/main/app-of-apps.yaml

# Monitor the deployment
print_status "${YELLOW}" "⏳ Monitoring the deployment..."
kubectl get applications -n argocd

# Wait until all pods in the data-lakehouse namespace are running and ready
print_status "${YELLOW}" "⏳ Waiting for all components in the data-lakehouse namespace to be ready..."
check_pods_ready "data-lakehouse" 600 # Increased timeout to 10 minutes

# Get the initial ArgoCD password
print_status "${YELLOW}" "⏳ Getting the initial ArgoCD password..."
argocd_password=$(kubectl -n argocd get secret argocd-initial-admin-secret -o jsonpath="{.data.password}" | base64 --decode)
print_status "${GREEN}" "✔ Initial ArgoCD password: $argocd_password"

# Display summary of deployed services
display_summary

print_status "${GREEN}" "🎉 Deployment completed successfully!"
print_status "${YELLOW}" "To access the ArgoCD UI, run the following command in another terminal:"
echo "kubectl port-forward svc/argocd-server -n argocd 8080:443"