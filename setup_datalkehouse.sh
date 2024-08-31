#!/bin/bash

# Function to exit gracefully
exit_gracefully() {
  echo "Exiting gracefully..."
  exit 1
}

# Function to check if all pods in a namespace are running and ready
check_pods_ready() {
  namespace=$1
  while true; do
    not_ready_pods=$(kubectl get pods -n "$namespace" --no-headers | awk '$3 != "Running" || $2 != "1/1" {print $1}')
    if [ -z "$not_ready_pods" ]; then
      echo "All pods in the $namespace namespace are running and ready."
      break
    else
      echo "Waiting for all pods in the $namespace namespace to be running and ready..."
      sleep 10
    fi
  done
}

# Function to install Minikube if it's not already installed
install_minikube() {
  echo "Minikube is not installed. Installing Minikube..."
  curl -LO https://storage.googleapis.com/minikube/releases/latest/minikube-linux-amd64
  sudo install minikube-linux-amd64 /usr/local/bin/minikube
  if [ $? -ne 0 ]; then
    echo "Failed to install Minikube."
    exit_gracefully
  fi
  echo "Minikube installed successfully."
}

# Check if kubectl is installed
if ! command -v kubectl &> /dev/null; then
  echo "kubectl is not installed. Please install it and try again."
  exit_gracefully
fi

# Check if Minikube is installed, and install it if not
if ! command -v minikube &> /dev/null; then
  install_minikube
fi

# Prompt the user to decide whether to use Minikube
read -p "Do you want to use Minikube? (yes/no): " use_minikube

if [[ "$use_minikube" == "yes" ]]; then
  # Start Minikube with high availability
  echo "Starting Minikube with high availability..."
  minikube start --ha --driver=docker --container-runtime=containerd
  
  if [ $? -ne 0 ]; then
    echo "Failed to start Minikube."
    exit_gracefully
  fi
else
  # Check if a current context exists
  current_context=$(kubectl config current-context)
  
  if [ -z "$current_context" ]; then
    echo "No current Kubernetes context detected."
    exit_gracefully
  else
    echo "Using the current Kubernetes context: $current_context"
  fi
fi



# Set up ArgoCD
echo "Setting up ArgoCD..."
kubectl create ns argocd
kubectl apply -n argocd -f https://raw.githubusercontent.com/argoproj/argo-cd/stable/manifests/install.yaml

# Deploy the ArgoCD application of applications
echo "Deploying the ArgoCD application of applications..."
kubectl apply -f https://raw.githubusercontent.com/joshyorko/open-datalakehouse/main/app-of-apps.yaml

# Monitor the deployment
echo "Monitoring the deployment..."
kubectl get applications -n argocd

# Wait until all pods in the data-lakehouse namespace are running and ready
check_pods_ready "data-lakehouse"

# Access ArgoCD UI (Note: User will have to manually log in via the port-forwarded service)
echo "To access the ArgoCD UI, run the following command in another terminal:"
echo "kubectl port-forward svc/argocd-server -n argocd 8080:443"

# Get the initial ArgoCD password
echo "Getting the initial ArgoCD password..."
argocd_password=$(kubectl -n argocd get secret argocd-initial-admin-secret -o jsonpath="{.data.password}" | base64 --decode)
echo "Initial ArgoCD password: $argocd_password"

echo "Deployment completed successfully. You can now access the services deployed in the data-lakehouse namespace."
