#!/bin/bash

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Exit immediately if a command exits with a non-zero status
set -e  
# Catch errors in pipelines for more robust error handling
set -o pipefail

# Function to print status messages with colors
print_status() {
  local color=$1
  local message=$2
  echo -e "${color}${message}${NC}"
}

# Function to handle graceful exit in case of errors
exit_gracefully() {
  print_status "${RED}" "Exiting gracefully..."
  exit 1
}

# Wait for all resources (pods, deployments, etc.) in the namespace to be in a ready state
wait_for_namespace_resources() {
  local namespace=$1
  local timeout=${2:-300}  # Timeout in seconds, default to 300

  # Check if the namespace exists
  if ! kubectl get namespace "$namespace" &>/dev/null; then
    print_status "${RED}" "\u274c Namespace $namespace does not exist."
    return 1
  fi

  print_status "${YELLOW}" "\u231b Waiting for resources in the namespace $namespace to be ready..."

  # Wait for all pods in the namespace to be ready
  if ! kubectl wait --for=condition=Ready pods --all -n "$namespace" --timeout=${timeout}s; then
    print_status "${RED}" "\u274c Timeout reached. Some pods in $namespace are still not ready."
    return 1
  fi

  # Wait for all deployments in the namespace to be available
  if ! kubectl wait --for=condition=available deployments --all -n "$namespace" --timeout=${timeout}s; then
    print_status "${RED}" "\u274c Timeout reached. Some deployments in $namespace are still not ready."
    return 1
  fi

  print_status "${GREEN}" "\u2714 All resources in $namespace are ready."
  return 0
}

# Function to install a tool if it is not already installed
install_tool() {
  local tool_name=$1
  local install_command=$2

  # Check if the tool is already installed
  if ! command -v "$tool_name" &> /dev/null; then
    print_status "${YELLOW}" "\u231b Installing $tool_name..."
    # Execute the install command without using eval to avoid security risks
    bash -c "$install_command" || {
      print_status "${RED}" "\u274c Failed to install $tool_name."
      exit_gracefully
    }
    print_status "${GREEN}" "\u2714 $tool_name installed successfully."
  else
    print_status "${GREEN}" "$tool_name is already installed."
  fi
}

# Function to set up the Kubernetes platform (minikube, k3s, or current context)
setup_platform() {
  local platform=$1
  case $platform in
    "minikube")
      # Install Minikube if not installed
      install_tool "minikube" "curl -LO https://storage.googleapis.com/minikube/releases/latest/minikube-linux-amd64 && sudo install minikube-linux-amd64 /usr/local/bin/minikube"
      print_status "${YELLOW}" "Starting Minikube with high availability..."
      # Start Minikube with specified resources
      minikube start
      ;;

    "k3s")
      # Install K3s if not installed
      install_tool "k3s" "curl -sfL https://get.k3s.io | sh -s - --write-kubeconfig-mode 644"
      print_status "${YELLOW}" "Starting K3s..."
      # Start K3s service if it is not already running
      if ! systemctl is-active --quiet k3s; then
        sudo systemctl start k3s
      fi
      ;;

    "current")
      # Use the current Kubernetes context if available
      current_context=$(kubectl config current-context 2>/dev/null)
      if [ -z "$current_context" ]; then
        print_status "${RED}" "\u274c No current Kubernetes context detected. Please set up a cluster or specify --platform as minikube or k3s."
        exit_gracefully
      else
        print_status "${GREEN}" "\u2714 Using the current Kubernetes context: $current_context"
      fi
      ;;

    *)
      # Handle invalid platform specification
      print_status "${RED}" "\u274c Invalid platform specified. Use --platform with current, minikube, or k3s."
      exit_gracefully
      ;;
  esac
}

# Function to parse the script arguments
parse_arguments() {
  PLATFORM="current"
  while [[ "$#" -gt 0 ]]; do
    case $1 in
      --platform)
        PLATFORM="$2"
        shift
        ;;
      *)
        # Handle unknown parameters
        print_status "${RED}" "Unknown parameter passed: $1"
        exit_gracefully
        ;;
    esac
    shift
  done
}

# Main script starts here
print_status "${GREEN}" "\ud83d\ude80 Starting Data Lakehouse Setup"

# Install kubectl if not installed
install_tool "kubectl" "curl -LO https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl && sudo install -o root -g root -m 0755 kubectl /usr/local/bin/kubectl"

# Install Helm if not installed
install_tool "helm" "curl https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash"

# Parse script arguments
parse_arguments "$@"

# Set up the platform based on the provided argument
setup_platform "$PLATFORM"

# Set up ArgoCD
print_status "${YELLOW}" "\u231b Setting up ArgoCD..."
# Create the argocd namespace if it does not exist
if ! kubectl get namespace argocd &>/dev/null; then
  kubectl create ns argocd
fi
# Apply the ArgoCD installation manifests
kubectl apply -n argocd -f https://raw.githubusercontent.com/argoproj/argo-cd/stable/manifests/install.yaml
# Wait for all ArgoCD resources to be ready
wait_for_namespace_resources "argocd"

# Deploy the ArgoCD application of applications
print_status "${YELLOW}" "\u231b Deploying the ArgoCD application of applications..."
kubectl apply -n argocd -f https://raw.githubusercontent.com/joshyorko/open-datalakehouse/main/app-of-apps.yaml

# Monitor the ArgoCD deployment
print_status "${YELLOW}" "\u231b Monitoring the deployment..."
kubectl get applications -n argocd

# Get the initial password for ArgoCD
print_status "${YELLOW}" "\u231b Getting the initial ArgoCD password..."
argocd_password=$(kubectl -n argocd get secret argocd-initial-admin-secret -o jsonpath="{.data.password}" | base64 --decode)
print_status "${GREEN}" "\u2714 Initial ArgoCD password: $argocd_password"

# Ensure KUBECONFIG is correctly set for k3s
if [ "$PLATFORM" == "k3s" ]; then
  # Update KUBECONFIG environment variable for K3s
  echo "export KUBECONFIG=/etc/rancher/k3s/k3s.yaml" >> ~/.bashrc
  export KUBECONFIG=/etc/rancher/k3s/k3s.yaml
  print_status "${GREEN}" "\u2714 KUBECONFIG set to /etc/rancher/k3s/k3s.yaml"
  print_status "${YELLOW}" "Please manually run 'source ~/.bashrc' or start a new shell session to apply the changes."
fi

# Verify that Kubernetes nodes are accessible
kubectl get nodes

# Print deployment success message
print_status "${GREEN}" "\ud83c\udf89 Deployment completed successfully!"
print_status "${YELLOW}" "To access the ArgoCD UI, run the following command in another terminal:"
echo "kubectl port-forward svc/argocd-server -n argocd 8080:443"
