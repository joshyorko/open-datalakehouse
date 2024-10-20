#!/bin/bash

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

print_status() {
  local color=$1
  local message=$2
  echo -e "${color}${message}${NC}"
}

exit_gracefully() {
  print_status "${RED}" "Exiting gracefully..."
  exit 1
}

# Improved to wait for all resources (pods, deployments, etc.) to be in ready state
wait_for_namespace_resources() {
  local namespace=$1
  local timeout=${2:-300}
  local start_time=$(date +%s)

  if ! kubectl get namespace "$namespace" &>/dev/null; then
    print_status "${RED}" "\u274c Namespace $namespace does not exist."
    return 1
  fi

  if ! command -v jq &> /dev/null; then
    print_status "${RED}" "\u274c 'jq' is not installed. Please install 'jq' to proceed."
    exit_gracefully
  fi

  print_status "${YELLOW}" "\u231b Waiting for resources in the namespace $namespace to be ready..."

  while true; do
    local current_time=$(date +%s)
    local elapsed_time=$((current_time - start_time))

    if [ $elapsed_time -ge $timeout ]; then
      print_status "${RED}" "\u274c Timeout reached. Some resources in $namespace are still not ready."
      return 1
    fi

    # Check for Pods readiness
    local pods_ready=$(kubectl get pods -n "$namespace" -o json | jq -r '.items[] | .status.conditions[] | select(.type == "Ready") | .status' | grep -c "True")
    local total_pods=$(kubectl get pods -n "$namespace" -o json | jq '.items | length')
    
    # Check for Deployments readiness
    local deployments_ready=$(kubectl get deployments -n "$namespace" -o json | jq '[.items[] | select(.status.readyReplicas == .status.replicas and .status.replicas > 0)] | length')
    local total_deployments=$(kubectl get deployments -n "$namespace" -o json | jq '.items | length')

    if [ "$pods_ready" -eq "$total_pods" ] && [ "$deployments_ready" -eq "$total_deployments" ]; then
      print_status "${GREEN}" "\u2714 All $total_pods pods and $total_deployments deployments in $namespace are ready."
      return 0
    fi

    print_status "${YELLOW}" "\u231b Waiting for resources in $namespace to be ready... (${elapsed_time}s elapsed)"
    print_status "${YELLOW}" "   Pods: $pods_ready/$total_pods, Deployments: $deployments_ready/$total_deployments"
    sleep 10
  done
}

install_minikube() {
  print_status "${YELLOW}" "\u231b Minikube is not installed. Installing Minikube..."
  curl -LO https://storage.googleapis.com/minikube/releases/latest/minikube-linux-amd64
  sudo install minikube-linux-amd64 /usr/local/bin/minikube
  if [ $? -ne 0 ]; then
    print_status "${RED}" "\u274c Failed to install Minikube."
    exit_gracefully
  fi
  print_status "${GREEN}" "\u2714 Minikube installed successfully."
}

install_k3s() {
  print_status "${YELLOW}" "\u231b Installing K3s..."
  curl -sfL https://get.k3s.io | sh -s - --write-kubeconfig-mode 644
  if [ $? -ne 0 ]; then
    print_status "${RED}" "\u274c Failed to install K3s."
    exit_gracefully
  fi
  print_status "${GREEN}" "\u2714 K3s installed successfully."
}

install_kubectl() {
  print_status "${YELLOW}" "\u231b Installing kubectl..."
  curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl"
  sudo install -o root -g root -m 0755 kubectl /usr/local/bin/kubectl
  if [ $? -ne 0 ]; then
    print_status "${RED}" "\u274c Failed to install kubectl."
    exit_gracefully
  fi
  print_status "${GREEN}" "\u2714 kubectl installed successfully."
}

install_helm() {
  print_status "${YELLOW}" "\u231b Installing Helm..."
  curl https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash
  if [ $? -ne 0 ]; then
    print_status "${RED}" "\u274c Failed to install Helm."
    exit_gracefully
  fi
  print_status "${GREEN}" "\u2714 Helm installed successfully."
}

display_summary() {
  print_status "${GREEN}" "\n\ud83d\udcca Data Lakehouse Deployment Summary:"
  echo "----------------------------------------"
  
  local resources=$(kubectl get all -n data-lakehouse -o json)
  local total_resources=$(echo "$resources" | jq '.items | length')
  
  if [ "$total_resources" -eq 0 ]; then
    print_status "${YELLOW}" "\u231b No resources found in the data-lakehouse namespace. Waiting for resources to be created..."
  else
    kubectl rollout status deployment -n data-lakehouse
  fi
  
  echo "----------------------------------------"
  print_status "${YELLOW}" "To access these services, you may need to set up port-forwarding or use a LoadBalancer."
}

print_status "${GREEN}" "\ud83d\ude80 Starting Data Lakehouse Setup"

# Check for kubectl and install if not present
if ! command -v kubectl &> /dev/null; then
  install_kubectl
fi

# Check for helm and install if not present
if ! command -v helm &> /dev/null; then
  install_helm
fi

PLATFORM="current"

# Parse the platform flag
while [[ "$#" -gt 0 ]]; do
  case $1 in
    --platform)
      PLATFORM="$2"
      shift
      ;;
    *)
      print_status "${RED}" "Unknown parameter passed: $1"
      exit_gracefully
      ;;
  esac
  shift
done

case $PLATFORM in
  "minikube")
    print_status "${YELLOW}" "Setting up Minikube..."

    if ! command -v minikube &> /dev/null; then
      install_minikube
    fi

    print_status "${YELLOW}" "Starting Minikube with high availability..."
    minikube start --ha --driver=docker --container-runtime=containerd --memory=8192 --cpus=4
    
    if [ $? -ne 0 ]; then
      print_status "${RED}" "\u274c Failed to start Minikube."
      exit_gracefully
    fi
    print_status "${GREEN}" "\u2714 Minikube started successfully."
    ;;

  "k3s")
    print_status "${YELLOW}" "Setting up K3s..."

    if ! command -v k3s &> /dev/null; then
      install_k3s
    fi

    print_status "${YELLOW}" "Starting K3s..."
    if systemctl is-active --quiet k3s; then
      print_status "${GREEN}" "\u2714 K3s service is already running."
    else
      sudo systemctl start k3s
      if [ $? -ne 0 ]; then
        print_status "${RED}" "\u274c Failed to start K3s."
        exit_gracefully
      fi
      print_status "${GREEN}" "\u2714 K3s started successfully."
    fi
    ;;

  "current")
    current_context=$(kubectl config current-context 2>/dev/null)
    if [ -z "$current_context" ]; then
      print_status "${RED}" "\u274c No current Kubernetes context detected. Please set up a cluster or specify --platform as minikube or k3s."
      exit_gracefully
    else
      print_status "${GREEN}" "\u2714 Using the current Kubernetes context: $current_context"
    fi
    ;;

  *)
    print_status "${RED}" "\u274c Invalid platform specified. Use --platform with current, minikube, or k3s."
    exit_gracefully
    ;;
esac

# Set up ArgoCD
print_status "${YELLOW}" "\u231b Setting up ArgoCD..."
kubectl create ns argocd
kubectl apply -n argocd -f https://raw.githubusercontent.com/argoproj/argo-cd/stable/manifests/install.yaml
wait_for_namespace_resources "argocd"

# Deploy the ArgoCD application of applications
print_status "${YELLOW}" "\u231b Deploying the ArgoCD application of applications..."
kubectl apply -n argocd -f https://raw.githubusercontent.com/joshyorko/open-datalakehouse/main/app-of-apps.yaml

# Monitor the deployment
print_status "${YELLOW}" "\u231b Monitoring the deployment..."
kubectl get applications -n argocd

# Get the initial ArgoCD password
print_status "${YELLOW}" "\u231b Getting the initial ArgoCD password..."
argocd_password=$(kubectl -n argocd get secret argocd-initial-admin-secret -o jsonpath="{.data.password}" | base64 --decode)

print_status "${GREEN}" "\u2714 Initial ArgoCD password: $argocd_password"

# Ensure KUBECONFIG is correctly set
if [ "$PLATFORM" == "k3s" ]; then
  echo "export KUBECONFIG=/etc/rancher/k3s/k3s.yaml" >> ~/.bashrc
  
  export KUBECONFIG=/etc/rancher/k3s/k3s.yaml
  print_status "${GREEN}" "\u2714 KUBECONFIG set to /etc/rancher/k3s/k3s.yaml"
  print_status "${YELLOW}" "Please manually run 'source ~/.bashrc' or start a new shell session to apply the changes."
fi

# Verify KUBECONFIG setup
kubectl get nodes

print_status "${GREEN}" "\ud83c\udf89 Deployment completed successfully!"
print_status "${YELLOW}" "To access the ArgoCD UI, run the following command in another terminal:"
echo "kubectl port-forward svc/argocd-server -n argocd 8080:443"
