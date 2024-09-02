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

check_pods_ready() {
  local namespace=$1
  local timeout=${2:-300} 
  local start_time=$(date +%s)

  if ! kubectl get namespace "$namespace" &>/dev/null; then
    print_status "${RED}" "❌ Namespace $namespace does not exist."
    return 1
  fi

  while true; do
    local current_time=$(date +%s)
    local elapsed_time=$((current_time - start_time))

    if [ $elapsed_time -ge $timeout ]; then
      print_status "${RED}" "❌ Timeout reached. Some pods in $namespace are still not ready."
      return 1
    fi

    local pod_status=$(kubectl get pods -n "$namespace" -o json)
    local total_pods=$(echo "$pod_status" | jq '.items | length')

    if [ "$total_pods" -eq 0 ]; then
      print_status "${YELLOW}" "⏳ No pods found in $namespace. Waiting for pods to be created... (${elapsed_time}s elapsed)"
      sleep 10
      continue
    fi

    local running_pods=$(echo "$pod_status" | jq '[.items[] | select(.status.phase == "Running")] | length')
    local pending_pods=$(echo "$pod_status" | jq '[.items[] | select(.status.phase == "Pending")] | length')
    local failed_pods=$(echo "$pod_status" | jq '[.items[] | select(.status.phase == "Failed")] | length')

    if [ "$running_pods" -eq "$total_pods" ]; then
      local not_ready_pods=$(echo "$pod_status" | jq '[.items[] | select(.status.conditions[] | select(.type == "Ready" and .status == "False"))] | length')
      if [ "$not_ready_pods" -eq 0 ]; then
        print_status "${GREEN}" "✔ All $total_pods pods in the $namespace namespace are running and ready."
        return 0
      fi
    fi

    print_status "${YELLOW}" "⏳ Waiting for pods in $namespace to be ready... (${elapsed_time}s elapsed)"
    print_status "${YELLOW}" "   Total: $total_pods, Running: $running_pods, Pending: $pending_pods, Failed: $failed_pods"
    
    if [ "$failed_pods" -gt 0 ]; then
      print_status "${RED}" "   Warning: $failed_pods pods have failed. Check the logs for more information."
    fi

    sleep 10
  done
}

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

install_k3s() {
  print_status "${YELLOW}" "⏳ Installing K3s..."
  curl -sfL https://get.k3s.io | sh -s - --write-kubeconfig-mode 644
  if [ $? -ne 0 ]; then
    print_status "${RED}" "❌ Failed to install K3s."
    exit_gracefully
  fi
  print_status "${GREEN}" "✔ K3s installed successfully."
}

start_k3s() {
  print_status "${YELLOW}" "⏳ Starting K3s..."
  sudo systemctl start k3s
  if [ $? -ne 0 ]; then
    print_status "${RED}" "❌ Failed to start K3s."
    exit_gracefully
  fi
  export KUBECONFIG="/etc/rancher/k3s/k3s.yaml"
  print_status "${GREEN}" "✔ KUBECONFIG set to use K3s kubeconfig."
}

install_kubectl() {
  print_status "${YELLOW}" "⏳ Installing kubectl..."
  curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl"
  sudo install -o root -g root -m 0755 kubectl /usr/local/bin/kubectl
  if [ $? -ne 0 ]; then
    print_status "${RED}" "❌ Failed to install kubectl."
    exit_gracefully
  fi
  print_status "${GREEN}" "✔ kubectl installed successfully."
}

install_helm() {
  print_status "${YELLOW}" "⏳ Installing Helm..."
  curl https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash
  if [ $? -ne 0 ]; then
    print_status "${RED}" "❌ Failed to install Helm."
    exit_gracefully
  fi
  print_status "${GREEN}" "✔ Helm installed successfully."
}

display_summary() {
  print_status "${GREEN}" "\n📊 Data Lakehouse Deployment Summary:"
  echo "----------------------------------------"
  
  local resources=$(kubectl get all -n data-lakehouse -o json)
  local total_resources=$(echo "$resources" | jq '.items | length')
  
  if [ "$total_resources" -eq 0 ]; then
    print_status "${YELLOW}" "⏳ No resources found in the data-lakehouse namespace. Waiting for resources to be created..."
  else
    kubectl rollout status deployment -n data-lakehouse
  fi
  
  echo "----------------------------------------"
  print_status "${YELLOW}" "To access these services, you may need to set up port-forwarding or use a LoadBalancer."
}

print_status "${GREEN}" "🚀 Starting Data Lakehouse Setup"

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
      print_status "${RED}" "❌ Failed to start Minikube."
      exit_gracefully
    fi
    print_status "${GREEN}" "✔ Minikube started successfully."
    ;;

  "k3s")
    print_status "${YELLOW}" "Setting up K3s..."

    if ! command -v k3s &> /dev/null; then
      install_k3s
    fi

    start_k3s
    ;;

  "current")
    current_context=$(kubectl config current-context 2>/dev/null)
    if [ -z "$current_context" ]; then
      print_status "${RED}" "❌ No current Kubernetes context detected. Please set up a cluster or specify --platform as minikube or k3s."
      exit_gracefully
    else
      print_status "${GREEN}" "✔ Using the current Kubernetes context: $current_context"
    fi
    ;;

  *)
    print_status "${RED}" "❌ Invalid platform specified. Use --platform with current, minikube, or k3s."
    exit_gracefully
    ;;
esac

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
kubectl apply -n argocd -f https://raw.githubusercontent.com/joshyorko/open-datalakehouse/longhorn_setup/app-of-apps.yaml

# Monitor the deployment
print_status "${YELLOW}" "⏳ Monitoring the deployment..."
kubectl get applications -n argocd

# Get the initial ArgoCD password
print_status "${YELLOW}" "⏳ Getting the initial ArgoCD password..."
argocd_password=$(kubectl -n argocd get secret argocd-initial-admin-secret -o jsonpath="{.data.password}" | base64 --decode)

print_status "${GREEN}" "✔ Initial ArgoCD password: $argocd_password"

# Ensure KUBECONFIG is correctly set
if [ "$PLATFORM" == "k3s" ]; then
  echo "export KUBECONFIG=/etc/rancher/k3s/k3s.yaml" >> ~/.bashrc
  
  export KUBECONFIG=/etc/rancher/k3s/k3s.yaml
  print_status "${GREEN}" "✔ KUBECONFIG set to /etc/rancher/k3s/k3s.yaml"
fi

# Verify KUBECONFIG setup
kubectl get nodes

print_status "${GREEN}" "🎉 Deployment completed successfully!"
print_status "${YELLOW}" "To access the ArgoCD UI, run the following command in another terminal:"
echo "kubectl port-forward svc/argocd-server -n argocd 8080:443"
source ~/.bashrc
