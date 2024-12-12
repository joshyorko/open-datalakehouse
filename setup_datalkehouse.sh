
# Script version
VERSION=\"1.0.0\"

# Color definitions
RED='\\033[0;31m'
GREEN='\\033[0;32m'
YELLOW='\\033[1;33m'
NC='\\033[0m' # No Color

# Exit immediately if a command exits with a non-zero status
set -e
# Catch errors in pipelines
set -o pipefail
# Treat unset variables as errors
set -u

# Required versions
REQUIRED_MEMORY=\"4096\"  # 4GB minimum
REQUIRED_CPU=\"2\"        # 2 cores minimum

# Function to print status messages with colors and logging
print_status() {
    local color=$1
    local message=$2
    local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    echo -e \"${color}[${timestamp}] ${message}${NC}\" | tee -a setup.log
}

# Function to log errors and exit gracefully
exit_gracefully() {
    local message=${1:-\"An error occurred\"}
    print_status \"${RED}\" \"ERROR: ${message}\"
    print_status \"${RED}\" \"Check setup.log for details\"
    exit 1
}

# Trap errors and call exit_gracefully
trap 'exit_gracefully \"Command failed at line $LINENO\"' ERR

# Function to check system requirements
check_requirements() {
    print_status \"${YELLOW}\" \"Checking system requirements...\"
    
    # Check available memory
    local available_memory=$(free -m | awk '/Mem:/ {print $2}')
    if [ \"${available_memory}\" -lt \"${REQUIRED_MEMORY}\" ]; then
        exit_gracefully \"Insufficient memory. Required: ${REQUIRED_MEMORY}MB, Available: ${available_memory}MB\"
    fi

    # Check available CPU cores
    local available_cpu=$(nproc)
    if [ \"${available_cpu}\" -lt \"${REQUIRED_CPU}\" ]; then
        exit_gracefully \"Insufficient CPU cores. Required: ${REQUIRED_CPU}, Available: ${available_cpu}\"
    fi

    # Check for required commands
    local required_commands=(\"curl\" \"wget\" \"sudo\")
    for cmd in \"${required_commands[@]}\"; do
        if ! command -v \"${cmd}\" &> /dev/null; then
            exit_gracefully \"Required command not found: ${cmd}\"
        fi
    fi

    print_status \"${GREEN}\" \"System requirements met ✓\"
}

# Function to handle tool version checking
check_tool_version() {
    local tool_name=$1
    local min_version=$2
    local current_version

    case \"${tool_name}\" in
        \"kubectl\")
            current_version=$(kubectl version --client -o json | jq -r '.clientVersion.gitVersion' | tr -d 'v')
            ;;
        \"helm\")
            current_version=$(helm version --template='{{.Version}}' | tr -d 'v')
            ;;
        *)
            print_status \"${YELLOW}\" \"No version check implemented for ${tool_name}\"
            return 0
            ;;
    esac

    if ! command -v \"semver\" &> /dev/null; then
        npm install -g semver &> /dev/null
    fi

    if ! semver -r \">=${min_version}\" \"${current_version}\" &> /dev/null; then
        exit_gracefully \"${tool_name} version ${current_version} is less than required version ${min_version}\"
    fi
}

# Enhanced wait_for_namespace_resources function with better error handling
wait_for_namespace_resources() {
    local namespace=$1
    local timeout=${2:-300}
    local interval=${3:-5}
    local elapsed=0

    print_status \"${YELLOW}\" \"Waiting for resources in namespace ${namespace}...\"

    while [ \"${elapsed}\" -lt \"${timeout}\" ]; do
        if ! kubectl get namespace \"${namespace}\" &>/dev/null; then
            sleep \"${interval}\"
            elapsed=$((elapsed + interval))
            continue
        fi

        local unready_pods=$(kubectl get pods -n \"${namespace}\" --no-headers 2>/dev/null | grep -v \"Running\\|Completed\" || true)
        local unready_deployments=$(kubectl get deployments -n \"${namespace}\" --no-headers 2>/dev/null | awk '$5 != $4' || true)

        if [ -z \"${unready_pods}\" ] && [ -z \"${unready_deployments}\" ]; then
            print_status \"${GREEN}\" \"All resources in ${namespace} are ready ✓\"
            return 0
        fi

        sleep \"${interval}\"
        elapsed=$((elapsed + interval))
    done

    print_status \"${RED}\" \"Timeout waiting for resources in ${namespace}\"
    kubectl get pods,deployments -n \"${namespace}\"
    return 1
}

# Enhanced install_tool function with version checking and backup
install_tool() {
    local tool_name=$1
    local install_command=$2
    local min_version=${3:-\"0.0.0\"}
    local backup_dir=\"/tmp/tool_backups\"

    # Create backup directory if it doesn't exist
    mkdir -p \"${backup_dir}\"

    if command -v \"${tool_name}\" &> /dev/null; then
        # Backup existing installation
        if [ -f \"$(command -v \"${tool_name}\")\" ]; then
            cp \"$(command -v \"${tool_name}\")\" \"${backup_dir}/${tool_name}.backup\"
        fi
    fi

    print_status \"${YELLOW}\" \"Installing ${tool_name}...\"
    if ! eval \"${install_command}\"; then
        if [ -f \"${backup_dir}/${tool_name}.backup\" ]; then
            print_status \"${YELLOW}\" \"Restoring ${tool_name} from backup...\"
            sudo cp \"${backup_dir}/${tool_name}.backup\" \"$(dirname \"$(command -v \"${tool_name}\")\")\"
        fi
        exit_gracefully \"Failed to install ${tool_name}\"
    fi

    # Verify installation and version
    if ! command -v \"${tool_name}\" &> /dev/null; then
        exit_gracefully \"${tool_name} installation verification failed\"
    fi

    check_tool_version \"${tool_name}\" \"${min_version}\"
}

# Enhanced setup_platform function with resource checks
setup_platform() {
    local platform=$1
    local memory_requirement=${2:-\"2048\"}  # Default 2GB
    
    case \"${platform}\" in
        \"minikube\")
            install_tool \"minikube\" \\
                \"curl -LO https://storage.googleapis.com/minikube/releases/latest/minikube-linux-amd64 && \\
                sudo install minikube-linux-amd64 /usr/local/bin/minikube\" \\
                \"1.30.0\"  # Minimum minikube version

            print_status \"${YELLOW}\" \"Starting Minikube with high availability...\"
            minikube start --memory \"${memory_requirement}\" \\
                          --cpus 2 \\
                          --disk-size 20g \\
                          --kubernetes-version=stable \\
                          --addons=dashboard,metrics-server
            ;;

        \"k3s\")
            if [ ! -f /etc/systemd/system/k3s.service ]; then
                curl -sfL https://get.k3s.io | sh -s - \\
                    --write-kubeconfig-mode 644 \\
                    --disable traefik \\  # We'll use our own ingress
                    --disable servicelb  # We'll use metallb
            fi

            # Wait for k3s to be ready
            systemctl is-active --quiet k3s || systemctl start k3s
            sleep 10  # Give k3s time to initialize
            ;;

        \"current\")
            if ! kubectl config current-context &>/dev/null; then
                exit_gracefully \"No current Kubernetes context detected\"
            fi
            ;;

        *)
            exit_gracefully \"Invalid platform: ${platform}. Use 'current', 'minikube', or 'k3s'\"
            ;;
    esac
}

# Function to validate ArgoCD installation
validate_argocd() {
    print_status \"${YELLOW}\" \"Validating ArgoCD installation...\"
    
    # Check if ArgoCD CLI is installed
    if ! command -v argocd &> /dev/null; then
        print_status \"${YELLOW}\" \"Installing ArgoCD CLI...\"
        curl -sSL -o argocd https://github.com/argoproj/argo-cd/releases/latest/download/argocd-linux-amd64
        sudo install -m 555 argocd /usr/local/bin/argocd
        rm argocd
    fi

    # Verify ArgoCD server is responsive
    local retries=0
    local max_retries=30
    while [ \"${retries}\" -lt \"${max_retries}\" ]; do
        if kubectl port-forward svc/argocd-server -n argocd 8080:443 &>/dev/null & then
            sleep 5
            if curl -sk https://localhost:8080 &>/dev/null; then
                print_status \"${GREEN}\" \"ArgoCD server is responsive ✓\"
                pkill -f \"port-forward.*8080:443\"
                return 0
            fi
            pkill -f \"port-forward.*8080:443\"
        fi
        retries=$((retries + 1))
        sleep 2
    done

    exit_gracefully \"ArgoCD server validation failed\"
}

# Function to set up monitoring
setup_monitoring() {
    print_status \"${YELLOW}\" \"Setting up monitoring stack...\"

    # Add Prometheus operator helm repo
    helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
    helm repo update

    # Install Prometheus stack
    helm upgrade --install prometheus prometheus-community/kube-prometheus-stack \\
        --namespace monitoring \\
        --create-namespace \\
        --set grafana.enabled=true \\
        --set prometheus.prometheusSpec.serviceMonitorSelectorNilUsesHelmValues=false

    wait_for_namespace_resources \"monitoring\" 600
}

# Main script execution
main() {
    print_status \"${GREEN}\" \"🚀 Starting Data Lakehouse Setup v${VERSION}\"
    
    # Parse command line arguments
    local platform=\"current\"
    local skip_monitoring=false
    local memory_requirement=\"2048\"

    while [[ $# -gt 0 ]]; do
        case $1 in
            --platform)
                platform=\"$2\"
                shift 2
                ;;
            --memory)
                memory_requirement=\"$2\"
                shift 2
                ;;
            --skip-monitoring)
                skip_monitoring=true
                shift
                ;;
            --help)
                print_status \"${GREEN}\" \"Usage: $0 [--platform <current|minikube|k3s>] [--memory <MB>] [--skip-monitoring]\"
                exit 0
                ;;
            *)
                exit_gracefully \"Unknown parameter: $1\"
                ;;
        esac
    done

    # Initialize log file
    echo \"=== Data Lakehouse Setup Log $(date) ===\" > setup.log

    # Run setup steps
    check_requirements
    setup_platform \"${platform}\" \"${memory_requirement}\"
    
    # Install required tools with version checks
    install_tool \"kubectl\" \\
        \"curl -LO https://dl.k8s.io/release/\\$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl && \\
        sudo install -o root -g root -m 0755 kubectl /usr/local/bin/kubectl\" \\
        \"1.24.0\"

    install_tool \"helm\" \\
        \"curl https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash\" \\
        \"3.10.0\"

    # Set up ArgoCD
    if ! kubectl get namespace argocd &>/dev/null; then
        kubectl create namespace argocd
    fi

    print_status \"${YELLOW}\" \"Installing ArgoCD...\"
    kubectl apply -n argocd -f https://raw.githubusercontent.com/argoproj/argo-cd/stable/manifests/install.yaml
    
    wait_for_namespace_resources \"argocd\" 600
    validate_argocd

    # Deploy the application of applications
    print_status \"${YELLOW}\" \"Deploying the ArgoCD application of applications...\"
    kubectl apply -n argocd -f https://raw.githubusercontent.com/joshyorko/open-datalakehouse/main/app-of-apps.yaml

    # Set up monitoring if not skipped
    if [ \"${skip_monitoring}\" = false ]; then
        setup_monitoring
    fi

    # Get ArgoCD initial password
    local argocd_password
    argocd_password=$(kubectl -n argocd get secret argocd-initial-admin-secret -o jsonpath=\"{.data.password}\" | base64 --decode)
    
    # Print success message and credentials
    print_status \"${GREEN}\" \"🎉 Deployment completed successfully!\"
    print_status \"${YELLOW}\" \"ArgoCD Credentials:\"
    echo \"Username: admin\"
    echo \"Password: ${argocd_password}\"
    print_status \"${YELLOW}\" \"To access the ArgoCD UI, run:\"
    echo \"kubectl port-forward svc/argocd-server -n argocd 8080:443\"
    print_status \"${YELLOW}\" \"Then visit: https://localhost:8080\"
}

# Execute main function with all arguments
main \"$@\"`
