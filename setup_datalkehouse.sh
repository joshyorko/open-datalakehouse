#!/bin/bash
# Script version
VERSION="1.0.0"

# Color definitions
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Exit immediately if a command exits with a non-zero status
set -e
# Catch errors in pipelines
set -o pipefail
# Treat unset variables as errors
set -u

# Required versions
REQUIRED_MEMORY="1024"  # 4GB minimum
REQUIRED_CPU="2"        # 2 cores minimum

# Progress bar variables
PROGRESS_BAR_WIDTH=50
PROGRESS_BAR_STEP=0

###############################################################################
# VERSION COMPARISON FUNCTIONS
###############################################################################

# Returns 0 if $1 >= $2 in "x.y.z" version format, else returns 1
version_gte() {
    local v1="$1"
    local v2="$2"
    # Natural sort the two versions, then check if the first line is v2
    if [[ "$(printf '%s\n%s\n' "$v1" "$v2" | sort -V | head -n1)" == "$v2" ]]; then
        # If v2 is <= v1, then v1 >= v2
        return 0
    else
        return 1
    fi
}

###############################################################################
# PRINTING AND ERROR-TRAPPING
###############################################################################
print_status() {
    local color="$1"
    local message="$2"
    local timestamp
    timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    echo -e "${color}[${timestamp}] ${message}${NC}" | tee -a setup.log
}

exit_gracefully() {
    local message=${1:-"An error occurred"}
    print_status "${RED}" "ERROR: ${message}"
    print_status "${RED}" "Check setup.log for details"
    exit 1
}

trap 'exit_gracefully "Command failed at line $LINENO"' ERR

###############################################################################
# REQUIREMENTS CHECK
###############################################################################
check_requirements() {
    print_status "${YELLOW}" "Checking system requirements..."

    # Check available memory
    local available_memory
    available_memory=$(free -m | awk '/Mem:/ {print $2}')
    if [ "${available_memory}" -lt "${REQUIRED_MEMORY}" ]; then
        exit_gracefully "Insufficient memory. Required: ${REQUIRED_MEMORY}MB, Available: ${available_memory}MB"
    fi

    # Check available CPU cores
    local available_cpu
    available_cpu=$(nproc)
    if [ "${available_cpu}" -lt "${REQUIRED_CPU}" ]; then
        exit_gracefully "Insufficient CPU cores. Required: ${REQUIRED_CPU}, Available: ${available_cpu}"
    fi

    # Check for required commands
    local required_commands=("curl" "wget" "sudo")
    for cmd in "${required_commands[@]}"; do
        if ! command -v "${cmd}" &> /dev/null; then
            exit_gracefully "Required command not found: ${cmd}"
        fi
    done

    print_status "${GREEN}" "System requirements met ✓"
}

###############################################################################
# TOOL VERSION CHECK HELPERS
###############################################################################
check_tool_version() {
    local tool_name="$1"
    local min_version="$2"
    local current_version=""

    case "${tool_name}" in
        "kubectl")
            current_version=$(kubectl version --client -o json | jq -r '.clientVersion.gitVersion' | tr -d 'v')
            ;;
        "helm")
            current_version=$(helm version --template='{{.Version}}' | tr -d 'v')
            ;;
        *)
            print_status "${YELLOW}" "No version check implemented for ${tool_name}"
            return 0
            ;;
    esac

    # If current_version < min_version, fail
    if ! version_gte "${current_version}" "${min_version}"; then
        exit_gracefully "${tool_name} version ${current_version} is less than required version ${min_version}"
    fi
}

check_tool_version_silent() {
    local tool_name="$1"
    local min_version="$2"
    local current_version=""

    case "${tool_name}" in
        "kubectl")
            current_version=$(kubectl version --client -o json | jq -r '.clientVersion.gitVersion' | tr -d 'v')
            ;;
        "helm")
            current_version=$(helm version --template='{{.Version}}' | tr -d 'v')
            ;;
        *)
            return 0
            ;;
    esac

    # Returns 0 if current_version >= min_version, else returns 1
    if version_gte "${current_version}" "${min_version}"; then
        return 0
    else
        return 1
    fi
}

###############################################################################
# WAIT FOR NAMESPACE RESOURCES
###############################################################################
wait_for_namespace_resources() {
    local namespace="$1"
    local timeout="${2:-300}"
    local interval="${3:-5}"
    local elapsed=0

    print_status "${YELLOW}" "Waiting for resources in namespace ${namespace}..."

    while [ "${elapsed}" -lt "${timeout}" ]; do
        if ! kubectl get namespace "${namespace}" &>/dev/null; then
            sleep "${interval}"
            elapsed=$((elapsed + interval))
            continue
        fi

        local unready_pods
        unready_pods=$(kubectl get pods -n "${namespace}" --no-headers 2>/dev/null | grep -v "Running\|Completed" || true)

        local unready_deployments
        unready_deployments=$(kubectl get deployments -n "${namespace}" --no-headers 2>/dev/null | awk '{
            # Columns: NAME READY UP-TO-DATE AVAILABLE AGE
            split($2, a, "/")
            # If desired != ready or up-to-date != available, print
            if (a[1] != a[2] || $3 != $4) print
        }' || true)

        if [ -n "${unready_pods}" ] || [ -n "${unready_deployments}" ]; then
            print_status "${YELLOW}" "Still waiting for these resources in ${namespace}:"
            echo "${unready_pods}"
            echo "${unready_deployments}"
        fi

        if [ -z "${unready_pods}" ] && [ -z "${unready_deployments}" ]; then
            print_status "${GREEN}" "All resources in ${namespace} are ready ✓"
            return 0
        fi

        sleep "${interval}"
        elapsed=$((elapsed + interval))
    done

    print_status "${RED}" "Timeout waiting for resources in ${namespace}"
    kubectl get pods,deployments -n "${namespace}"
    return 1
}

###############################################################################
# INSTALL TOOL (WITH BACKUP)
###############################################################################
install_tool() {
    local tool_name="$1"
    local install_command="$2"
    local min_version="${3:-"0.0.0"}"
    local backup_dir="/tmp/tool_backups"

    # Create backup directory if it doesn't exist
    mkdir -p "${backup_dir}"

    # Check if already installed + meets version
    if command -v "${tool_name}" &> /dev/null; then
        if check_tool_version_silent "${tool_name}" "${min_version}"; then
            print_status "${GREEN}" "${tool_name} is already installed and meets version requirement (>=${min_version}). Skipping installation."
            return 0
        else
            print_status "${YELLOW}" "${tool_name} is installed but doesn't meet version requirement."
        fi

        # Backup existing installation
        if [ -f "$(command -v "${tool_name}")" ]; then
            cp "$(command -v "${tool_name}")" "${backup_dir}/${tool_name}.backup"
        fi
    fi

    print_status "${YELLOW}" "Installing ${tool_name}..."
    if ! eval "${install_command}"; then
        # If install fails, restore backup
        if [ -f "${backup_dir}/${tool_name}.backup" ]; then
            print_status "${YELLOW}" "Restoring ${tool_name} from backup..."
            sudo cp "${backup_dir}/${tool_name}.backup" "$(dirname "$(command -v "${tool_name}")")"
        fi
        exit_gracefully "Failed to install ${tool_name}"
    fi

    # Verify installation and version
    if ! command -v "${tool_name}" &> /dev/null; then
        exit_gracefully "${tool_name} installation verification failed"
    fi

    check_tool_version "${tool_name}" "${min_version}"
}

###############################################################################
# SETUP PLATFORM
###############################################################################
setup_platform() {
    local platform="$1"
    local memory_requirement="${2:-"2048"}"  # Default 2GB

    case "${platform}" in
        "minikube")
            install_tool "minikube" \
                "curl -LO https://storage.googleapis.com/minikube/releases/latest/minikube-linux-amd64 && \
                sudo install minikube-linux-amd64 /usr/local/bin/minikube" \
                "1.30.0"

            print_status "${YELLOW}" "Starting Minikube with high availability..."
            minikube start --memory "${memory_requirement}" \
                           --cpus 2 \
                           --disk-size 20g \
                           --kubernetes-version=stable \
                           --addons=dashboard
            ;;

        "k3s")
            if [ ! -f /etc/systemd/system/k3s.service ]; then
                curl -sfL https://get.k3s.io | sh -s - \
                    --write-kubeconfig-mode 644 \
                    --disable traefik \
                    --disable servicelb
            fi

            # Wait for k3s to be ready
            systemctl is-active --quiet k3s || systemctl start k3s
            sleep 10  # Give k3s time to initialize
            ;;

        "current")
            # Just assume user is already connected to a cluster
            if ! kubectl config current-context &>/dev/null; then
                exit_gracefully "No current Kubernetes context detected"
            fi
            ;;

        *)
            exit_gracefully "Invalid platform: ${platform}. Use 'current', 'minikube', or 'k3s'"
            ;;
    esac
}

###############################################################################
# VALIDATE ARGOCD
###############################################################################
validate_argocd() {
    print_status "${YELLOW}" "Validating ArgoCD installation..."

    # Check if ArgoCD CLI is installed
    if ! command -v argocd &> /dev/null; then
        print_status "${YELLOW}" "Installing ArgoCD CLI..."
        curl -sSL -o argocd https://github.com/argoproj/argo-cd/releases/latest/download/argocd-linux-amd64
        sudo install -m 555 argocd /usr/local/bin/argocd
        rm argocd
    fi

    # Verify ArgoCD server is responsive
    local retries=0
    local max_retries=30
    while [ "${retries}" -lt "${max_retries}" ]; do
        if kubectl port-forward svc/argocd-server -n argocd 8080:443 &>/dev/null & then
            sleep 5
            if curl -sk https://localhost:8080 &>/dev/null; then
                print_status "${GREEN}" "ArgoCD server is responsive ✓"
                pkill -f "port-forward.*8080:443"
                return 0
            fi
            pkill -f "port-forward.*8080:443"
        fi
        retries=$((retries + 1))
        sleep 2
    done

    exit_gracefully "ArgoCD server validation failed"
}

###############################################################################
# PROGRESS BAR
###############################################################################
update_progress_bar() {
    local progress=$1
    local completed=$((progress * PROGRESS_BAR_WIDTH / 100))
    local remaining=$((PROGRESS_BAR_WIDTH - completed))

    printf "\rProgress: ["
    for ((i = 0; i < completed; i++)); do
        printf "#"
    done
    for ((i = 0; i < remaining; i++)); do
        printf "-"
    done
    printf "] %d%%" "${progress}"
}

###############################################################################
# CLEANUP AND ROLLBACK
###############################################################################
cleanup_and_rollback() {
    print_status "${YELLOW}" "Performing cleanup and rollback..."

    # Delete ArgoCD namespace
    if kubectl get namespace argocd &>/dev/null; then
        kubectl delete namespace argocd
    fi

    # Delete data-lakehouse namespace
    if kubectl get namespace data-lakehouse &>/dev/null; then
        kubectl delete namespace data-lakehouse
    fi

    print_status "${GREEN}" "Cleanup and rollback completed ✓"
}

###############################################################################
# VALIDATE KUBERNETES CLUSTER CONFIGURATION
###############################################################################
validate_k8s_cluster_config() {
    print_status "${YELLOW}" "Validating Kubernetes cluster configuration..."

    # Check if the cluster is accessible
    if ! kubectl cluster-info &>/dev/null; then
        exit_gracefully "Kubernetes cluster is not accessible"
    fi

    # Check if the cluster has sufficient resources
    local available_memory
    available_memory=$(kubectl get nodes -o jsonpath='{.items[*].status.allocatable.memory}' | awk '{sum += $1} END {print sum}')
    if [ "${available_memory}" -lt "${REQUIRED_MEMORY}" ]; then
        exit_gracefully "Insufficient cluster memory. Required: ${REQUIRED_MEMORY}MB, Available: ${available_memory}MB"
    fi

    local available_cpu
    available_cpu=$(kubectl get nodes -o jsonpath='{.items[*].status.allocatable.cpu}' | awk '{sum += $1} END {print sum}')
    if [ "${available_cpu}" -lt "${REQUIRED_CPU}" ]; then
        exit_gracefully "Insufficient cluster CPU cores. Required: ${REQUIRED_CPU}, Available: ${available_cpu}"
    fi

    print_status "${GREEN}" "Kubernetes cluster configuration is valid ✓"
}

###############################################################################
# CHECK AVAILABLE DISK SPACE
###############################################################################
check_disk_space() {
    print_status "${YELLOW}" "Checking available disk space..."

    local available_disk_space
    available_disk_space=$(df -m / | awk 'NR==2 {print $4}')
    if [ "${available_disk_space}" -lt 20480 ]; then  # 20GB minimum
        exit_gracefully "Insufficient disk space. Required: 20480MB, Available: ${available_disk_space}MB"
    fi

    print_status "${GREEN}" "Sufficient disk space available ✓"
}

###############################################################################
# MAIN
###############################################################################
main() {
    print_status "${GREEN}" "🚀 Starting Data Lakehouse Setup v${VERSION}"

    # Parse command line arguments
    local platform="current"
    local memory_requirement="2048"

    while [[ $# -gt 0 ]]; do
        case $1 in
            --platform)
                platform="$2"
                shift 2
                ;;
            --memory)
                memory_requirement="$2"
                shift 2
                ;;
            --help)
                print_status "${GREEN}" "Usage: $0 [--platform <current|minikube|k3s>] [--memory <MB>]"
                exit 0
                ;;
            *)
                exit_gracefully "Unknown parameter: $1"
                ;;
        esac
    done

    # Initialize log file
    echo "=== Data Lakehouse Setup Log $(date) ===" > setup.log

    # Step 1: Check requirements
    check_requirements
    update_progress_bar 10

    # Step 2: Setup platform
    setup_platform "${platform}" "${memory_requirement}"
    update_progress_bar 30

    # Step 3: Install required tools with version checks
    install_tool "kubectl" \
        "curl -LO https://dl.k8s.io/release/\$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl && \
        sudo install -o root -g root -m 0755 kubectl /usr/local/bin/kubectl" \
        "1.24.0"
    update_progress_bar 50

    install_tool "helm" \
        "curl https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash" \
        "3.10.0"
    update_progress_bar 70

    # Step 4: Set up ArgoCD
    if ! kubectl get namespace argocd &>/dev/null; then
        kubectl create namespace argocd
    fi

    print_status "${YELLOW}" "Installing ArgoCD..."
    kubectl apply -n argocd -f https://raw.githubusercontent.com/argoproj/argo-cd/stable/manifests/install.yaml
    wait_for_namespace_resources "argocd" 600
    validate_argocd
    update_progress_bar 90

    # Step 5: Deploy the application of applications
    print_status "${YELLOW}" "Deploying the ArgoCD application of applications..."
    kubectl apply -n argocd -f https://raw.githubusercontent.com/joshyorko/open-datalakehouse/main/app-of-apps.yaml

    wait_for_namespace_resources "data-lakehouse" 600
    update_progress_bar 100

    # Step 6: Print ArgoCD initial password
    local argocd_password
    argocd_password=$(kubectl -n argocd get secret argocd-initial-admin-secret -o jsonpath="{.data.password}" | base64 --decode)

    print_status "${GREEN}" "🎉 Deployment completed successfully!"
    print_status "${YELLOW}" "ArgoCD Credentials:"
    echo "Username: admin"
    echo "Password: ${argocd_password}"
    print_status "${YELLOW}" "To access the ArgoCD UI, run:"
    echo "  kubectl port-forward svc/argocd-server -n argocd 8080:443"
    print_status "${YELLOW}" "Then visit: https://localhost:8080"
}

main "$@"
