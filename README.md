# Open Datalakehouse - Bootstrapping a Datalakehouse on Kubernetes
![Logo](utils/nessie_dremio_sad.png)
## Technologies Used:

ArgoCD, Minio, Dremio, Cert-Manager, Let's Encrypt, Apache Superset, Project Nessie, PostgreSQL, Spark, Nginx

## The Tutorial: A Step-by-Step Guide to Controlled Chaos


Prerequisites:
- A Computer/s (one that doesn't burst into flames at the mention of Kubernetes)
- A Kubernetes Cluster (Minikube for the faint of heart, [TechnoTim's k3s-ansible](https://github.com/techno-tim/k3s-ansible) for the brave)
- MetalLB (unless you're on EKS, in which case, they've got you covered, bro)
- Helm (because manually managing Kubernetes resources is like trying to herd cats while blindfolded)

For this tutorial, we'll focus on Minikube. Don't worry, EKS is coming but only at the end.



### Step 1: Install Minikube

First, let's get Minikube up and running. Choose your poison:

```bash
# For macOS
brew install minikube

# For Linux
curl -LO https://storage.googleapis.com/minikube/releases/latest/minikube-linux-amd64
sudo install minikube-linux-amd64 /usr/local/bin/minikube

# For Windows (using PowerShell)
New-Item -Path 'c:\' -Name 'minikube' -ItemType Directory -Force
Invoke-WebRequest -OutFile 'c:\minikube\minikube.exe' -Uri 'https://github.com/kubernetes/minikube/releases/latest/download/minikube-windows-amd64.exe'
Add-MemberPath 'c:\minikube'
```

### Step 2: Install Helm

Because who doesn't love another layer of abstraction?

```bash
# For macOS
brew install helm

# For Linux
curl https://raw.githubusercontent.com/helm/helm/master/scripts/get-helm-3 | bash

# For Windows (using PowerShell)
choco install kubernetes-helm
```

### Step 3: Install ArgoCD and ArgoCD CLI (TODO LATER)

Stay tuned for more pain... I mean, progress!



## Directory Explanations:

1. `application-charts/`: Contains subdirectories for each application, each with its own `values.yaml` file for Helm chart configuration.

2. `apps/`: Contains ArgoCD application definition YAML files for each application.

3. `app-of-apps.yaml`: The main ArgoCD application that manages all other applications.

4. `scripts/`: Contains scripts for managing backups and running Spark jobs and creating Fake data for the datalakehouse.'

5. `utils/`: Contains utility scripts for managing the repository.
