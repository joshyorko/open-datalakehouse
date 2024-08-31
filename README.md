# Open Datalakehouse - Bootstrapping a Datalakehouse on Kubernetes
![Logo](utils/nessie_dremio_sad.png)
## Technologies Used:


Whoami


ArgoCD, Minio, Dremio,  Apache Superset, Project Nessie, PostgreSQL, jupyter labs, PySpark


## Introduction

This project is a demonstration of how to bootstrap a datalakehouse on Kubernetes. The project uses ArgoCD to deploy the applications on Kubernetes. The project uses Minio as the object storage, Dremio as query engine on the data lakehouse, Apache Superset as the BI tool, Project Nessie as the catalogue, and PostgreSQL as the metadata store. The project also uses Jupyter labs and PySpark for data processing and analysis.

## Prerequisites

- Kubernetes Cluster
- Helm
- ArgoCD CLI (Optional)
- kubectl
- Docker (Optional) - to build and run company datalake creation.

1. kubectl create ns longhorn-system
2. helm repo add longhorn https://charts.longhorn.io
3. helm repo update
4. helm install longhorn longhorn/longhorn --namespace longhorn-system
5. kubectl create ns argocd
6. kubectl apply -n argocd -f https://raw.githubusercontent.com/argoproj/argo-cd/stable/manifests/install.yaml
7. argocd admin initial-password -n argocd  
8. kubectl port-forward svc/argocd-server -n argocd 8080:443
9. argocd login localhost:8080
10. argocd account update-password
11. kubectl apply -f https://raw.githubusercontent.com/joshyorko/open-datalakehouse/main/app-of-apps.yaml


This  will bootstrap your data lake for dremio setup and nessies setup I suggest you follow this guide here :

This is for the dremio ui setup for nessie and also pretty much the same for s3 storage setup for dremio

Set root path to “warehouse” (any bucket you have access too) Set the following connection properties:
fs.s3a.path.style.access to true
fs.s3a.endpoint to dremio-minio:9000
dremio.s3.compat to true

kubectl get services -n data-lakehouse





