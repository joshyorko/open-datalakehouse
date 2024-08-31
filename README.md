---
title: Open Datalakehouse - Bootstrapping a Datalakehouse on Kubernetes
author: Joshua Yorko, @joshyorko, joshua.yorko@gmail.com
---
# Open Datalakehouse - Bootstrapping a Datalakehouse on Kubernetes

![Logo](utils/nessie_dremio_sad.png)

**DISCLAIMER - THIS IS NOT MEANT FOR PRODUCTION! - Open a GitHub issue first! - DISCLAIMER**

---

## Whoami

Just a really big nerd who likes Distributed Systems and bootstrapping stuff 

Josh Yorko - @joshyorko - joshua.yorko@gmail.com

## Goal

To simplify the deployment and management of a complete data lakehouse on Kubernetes, demonstrating best practices in GitOps, distributed systems, and data engineering.

## Technologies Used

- Kubernetes (The foundation of our platform)
- ArgoCD (GitOps continuous delivery)
- Minio (S3-compatible object storage, using Bitnami chart)
- Dremio (SQL query engine for data lakes, using Bitnami chart)
- Project Nessie (Multi-modal versioned data catalog, using Bitnami chart)
  - PostgreSQL (Database for Nessie, using Bitnami chart)
- Apache Spark (Distributed data processing, using a custom image)
- Apache Superset (Business intelligence and data visualization, using official chart)
- Jupyter Labs (Custom PySpark Notebook with Spark built in)

## Prerequisites

- Kubernetes cluster (tested on Minikube, k3s, EKS)
- Helm (version v3.15.2)
- kubectl (compatible with your cluster version)
- Basic understanding of Kubernetes concepts and ArgoCD

## Quick Start

### Automated Setup Script

To streamline the setup process, a bash script has been provided to automate the creation of a high-availability Minikube cluster and the deployment of the data lakehouse components. The script will guide you through the following steps:

1. **Use Minikube or Current Context**: The script will ask if you want to use Minikube. If you choose Minikube, it will spin up a high-availability Minikube cluster using Docker and Containerd. If you prefer to use your current Kubernetes context, the script will proceed with that.

2. **Graceful Exit**: If no Kubernetes context is detected after choosing not to use Minikube, the script will exit gracefully.

3. **Deploy Components**: The script will automatically install Longhorn, ArgoCD, and the other components needed to set up the data lakehouse.

### Running the Setup Script

1. Clone this repository to your local machine:
   ```bash
   git clone https://github.com/joshyorko/open-datalakehouse.git
   cd open-datalakehouse
   ```

2. Make the script executable:
   ```bash
   chmod +x setup_datalakehouse.sh
   ```

3. Run the script:
   ```bash
   ./setup_datalakehouse.sh
   ```

The script will handle the setup process, including the deployment of Longhorn, ArgoCD, and the data lakehouse components.

### Manual Setup Steps (If Needed)

If you prefer to follow the steps manually or want to understand the process in more detail, you can follow these steps:

1. Set up Longhorn for storage:
   ```bash
   kubectl create ns longhorn-system
   helm repo add longhorn https://charts.longhorn.io
   helm repo update
   helm install longhorn longhorn/longhorn --namespace longhorn-system
   ```

2. Set up ArgoCD:
   ```bash
   kubectl create ns argocd
   kubectl apply -n argocd -f https://raw.githubusercontent.com/argoproj/argo-cd/stable/manifests/install.yaml
   ```

3. Get the initial ArgoCD password:
   ```bash
   kubectl -n argocd get secret argocd-initial-admin-secret -o jsonpath="{.data.password}" | base64 --decode
   ```

4. Access ArgoCD UI:
   ```bash
   kubectl port-forward svc/argocd-server -n argocd 8080:443
   ```

5. Log in to ArgoCD and change the password:
   ```bash
   argocd login localhost:8080
   argocd account update-password
   ```

6. Deploy the ArgoCD application of applications:
   ```bash
   kubectl apply -f https://raw.githubusercontent.com/joshyorko/open-datalakehouse/main/app-of-apps.yaml
   ```

7. Monitor the deployment:
   ```bash
   kubectl get applications -n argocd
   ```

8. Access the deployed applications:
    ```bash
    kubectl get services -n data-lakehouse
    ```

## Architecture Overview

This project deploys a complete data lakehouse architecture on Kubernetes:

- Minio serves as the object storage layer (deployed using Bitnami Helm chart)
- Dremio provides SQL query capabilities over the data lake (deployed using Bitnami Helm chart)
- Project Nessie acts as a versioned metadata catalog (deployed using Bitnami Helm chart)
  - Nessie relies on a PostgreSQL database, also deployed using a Bitnami Helm chart
- Apache Superset offers data visualization and exploration (deployed using the official Helm chart)
- Custom Jupyter Lab Image with Spark built in for PySpark Notebooks enables distributed data processing (custom image built and maintained by the project author)

By using Bitnami charts for Dremio, Nessie, Minio, and PostgreSQL, we ensure consistent and well-maintained deployments of these components. The official Superset chart provides the latest features and best practices for deploying Superset. The custom Spark image allows for tailored configuration and dependencies specific to this project's needs.

## Dremio UI Setup for Nessie and S3 Storage

After deploying Dremio, follow these steps to set up the connection to Nessie and S3 storage:

1. Log in to the Dremio UI
2. Add http://nessie:19120/api/v1 as the ness url in the Dremio UI
3. Set the root path to "warehouse" (or any bucket you have access to)
4. Set the following connection properties:
   - `fs.s3a.path.style.access` to `true`
   - `fs.s3a.endpoint` to `dremio-minio:9000`
   - `dremio.s3.compat` to `true`

These settings will ensure that Dremio can properly communicate with Minio for S3-compatible storage and Nessie for metadata management.

## Data Generation and Analysis Tools

This project includes several tools to help you generate sample data and analyze it within your data lakehouse:

### Data Generation Scripts

1. Go Script (`scripts/main_minio.go`):
   - Generates fake company, employee, and department data.
   - Writes data directly to MinIO in Parquet format.
   - Supports concurrent data generation and upload for improved performance.

2. Python Script (`scripts/company.py`):
   - Generates fake company, employee, and department data.
   - Writes data to CSV files locally.
   - Provides a simpler alternative to the Go script.

3. FastAPI Application (`scripts/app.py`):
   - Offers a RESTful API for generating and uploading fake data to S3.
   - Useful for programmatic data generation and integration with other tools.

To use these scripts, navigate to the `scripts` directory and run them with Python or Go, depending on the script.

### Jupyter Notebooks

The project includes two Jupyter notebooks in the `DockerFiles/notebooks` directory:

1. `start_here.ipynb`:
   - Demonstrates how to initialize a Spark session and interact with the data lakehouse.
   - Shows examples of querying Iceberg tables and loading data into DuckDB for analysis.

2. `test.ipynb`:
   - Contains examples of writing data to Iceberg tables using Spark.
   - Demonstrates querying and analyzing data using Spark and DuckDB.

### Pre-built Docker Image

A pre-built Docker image is available on Docker Hub, containing all the necessary dependencies for running the Jupyter notebooks and interacting with the data lakehouse. To use this image:

1. Pull the image:
   ```bash
   docker pull jedock87/datalake-spark:latest
   ```

2. Run the container:
   ```bash
   docker run -p 8888:8888 -v /path/to/your/notebooks:/home/jovyan/work jedock87/datalake-spark:latest
   ```

This will start a Jupyter Lab instance with PySpark and all required dependencies pre-installed.

## Troubleshooting

1. Check ArgoCD application status:
   ```bash
   kubectl get applications -n argocd
   ```

2. View logs for a specific pod:
   ```bash
   kubectl logs -n <namespace> <pod-name>
   ```

3. Describe a pod for more details:
   ```bash
   kubectl describe pod -n <namespace> <pod-name>
   ```

## Conclusion

This project demonstrates a Kubernetes-native approach to building a modern data lakehouse. It leverages GitOps principles for deployment and management, showcasing the integration of various open-source technologies in a distributed systems architecture.

Remember, this setup is intended for development and testing purposes. For production deployments, additional security measures, high availability configurations, and performance tuning would be necessary.

Contributions and feedback are welcome! Open an issue or submit a pull request to help improve this project.