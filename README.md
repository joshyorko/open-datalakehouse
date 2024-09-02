---

title: Open Datalakehouse - Bootstrapping a Datalakehouse on Kubernetes  
author: Joshua Yorko, [@joshyorko](https://github.com/joshyorko), joshua.yorko@gmail.com  
---

# Open Datalakehouse - Bootstrapping a Datalakehouse on Kubernetes

![Logo](utils/nessie_dremio_sad.png)

**DISCLAIMER - THIS IS NOT MEANT FOR PRODUCTION! - Open a GitHub issue first! - DISCLAIMER**

---

## Whoami

Just a really big nerd who likes Distributed Systems and bootstrapping stuff

Josh Yorko - [@joshyorko](https://github.com/joshyorko) - joshua.yorko@gmail.com

## Goal

To simplify the deployment and management of a complete data lakehouse on Kubernetes, demonstrating best practices in GitOps, distributed systems, and data engineering. This project assumes that you have a basic understanding of Kubernetes and GitOps principles as well as experience with the tools and technologies used in the data lakehouse architecture.  This data lake is meant to work, however you will need to fine tune your workloads resources obviously to scale to your needs.

## Technologies Used

- [Kubernetes](https://kubernetes.io/) (The foundation of our platform)
- [ArgoCD](https://argoproj.github.io/argo-cd/) (GitOps continuous delivery)
- [Minio](https://min.io/) (S3-compatible object storage, using Bitnami chart)
- [Dremio](https://www.dremio.com/) (SQL query engine for data lakes, using Bitnami chart)
- [Project Nessie](https://projectnessie.org/) (Multi-modal versioned data catalog, using Bitnami chart)
  - [PostgreSQL](https://www.postgresql.org/) (Database for Nessie, using Bitnami chart)
- [Apache Superset](https://superset.apache.org/) (Business intelligence and data visualization, using official chart)
- [Jupyter Labs](https://jupyter.org/) (Custom PySpark Notebook with Spark built in)

## Prerequisites

- [Kubernetes cluster](https://kubernetes.io/docs/setup/) (tested on Minikube, k3s, EKS)
- [Helm](https://helm.sh/docs/intro/install/) (version v3.15.2)
- [kubectl](https://kubernetes.io/docs/tasks/tools/install-kubectl/) (compatible with your cluster version)
- Basic understanding of Kubernetes concepts and ArgoCD

## Quick Start

TLDR;

```bash
curl -sSL https://raw.githubusercontent.com/joshyorko/open-datalakehouse/main/setup_datalkehouse.sh | bash
```

or Assuming you have a cluster already setup

```bash
git clone https://github.com/joshyorko/open-datalakehouse.git
cd open-datalakehouse
kubectl create ns argocd
kubectl apply -n argocd -f https://raw.githubusercontent.com/argoproj/argo-cd/stable/manifests/install.yaml
kubectl apply -f app-of-apps.yaml
```

### Automated Setup Script

To streamline the setup process, a bash script has been provided to automate the creation of a high-availability Minikube cluster and the deployment of the data lakehouse components. The script will guide you through the following steps:

1. **Use Minikube or Current Context**: The script will detect if you have a Kubernetes context available. If not, it will use Minikube for local development.

2. **Graceful Exit**: If no Kubernetes context is detected after choosing not to use Minikube, the script will exit gracefully.

3. **Deploy Components**: The script will automatically install ArgoCD and apply the Open Datalakehouse from the app-of-apps.yaml manifest located in the root of the repository.

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

## Architecture Overview

This project deploys a complete data lakehouse architecture on Kubernetes:

- [Minio](https://min.io/) serves as the object storage layer (deployed using Bitnami Helm chart)
- [Dremio](https://www.dremio.com/) provides SQL query capabilities over the data lake (deployed using Bitnami Helm chart)
- [Project Nessie](https://projectnessie.org/) acts as a versioned metadata catalog (deployed using Bitnami Helm chart)
  - Nessie relies on a [PostgreSQL](https://www.postgresql.org/) database, also deployed using a Bitnami Helm chart
- [Apache Superset](https://superset.apache.org/) offers data visualization and exploration (deployed using the official Helm chart)
- Custom [Jupyter Lab](https://jupyter.org/) Image with Spark built in for PySpark Notebooks enables distributed data processing (custom image built and maintained by the project author)

By using Bitnami charts for Dremio, Nessie, Minio, and PostgreSQL, we ensure consistent and well-maintained deployments of these components. The official Superset chart provides the latest features and best practices for deploying Superset. The custom Spark image allows for tailored configuration and dependencies specific to this project's needs.

## Dremio UI Setup for Nessie and S3 Storage

After deploying Dremio, you will notice that the follow has been setup for you:

1. Log in to the Dremio UI
2. Nessie Has been added as a data source
3. Minio has been added as an Object Store
4. Three Workspace spaces have been created for you:
   - Bronze
   - Silver
   - Gold

These settings will ensure that Dremio can properly communicate with Minio for S3-compatible storage and Nessie for metadata management.

## Some Nice to Haves

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

1. Check [ArgoCD application status](https://argo-cd.readthedocs.io/en/stable/user-guide/application-status/):
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

