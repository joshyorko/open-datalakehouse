# Open Datalakehouse Repository Structure

```
open-datalakehouse/
│
├── application-charts/
│   ├── dremio/
│   │   └── values.yaml
│   ├── nessie/
│   │   └── values.yaml
│   ├── postgres-db/
│   │   └── values.yaml
│   ├── superset/
│   │   └── values.yaml
│   └── spark/
│       └── values.yaml
│
├── apps/
│   ├── dremio.yaml
│   ├── nessie.yaml
│   ├── postgres-db.yaml
│   ├── superset.yaml
│   └── spark.yaml
│
├── app-of-apps.yaml
│
└── README.md
```

## Directory Explanations:

1. `application-charts/`: Contains subdirectories for each application, each with its own `values.yaml` file for Helm chart configuration.

2. `apps/`: Contains ArgoCD application definition YAML files for each application.

3. `app-of-apps.yaml`: The main ArgoCD application that manages all other applications.

4. `README.md`: Project documentation and instructions.
```
