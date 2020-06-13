#!/usr/bin/env bash
set -e

# This File can't be executed in single step, please user Makefile.
# These commands are just for reference

# Step-1 Create minikube cluster
minikube start --vm-driver=virtualbox --memory=6096 --disk-size=20000mb --kubernetes-version v1.17.1
eval $(minikube docker-env)

# Step-2 create airflow namespace in Kubectl
kubectl config set-context minikube --cluster=minikube --namespace=airflow
kubectl delete namespace airflow || true
kubectl create namespace airflow

# Step-3 Build Docker image
docker build -t airflow-docker-local:1 docker-airflow/ --no-cache

# Step-4 Install helm dependecies and install charts into cluster
helm delete airflow || true
helm dependency build stable/airflow/
helm install airflow  -f airflow.yaml stable/airflow

# Step 5 - Open Airflow Web UI
minikube service airflow-web -n airflow

# Step 6 - Monitor Pods
kubectl get pods --watch


<<reference commands
 1) Copy a DAG in a hacky way
           $ kubectl get pods --template '{{range .items}}{{.metadata.name}}{{"\n"}}{{end}}' | grep "airflow-scheduler" | xargs -I {} kubectl cp s3_dag_test.py  {}:/usr/local/airflow/dags -n airflow
           $ kubectl get pods --template '{{range .items}}{{.metadata.name}}{{"\n"}}{{end}}' | grep "airflow-scheduler" | xargs -I {} kubectl cp deadpool_eg.py  {}:/usr/local/airflow/dags -n airflow

 2) RUN an image hackily
           $ kubectl -n airflow run -i -t python --image=python:3.7.4-slim-stretch --restart=Never --command -- /bin/sh

reference
