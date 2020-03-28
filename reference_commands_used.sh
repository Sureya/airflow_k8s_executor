#!/usr/bin/env bash
set -e

# This File can't be executed in single step, please user Makefile.
# These commands are just for reference

# Step-1 Create minikube cluster
minikube start --vm-driver=virtualbox --memory=6096 --disk-size=20000mb --kubernetes-version v1.15.0
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


# 1) To Copy a DAG into airflow
    # put your python dag file in `dags/` folder
    bash ./load_dags.sh dags 0
    # dags = folder in which all the dags are available
    # 0 = seconds to wait before uploading dags

# Create local registry
docker run -d -p 5000:5000 --restart=always --name registry registry:2

# Build and tag
docker build -t local_reg_test .
docker tag local_reg_test localhost:5000/local_reg_test:1
