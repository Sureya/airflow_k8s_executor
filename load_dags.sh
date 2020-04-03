#!/usr/bin/env bash

DAGS_FOLDER=$1
SLEEP_DURARTION=30
echo " Sleeping for ${SLEEP_DURARTION}s"
sleep ${SLEEP_DURARTION}

for filename in ${DAGS_FOLDER}/*.py; do
    echo " COPYING DAG: ${filename}"
    kubectl get pods --template '{{range .items}}{{.metadata.name}}{{"\n"}}{{end}}' | grep "airflow-scheduler" | xargs -I {} kubectl cp ${filename}  {}:/usr/local/airflow/dags -n airflow
done