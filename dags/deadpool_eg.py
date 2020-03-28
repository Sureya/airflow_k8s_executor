from airflow import DAG
from datetime import datetime, timedelta
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

namespace = 'airflow'
in_cluster = True
config_file = None

dag = DAG('destination-keywords',
          schedule_interval='@once',
          default_args=default_args)

with dag:
    clean = KubernetesPodOperator(
        namespace=namespace,
        image="hello-world",
        labels={"foo": "bar"},
        env_vars={'TASK_1_NAME': 'TASK1'},
        name="clean_destination_name",
        task_id="clean_destination_name",
        in_cluster=in_cluster,
        get_logs=True,
        image_pull_policy='IfNotPresent',
        is_delete_operator_pod=True,
    )

    idds = KubernetesPodOperator(
        namespace=namespace,
        image="hello-world",
        labels={"foo": "bar"},
        env_vars={'TASK_2_NAME': 'TASK2'},
        name="idds",
        task_id="idds",
        in_cluster=in_cluster,
        get_logs=True,
        image_pull_policy='IfNotPresent',
        is_delete_operator_pod=True,
    )

    kvs = KubernetesPodOperator(
        namespace=namespace,
        image="hello-world",
        labels={"foo": "bar"},
        env_vars={'TASK_3_NAME': 'TASK3'},
        name="kvs",
        task_id="kvs",
        in_cluster=in_cluster,
        get_logs=True,
        image_pull_policy='IfNotPresent',
        is_delete_operator_pod=True,
    )

    format_results = KubernetesPodOperator(
        namespace=namespace,
        image="hello-world",
        labels={"foo": "bar"},
        env_vars={'TASK_3_NAME': 'TASK3'},
        name="format_results",
        task_id="format_results",
        in_cluster=in_cluster,
        get_logs=True,
        image_pull_policy='IfNotPresent',
        is_delete_operator_pod=True
    )

    clean >> [idds, kvs] >> format_results

