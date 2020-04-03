from airflow import DAG
from datetime import datetime, timedelta
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.subdag_operator import SubDagOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 3, 3),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=20),
}

DAG_NAME = 'destination-keywords'
SCHEDULE_INTERVAL = '0 8 * * *'

namespace = 'airflow'
in_cluster = True
config_file = None


def get_s3_key_names():
    return [
        "batch_1.csv",
        "batch_2.csv",
        "batch_3.csv"
    ]


def load_subdag(parent_dag_name, child_dag_name, args):
    dag_subdag = DAG(
        dag_id='{0}.{1}'.format(parent_dag_name, child_dag_name),
        default_args=args,
        schedule_interval=SCHEDULE_INTERVAL,
    )
    with dag_subdag:
        for file_name in get_s3_key_names():
            t = DummyOperator(
                task_id='load_subdag_{0}'.format(file_name),
                default_args=args,
                dag=dag_subdag,
            )

    return dag_subdag


dag = DAG(DAG_NAME, schedule_interval=SCHEDULE_INTERVAL,
          default_args=default_args)

with dag:
    load_tasks = SubDagOperator(
        task_id='load_tasks',
        subdag=load_subdag(DAG_NAME, 'load_tasks', default_args),
        default_args=default_args,
    )

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

    load_tasks >> clean >> [idds, kvs] >> format_results

