from airflow import DAG
from datetime import datetime, timedelta
from airflow.contrib.operators.kubernetes_pod_operator import \
    KubernetesPodOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.executors import get_default_executor

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 4, 3),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=22),
}

DAG_NAME = 'deadpool-v2-poc'
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

            lower = KubernetesPodOperator(
                namespace=namespace,
                image="sureya/country-processing:v1",
                labels={"foo": "bar"},
                arguments=[
                    "-e S3_FILE_NAME=sks-airflow-logs/airflow-poc/run_001/input/countries.csv",
                    "-e TASK_NAME=apply_lower"],
                env_vars={'AWS_ACCESS_KEY_ID': 'X',
                          'AWS_SECRET_ACCESS_KEY': 'X',
                          'AWS_DEFAULT_REGION': 'eu-west-2'},
                name="apply_lower_case",
                task_id="apply_lower_case",
                in_cluster=in_cluster,
                get_logs=True,
                image_pull_policy='IfNotPresent',
                is_delete_operator_pod=True
            )

            t >> lower

    return dag_subdag


dag = DAG(DAG_NAME, schedule_interval=SCHEDULE_INTERVAL,
          default_args=default_args)

with dag:
    load_tasks = SubDagOperator(
        task_id='deadpool_v2',
        subdag=load_subdag(DAG_NAME, 'deadpool_v2', default_args),
        default_args=default_args,
        executor=get_default_executor()
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

    load_tasks >> format_results
