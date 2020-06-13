from datetime import datetime, timedelta

from airflow.contrib.operators.emr_create_job_flow_operator import \
    EmrCreateJobFlowOperator
from airflow.executors import LocalExecutor
from airflow.models import DAG, Variable
from airflow.operators.docker_operator import DockerOperator
from airflow.operators.subdag_operator import SubDagOperator

import os
from typing import Optional, List

LAB_MYSQL = (
    'mysql+pymysql://cylons:XGm9DDm6CGeBr7Qc@am-cylons-aurora-mysql-cluster.'
    'cluster-c9czcayr4qpx.us-west-2.rds.amazonaws.com:3306/destinations'
    '?charset=utf8mb4&local_infile=1')

PROD_MYSQL = (
    'mysql+pymysql://cylons:XGm9DDm6CGeBr7Qc@am-cylons-aurora-mysql-cluster.'
    'cluster-cdaql20h6pjt.us-west-2.rds.amazonaws.com:3306/destinations'
    '?charset=utf8mb4&local_infile=1')

LAB_PROD_GCP = '/hcom/app/.credentials/gcp_cylons_generic.json'

# Last updated on 2020.02.13
LOCALE = ['ar_AE', 'cs_CZ', 'da_DK', 'de_AT', 'de_BE', 'de_CH', 'de_DE',
          'el_GR', 'en_AE', 'en_AS', 'en_AU', 'en_CA', 'en_CN', 'en_EM',
          'en_GB', 'en_HK', 'en_ID', 'en_IE', 'en_IL', 'en_IN', 'en_JP',
          'en_KR', 'en_LA', 'en_MX', 'en_MY', 'en_NZ', 'en_PH', 'en_SG',
          'en_TH', 'en_TW', 'en_US', 'en_VN', 'en_ZA', 'es_AR', 'es_BO',
          'es_BZ', 'es_CL', 'es_CO', 'es_CR', 'es_EC', 'es_ES', 'es_GT',
          'es_GY', 'es_HN', 'es_MX', 'es_NI', 'es_PA', 'es_PE', 'es_PY',
          'es_SV', 'es_US', 'es_UY', 'es_VE', 'et_EE', 'fi_FI', 'fr_BE',
          'fr_CA', 'fr_CH', 'fr_FR', 'fr_GF', 'hr_HR', 'hu_HU', 'in_ID',
          'is_IS', 'it_CH', 'it_IT', 'iw_IL', 'ja_JP', 'ko_KR', 'lt_LT',
          'lv_LV', 'ms_MY', 'nl_BE', 'nl_NL', 'nl_SR', 'no_NO', 'pl_PL',
          'pt_BR', 'pt_PT', 'ru_RU', 'sk_SK', 'sv_SE', 'th_TH', 'tr_TR',
          'uk_UA', 'vi_VN', 'zh_CN', 'zh_HK', 'zh_TW']

# Last updated on 2020.02.13
# unused, just for reference
FALLBACK_FEEDER = {
    'AE-ar': None,
    'AE-en': 'US-en',
    'AR-es': 'ES-es',
    'AS-en': 'US-en',
    'AT-de': 'DE-de',
    'AU-en': 'US-en',
    'BE-de': 'DE-de',
    'BE-fr': 'FR-fr',
    'BE-nl': 'NL-nl',
    'BO-es': 'ES-es',
    'BR-pt': 'PT-pt',
    'BZ-es': 'ES-es',
    'CA-en': 'US-en',
    'CA-fr': 'FR-fr',
    'CH-de': 'DE-de',
    'CH-fr': 'FR-fr',
    'CH-it': 'IT-it',
    'CL-es': 'ES-es',
    'CN-en': 'US-en',
    'CN-zh': None,
    'CO-es': 'ES-es',
    'CR-es': 'ES-es',
    'CZ-cs': None,
    'DE-de': 'CH-de',
    'DK-da': None,
    'EC-es': 'ES-es',
    'EE-et': None,
    'EM-en': 'US-en',
    'ES-es': 'MX-es',
    'FI-fi': None,
    'FR-fr': 'CH-fr',
    'GB-en': 'US-en',
    'GF-fr': 'FR-fr',
    'GR-el': None,
    'GT-es': 'ES-es',
    'GY-es': 'ES-es',
    'HK-en': 'US-en',
    'HK-zh': None,
    'HN-es': 'ES-es',
    'HR-hr': None,
    'HU-hu': None,
    'ID-en': 'US-en',
    'ID-id': None,
    'IE-en': 'US-en',
    'IL-en': 'US-en',
    'IL-he': None,
    'IN-en': 'US-en',
    'IS-is': None,
    'IT-it': 'CH-it',
    'JP-en': 'US-en',
    'JP-ja': None,
    'KR-en': 'US-en',
    'KR-ko': None,
    'LA-en': 'US-en',
    'LT-lt': None,
    'LV-lv': None,
    'MX-en': 'US-en',
    'MX-es': 'ES-es',
    'MY-en': 'US-en',
    'MY-ms': None,
    'NI-es': 'ES-es',
    'NL-nl': 'BE-nl',
    'NO-no': None,
    'NZ-en': 'US-en',
    'PA-es': 'ES-es',
    'PE-es': 'ES-es',
    'PH-en': 'US-en',
    'PL-pl': None,
    'PT-pt': 'BR-pt',
    'PY-es': 'ES-es',
    'RU-ru': None,
    'SE-sv': None,
    'SG-en': 'US-en',
    'SK-sk': None,
    'SR-nl': 'NL-nl',
    'SV-es': 'ES-es',
    'TH-en': 'US-en',
    'TH-th': None,
    'TR-tr': None,
    'TW-en': 'US-en',
    'TW-zh': None,
    'UA-uk': None,
    'US-en': 'GB-en',
    'US-es': 'ES-es',
    'UY-es': 'ES-es',
    'VE-es': 'ES-es',
    'VN-en': 'US-en',
    'VN-vi': None,
    'ZA-en': 'US-en'
}

# Last updated on 2020.02.13
FALLBACK_LOCALE = {'ar_AE': None, 'en_AE': 'en_US', 'es_AR': 'es_ES',
                   'en_AS': 'en_US', 'de_AT': 'de_DE', 'en_AU': 'en_US',
                   'de_BE': 'de_DE', 'fr_BE': 'fr_FR', 'nl_BE': 'nl_NL',
                   'es_BO': 'es_ES', 'pt_BR': 'pt_PT', 'es_BZ': 'es_ES',
                   'en_CA': 'en_US', 'fr_CA': 'fr_FR', 'de_CH': 'de_DE',
                   'fr_CH': 'fr_FR', 'it_CH': 'it_IT', 'es_CL': 'es_ES',
                   'en_CN': 'en_US', 'zh_CN': None, 'es_CO': 'es_ES',
                   'es_CR': 'es_ES', 'cs_CZ': None, 'de_DE': 'de_CH',
                   'da_DK': None, 'es_EC': 'es_ES', 'et_EE': None,
                   'en_EM': 'en_US', 'es_ES': 'es_MX', 'fi_FI': None,
                   'fr_FR': 'fr_CH', 'en_GB': 'en_US', 'fr_GF': 'fr_FR',
                   'el_GR': None, 'es_GT': 'es_ES', 'es_GY': 'es_ES',
                   'en_HK': 'en_US', 'zh_HK': None, 'es_HN': 'es_ES',
                   'hr_HR': None, 'hu_HU': None, 'en_ID': 'en_US',
                   'in_ID': None, 'en_IE': 'en_US', 'en_IL': 'en_US',
                   'iw_IL': None, 'en_IN': 'en_US', 'is_IS': None,
                   'it_IT': 'it_CH', 'en_JP': 'en_US', 'ja_JP': None,
                   'en_KR': 'en_US', 'ko_KR': None, 'en_LA': 'en_US',
                   'lt_LT': None, 'lv_LV': None, 'en_MX': 'en_US',
                   'es_MX': 'es_ES', 'en_MY': 'en_US', 'ms_MY': None,
                   'es_NI': 'es_ES', 'nl_NL': 'nl_BE', 'no_NO': None,
                   'en_NZ': 'en_US', 'es_PA': 'es_ES', 'es_PE': 'es_ES',
                   'en_PH': 'en_US', 'pl_PL': None, 'pt_PT': 'pt_BR',
                   'es_PY': 'es_ES', 'ru_RU': None, 'sv_SE': None,
                   'en_SG': 'en_US', 'sk_SK': None, 'nl_SR': 'nl_NL',
                   'es_SV': 'es_ES', 'en_TH': 'en_US', 'th_TH': None,
                   'tr_TR': None, 'en_TW': 'en_US', 'zh_TW': None,
                   'uk_UA': None, 'en_US': 'en_GB', 'es_US': 'es_ES',
                   'es_UY': 'es_ES', 'es_VE': 'es_ES', 'en_VN': 'en_US',
                   'vi_VN': None, 'en_ZA': 'en_US'}

PROD_S3_BUCKET = 'hcom-data-prod-am-tech-cylons-airflow-s3'
LAB_S3_BUCKET = 'hcom-data-lab-am-tech-cylons-airflow-s3'

PROD_HIVE_SHELL_SCRIPT = 's3://hcom-data-prod-am-tech-cylons-airflow-s3/destination_feed_script/dest_hive.sh'
LAB_HIVE_SHELL_SCRIPT = 's3://hcom-data-lab-am-tech-cylons-airflow-s3/destination_feed_script/dest_hive.sh'

PROD_LOG_URI = 's3://hcom-data-prod-am-tech-cylons-airflow-s3/destination_feed_script/logs'
LAB_LOG_URI = 's3://hcom-data-lab-am-tech-cylons-airflow-s3/destination_feed_script/logs'

PROD_HIVE_METASTURE_URIS = 'thrift://metastore-proxy.waggledance-us-west-2.hcom-data-prod.aws.hcom:48869'
LAB_HIVE_METASTURE_URIS = 'thrift://metastore-proxy.waggledance-us-west-2.hcom-data-lab.aws.hcom:48869'

PROD_EC2_SUBNET_ID = 'subnet-e7faae83'
LAB_EC2_SUBNET_ID = 'subnet-8f84e6f9'

PROD_EC2_KEY_NAME = 'hcom-data-prod-sem-tools'
LAB_EC2_KEY_NAME = 'hcom-data-lab-sem-tools'


EMR_TAGS = [
        {
            "Key": "CostCenter",
            "Value": '30198'
        },
        {
            "Key": "AssetProtectionLevel",
            "Value": '99'
        },
        {
            "Key": "RelationshipOwner",
            "Value": 'cylons@expedia.com'
        },
        {
            "Key": "DataClassification",
            "Value": 'Confidential'
        },
        {
            "Key": "Environment",
            "Value": 'DataWarehouse'
        },
        {
            "Key": "Team",
            "Value": 'CYLONS'
        },
        {
            "Key": "LaunchedBy",
            "Value": 'cylons@expedia.com'
        },
        {
            "Key": "Application",
            "Value": 'HCOM Cylons destination-feed'
        },
        {
            "Key": "Name",
            "Value": 'cylons-hive-external-table-creation'
        },
        {
            "Key": "Brand",
            "Value": 'Hotels.com'
        }
]


class MissingEnvError(Exception):
    pass


class UnknownEnvError(Exception):
    pass


def _get_env() -> Optional[str]:
    env = os.environ.copy()
    if 'ENVIRONMENT' in env:
        return env['ENVIRONMENT']
    else:
        raise MissingEnvError('Environment not found.')


def get_mysql_url() -> str:
    env = _get_env()
    if env == 'prod':
        return PROD_MYSQL
    elif env == 'lab':
        return LAB_MYSQL
    raise UnknownEnvError(
        'Unknown environment detected. lab|prod is required.')


def get_bq_creds() -> str:
    env = _get_env()
    if env in ('lab', 'prod'):
        return LAB_PROD_GCP
    raise UnknownEnvError(
        'Unknown environment detected. lab|prod is required.')


def get_s3_bucket() -> str:
    env = _get_env()
    if env == 'prod':
        return PROD_S3_BUCKET
    elif env == 'lab':
        return LAB_S3_BUCKET
    raise UnknownEnvError(
        'Unknown environment detected. lab|prod is required.')


def get_locale_list() -> List[str]:
    return LOCALE


def get_fallback_locale(locale: str) -> Optional[str]:
    fallback_locale = FALLBACK_LOCALE.get(locale, None)
    return fallback_locale


def get_hive_shell_script() -> str:
    env = _get_env()
    if env == 'prod':
        return PROD_HIVE_SHELL_SCRIPT
    elif env == 'lab':
        return LAB_HIVE_SHELL_SCRIPT
    raise UnknownEnvError(
        'Unknown environment detected. lab|prod is required.')


def get_log_uri() -> str:
    env = _get_env()
    if env == 'prod':
        return PROD_LOG_URI
    elif env == 'lab':
        return LAB_LOG_URI
    raise UnknownEnvError(
        'Unknown environment detected. lab|prod is required.')


def get_hive_metastore_uris() -> str:
    env = _get_env()
    if env == 'prod':
        return PROD_HIVE_METASTURE_URIS
    elif env == 'lab':
        return LAB_HIVE_METASTURE_URIS
    raise UnknownEnvError(
        'Unknown environment detected. lab|prod is required.')


def get_ec2_subnet_id() -> str:
    env = _get_env()
    if env == 'prod':
        return PROD_EC2_SUBNET_ID
    elif env == 'lab':
        return LAB_EC2_SUBNET_ID
    raise UnknownEnvError(
        'Unknown environment detected. lab|prod is required.')


def get_ec2_key_name() -> str:
    env = _get_env()
    if env == 'prod':
        return PROD_EC2_KEY_NAME
    elif env == 'lab':
        return LAB_EC2_KEY_NAME
    raise UnknownEnvError(
        'Unknown environment detected. lab|prod is required.')


def get_emr_environment_tag() -> str:
    env = _get_env()
    if env in ['prod', 'lab']:
        return env.capitalize()

    else:
        raise UnknownEnvError(
            'Unknown environment detected. lab|prod is required.')




default_args = {
    'owner': 'cylons',
    'depends_on_past': False,
    'start_date': datetime(2019, 2, 27),
    'email': ['cylons@expedia.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=15),
    'catchup': False,
}

DAG_NAME = 'destination_feed'

dag = DAG(
    dag_id=DAG_NAME,
    default_args=default_args,
    schedule_interval="0 0 * * 1-5"
)

hive_host = Variable.get("hive-host", default_var='')


def destination_etl_subdag(parent_dag_name, child_dag_name, args):
    dag_subdag = DAG(
        dag_id=f'{parent_dag_name}.{child_dag_name}',
        default_args=args,
        schedule_interval="@once"
    )

    for locale in get_locale_list():
        fallback_locale = get_fallback_locale(locale)
        DockerOperator(
            task_id=f'destination_etl_{locale}',
            image='destination-feed',
            command=f' --database="{get_mysql_url()}" --option=locale'
                    f' --locale={locale} --fallback-locale={fallback_locale}',
            auto_remove=True,
            volumes=['/home/ec2-user/.credentials:/hcom/app/.credentials'],
            default_args=args,
            execution_timeout=timedelta(minutes=15),
            pool='geo',
            dag=dag_subdag
        )

    return dag_subdag


destination_relationship = DockerOperator(
    task_id='destination_relationship',
    default_args=default_args,
    image='destination-feed',
    command=f'--database="{get_mysql_url()}" --option=relation',
    auto_remove=True,
    volumes=['/home/ec2-user/.credentials:/hcom/app/.credentials'],
    execution_timeout=timedelta(minutes=10),
    dag=dag,
)

destination_prepare = DockerOperator(
    task_id='destination_prepare',
    default_args=default_args,
    image='destination-feed',
    command=f' --database="{get_mysql_url()}" --option=prepare',
    auto_remove=True,
    volumes=['/home/ec2-user/.credentials:/hcom/app/.credentials'],
    execution_timeout=timedelta(minutes=5),
    dag=dag,
)

destination_etl_section = SubDagOperator(
    task_id='destination_etl_section',
    executor=LocalExecutor(),
    subdag=destination_etl_subdag(DAG_NAME,
                                  'destination_etl_section',
                                  default_args),
    default_args=default_args,
    dag=dag,
)

destination_annual_performance = DockerOperator(
    task_id='destination_annual_performance',
    default_args=default_args,
    image='destination-feed',
    command=f' --database="{get_mysql_url()}"'
            f' --option=destination_annual_performance'
            f' --bq-path="{get_bq_creds()}"',
    auto_remove=True,
    volumes=['/home/ec2-user/.credentials:/hcom/app/.credentials'],
    execution_timeout=timedelta(minutes=90),
    dag=dag,
)

destination_disambiguation = DockerOperator(
    task_id='destination_disambiguation',
    default_args=default_args,
    image='destination-feed',
    command=f' --database="{get_mysql_url()}" --option=disambiguate',
    auto_remove=True,
    volumes=['/home/ec2-user/.credentials:/hcom/app/.credentials'],
    execution_timeout=timedelta(minutes=90),
    dag=dag,
)

update_view = DockerOperator(
    task_id='update_view',
    default_args=default_args,
    image='destination-feed',
    command=f' --database="{get_mysql_url()}" --option=update',
    auto_remove=True,
    volumes=['/home/ec2-user/.credentials:/hcom/app/.credentials'],
    execution_timeout=timedelta(minutes=5),
    dag=dag,
)

destination_country_codes = DockerOperator(
    task_id='destination_country_codes',
    default_args=default_args,
    image='destination-feed',
    command=f' --database="{get_mysql_url()}" --option=country-codes',
    auto_remove=True,
    volumes=['/home/ec2-user/.credentials:/hcom/app/.credentials'],
    execution_timeout=timedelta(minutes=10),
    dag=dag,
)

# This managed to create an external Hive table on top of S3 data
STEPS = [
    {
        'Name': 'Execute Hive query',
        'HadoopJarStep': {
            'Jar': 's3://us-west-2.elasticmapreduce/libs/script-runner/script-runner.jar',
            'Args': [get_hive_shell_script()]
        },
        'ActionOnFailure': 'CONTINUE'
    }
]

JOB_FLOW_OVERRIDES = {
    'Name': 'cylons-hive-external-table-creation',
    'LogUri': get_log_uri(),
    'Instances':
        {'InstanceGroups': [
            {'Name': 'Master nodes', 'Market': 'ON_DEMAND',
             'InstanceRole': 'MASTER', 'InstanceType': 'r3.2xlarge',
             'InstanceCount': 1,
             'Configurations':
                 [
                     {
                         'Classification': 'hive-site',
                         "Properties": {
                             "hive.metastore.uris": get_hive_metastore_uris()
                         }
                     }
                 ]
             },
            {'Name': 'Slave nodes', 'Market': 'ON_DEMAND',
             'InstanceRole': 'CORE', 'InstanceType': 'r3.2xlarge',
             'InstanceCount': 1}
        ],
            'KeepJobFlowAliveWhenNoSteps': False,
            'Ec2SubnetId': get_ec2_subnet_id(),
            'Ec2KeyName': get_ec2_key_name(),
            'TerminationProtected': True
        },
    'Applications': [{'Name': 'Hive'}],
    'Tags': EMR_TAGS,
    'Steps': STEPS,
}

emr_cluster_creator = EmrCreateJobFlowOperator(
    task_id='emr_cluster_creator',
    job_flow_overrides=JOB_FLOW_OVERRIDES,
    dag=dag
)

destination_upload = DockerOperator(
    task_id='destination_upload',
    default_args=default_args,
    image='destination-feed',
    command=f' --database="{get_mysql_url()}" --option=upload'
            f' --bucket="{get_s3_bucket()}"'
            f' --hive-host={hive_host}',
    auto_remove=True,
    volumes=[
        '/home/ec2-user/.credentials:/hcom/app/.credentials',
        '/home/ec2-user/.aws:/home/hcom/.aws'
    ],
    execution_timeout=timedelta(minutes=120),
    dag=dag,
)

destination_relationship >> destination_prepare >> destination_etl_section \
>> update_view >> destination_annual_performance >> destination_disambiguation \
>> destination_country_codes >> destination_upload >> emr_cluster_creator
