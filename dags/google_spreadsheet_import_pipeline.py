import os
import logging
from datetime import timedelta
from airflow import DAG
from data_pipeline.utils.data_store.bq_data_service import (
    create_table_if_not_exist,
    does_bigquery_table_exist,
)
from data_pipeline.utils.dags.data_pipeline_dag_utils import (
    get_default_args,
    create_python_task,
)
from data_pipeline.spreadsheet_data.google_spreadsheet_config import MultiCsvSheet
from data_pipeline.utils.data_store.s3_data_service import (
    download_s3_yaml_object_as_json,
    download_s3_json_object,
)
from data_pipeline.spreadsheet_data.google_spreadsheet_etl import etl_google_spreadsheet

LOGGER = logging.getLogger(__name__)

DEPLOYMENT_ENV = 'DEPLOYMENT_ENV'
DEFAULT_DEPLOYMENT_ENV_VALUE = None


def get_env_var_or_use_default(env_var_name, default_value):
    return os.getenv(env_var_name, default_value)


def get_data_config(bucket_name, s3_object_key, **kwargs):
    data_config_dict = download_s3_yaml_object_as_json(
        bucket_name, s3_object_key
    )
    data_config = MultiCsvSheet(data_config_dict)
    dep_env = get_env_var_or_use_default(DEPLOYMENT_ENV,
                                         DEFAULT_DEPLOYMENT_ENV_VALUE)
    env_based_data_config = (
        data_config if dep_env is None
        else data_config.modify_config_based_on_deployment_env(dep_env)
    )
    kwargs["ti"].xcom_push(key="data_config", value=env_based_data_config)


def create_bq_table_if_not_exist(**kwargs):
    dag_context = kwargs["ti"]
    multi_sheet_config = dag_context.xcom_pull(
        key="data_config",
        task_ids="get_data_config")

    for _, sheet_config in multi_sheet_config.sheets_config.items():
        dataset = sheet_config.dataset_name
        table = sheet_config.table_name
        if not does_bigquery_table_exist(project_name=multi_sheet_config.gcp_project,
                                         dataset_name=dataset, table_name=table):
            schema_json = download_s3_json_object(
                sheet_config.schema_bucket, sheet_config.schema_bucket_object_name
            )
            create_table_if_not_exist(
                project_name=multi_sheet_config.gcp_project,
                dataset_name=dataset,
                table_name=table,
                json_schema=schema_json,
            )


def google_spreadsheet_data_etl(**kwargs):
    dag_context = kwargs["ti"]
    data_config = dag_context.xcom_pull(
        key="data_config",
        task_ids="get_data_config")
    etl_google_spreadsheet(data_config)


def create_dag(dag_name,
               bucket_name,
               s3_object_key,
               schedule_interval,
               args):
    dag = DAG(
            dag_id=dag_name,
            default_args=args,
            schedule_interval=schedule_interval,
            dagrun_timeout=timedelta(minutes=60)
    )

    with dag:
        get_data_config_task = create_python_task(
            dag, 'get_data_config', get_data_config, retries=5,
            op_kwargs={'bucket_name': bucket_name,
                       's3_object_key': s3_object_key}
        )
        create_table_if_not_exist_task = create_python_task(
            dag, "create_table_if_not_exist", create_bq_table_if_not_exist, retries=5
        )
        google_spreadsheet_data_etl_task = create_python_task(
            dag,
            "google_spreadsheet_data_etl",
            google_spreadsheet_data_etl,
        )
        google_spreadsheet_data_etl_task.set_upstream(create_table_if_not_exist_task)
        create_table_if_not_exist_task.set_upstream(get_data_config_task)

    return dag


dag_specs = [
    {"dag_name": "Load_Google_Spreadsheet_Into_Bigquery_BRE_keywords",
     "bucket" : "ci-elife-data-pipeline",
     "s3_object" : "airflow_test/spreadsheet_data/BRE_keywords/bre_keywords.config.yaml",
     "schedule_interval": "@daily"
     },
    {"dag_name": "Load_Google_Spreadsheet_Into_Bigquery_BRE_Distribution",
     "bucket": "ci-elife-data-pipeline",
     "s3_object": "airflow_test/spreadsheet_data/BRE_distribution/data-pipeline.config.yaml",
     "schedule_interval": "@daily"
     }

]

for d_spec in dag_specs:
    globals()[d_spec.get("dag_name")] = create_dag(dag_name=d_spec.get("dag_name"),
                                                   bucket_name=d_spec.get("bucket"),
                                                   s3_object_key=d_spec.get("s3_object"),
                                                   schedule_interval=d_spec.get("schedule_interval"),
                                                   args=get_default_args())

