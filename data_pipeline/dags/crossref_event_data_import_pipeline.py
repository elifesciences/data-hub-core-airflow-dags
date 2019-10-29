from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
import logging
from datetime import timedelta
from pathlib import Path
from data_pipeline.utils.cloud_data_store.s3_data_service import upload_file
from data_pipeline.dags.data_pipeline_dag_utils import get_default_args
from data_pipeline.utils.cloud_data_store.bq_data_service import create_table_if_not_exist, does_bigquery_table_exist
from data_pipeline.utils.cloud_data_store.s3_data_service import download_s3_yaml_object_as_json, download_s3_json_object,upload_s3_object
import os
import json, datetime
from datetime import timezone

from data_pipeline.crossref_event_data.extract_crossref_data import get_last_run_day_from_cloud_storage, etl_data_return_errors, write_json_to_file
LOGGER = logging.getLogger(__name__)
DEFAULT_ARGS = get_default_args()

#TODO : create json keys as constant
CROSSREF_CONFIG_S3_BUCKET='prod-elife-data-pipeline'
CROSSREF_CONFIG_S3_OBJECT_KEY="airflow_test/crossref_event/elife-data-pipeline.de_dev.config.yaml"

def get_schedule_interval ():
    return os.getenv('CROSS_REF_IMPORT_SCHEDULE_INTERVAL', '@once')


dag = DAG(
    dag_id="Load_Crossref_Event_Into_Bigquery",
    default_args=DEFAULT_ARGS,
    schedule_interval=get_schedule_interval(),
    dagrun_timeout=timedelta(minutes=60),
)


def create_python_task(dag, task_id, python_callable, trigger_rule='all_success', retries=0):
    return PythonOperator(task_id=task_id, dag=dag, python_callable=python_callable, trigger_rule=trigger_rule, retries=retries)

def get_task_run_instance_fullname(task_context):
    return '___'.join([task_context.get('dag').dag_id, task_context.get('run_id'), task_context.get('task').task_id])

def branch_if_uninserted_row_exist(**kwargs):
    ti = kwargs["ti"]
    data_completely_loaded = bool(ti.xcom_pull(
        key="data_completely_loaded", task_ids="get_and_transform_load_crossref_event_data"
    ))
    if data_completely_loaded:
        return 'log_last_execution'
    else:
        return 'write_uninserted_to_object_store_and_cleanup'

def get_data_config(**kwargs):
    data_config = download_s3_yaml_object_as_json(CROSSREF_CONFIG_S3_BUCKET, CROSSREF_CONFIG_S3_OBJECT_KEY)
    kwargs["ti"].xcom_push(key="data_config", value=data_config)


def create_bq_table_if_not_exist(**kwargs):
    ti = kwargs["ti"]
    data_config = ti.xcom_pull(
        key="data_config", task_ids="get_data_config"
    )
    project_name = data_config.get('PROJECT_NAME')
    dataset = data_config.get('DATASET')
    table= data_config.get('TABLE')
    imported_timestamp_field = data_config.get('IMPORTED_TIMESTAMP_FIELD')

    does_table_exist = does_bigquery_table_exist(project_name=project_name, dataset_name=dataset, table_name=table)
    if not does_table_exist:
        schema_json = download_s3_json_object(data_config.get('SCHEMA_FILE').get('BUCKET'), data_config.get('SCHEMA_FILE').get('OBJECT_NAME'))

        new_schema = [x for x in schema_json if not imported_timestamp_field in x.keys()]
        new_schema.append(
            {
                "mode": "NULLABLE",
                "name": imported_timestamp_field,
                "type": "TIMESTAMP"
            }
        )
        create_table_if_not_exist(project_name=project_name, dataset_name=dataset, table_name=table,
                                     json_schema=new_schema)


def current_timestamp_as_string():
    dtobj = datetime.datetime.now(timezone.utc)
    return dtobj.strftime("%Y-%m-%dT%H:%M:%SZ")


def get_and_transform_load_crossref_event_data(**kwargs):
    ti = kwargs["ti"]
    data_config = ti.xcom_pull(
        key="data_config", task_ids="get_data_config"
    )
    state_file_name_key = data_config.get('STATE_FILE').get('OBJECT_NAME')
    state_file_bucket = data_config.get('STATE_FILE').get('BUCKET')

    temp_file_dir = data_config.get('LOCAL_TEMPFILE_DIR')
    Path(temp_file_dir).mkdir(parents=True, exist_ok=True)
    number_of_previous_day_to_process = data_config.get('NUMBER_OF_PREVIOUS_DAYS_TO_PROCESS')
    message_key = data_config.get('MESSAGE_KEY')
    publisher_id = data_config.get('PUBLISHER_ID')
    crossref_event_base_url = data_config.get('CROSSREF_EVENT_BASE_URL')
    event_key = data_config.get('EVENT_KEY')
    imported_timestamp_field = data_config.get('IMPORTED_TIMESTAMP_FIELD')
    dataset = data_config.get('DATASET')
    table = data_config.get('TABLE')

    current_timestamp = current_timestamp_as_string()
    last_run_date = get_last_run_day_from_cloud_storage(bucket=state_file_bucket, object_key=state_file_name_key, number_of_previous_day_to_process=number_of_previous_day_to_process)
    uninserted_rows, uninserted_rows_messages, latest_collected_record_date_as_string = \
        etl_data_return_errors(base_crossref_url=crossref_event_base_url,from_date_collected_as_string=last_run_date, publisher_id=publisher_id,
                               dataset_name=dataset, table_name=table, message_key=message_key, event_key=event_key,
                               imported_timestamp=current_timestamp, imported_timestamp_key=imported_timestamp_field)
    task_run_instance_fullname = get_task_run_instance_fullname(kwargs)
    uninserted_rows_filename = Path.joinpath(Path(temp_file_dir), '_'.join([task_run_instance_fullname, 'uninserted_rows']))
    uninserted_rows_messages_filename = Path.joinpath(Path(temp_file_dir), '_'.join([task_run_instance_fullname, 'uninserted_rows_messages']))

    if len(uninserted_rows) > 0:
        write_json_to_file(uninserted_rows, uninserted_rows_filename)
        write_json_to_file(uninserted_rows_messages, uninserted_rows_messages_filename)
        kwargs["ti"].xcom_push(key="uninserted_rows_filename", value=uninserted_rows_filename)
        kwargs["ti"].xcom_push(key="uninserted_rows_messages_file_name", value=uninserted_rows_messages_filename)
        kwargs["ti"].xcom_push(key="data_completely_loaded", value=False)
    else:
        kwargs["ti"].xcom_push(key="data_completely_loaded", value=True)

    kwargs["ti"].xcom_push(key="latest_collected_record_date_as_string", value=latest_collected_record_date_as_string)

branch_op = BranchPythonOperator(
    task_id='branch_if_uninserted_row_exist',
    python_callable=branch_if_uninserted_row_exist,
    dag=dag)

def get_tempfile_key(file_name:str, object_prefix:str ):
    return '/'.join([object_prefix, file_name])


def write_uninserted_to_object_store_and_cleanup(**kwargs):
    ti = kwargs["ti"]
    uninserted_rows_filename = ti.xcom_pull(
        key="uninserted_row_filename", task_ids="get_and_transform_load_crossref_event_data"
    )
    uninserted_rows_messages_file_name = ti.xcom_pull(
        key="uninserted_rows_messages_file_name", task_ids="get_and_transform_load_crossref_event_data"
    )

    data_config = ti.xcom_pull(
        key="data_config", task_ids="get_data_config"
    )

    temp_file_bucket = data_config.get('TEMP_OBJECT_DIR').get('BUCKET')
    temp_file_object_prefix = data_config.get('TEMP_OBJECT_DIR').get('BUCKET')
    uninserted_rows_filename_key = get_tempfile_key(uninserted_rows_filename.split('/')[-1], temp_file_object_prefix)
    uninserted_rows_messages_file_name_key = get_tempfile_key(uninserted_rows_messages_file_name.split('/')[-1], temp_file_object_prefix)

    if os.path.exists(uninserted_rows_messages_file_name):
        upload_file(file_name=uninserted_rows_messages_file_name, bucket=temp_file_bucket, object_key=uninserted_rows_messages_file_name_key)
        os.remove(uninserted_rows_messages_file_name)
    if os.path.exists(uninserted_rows_filename):
        upload_file(file_name=uninserted_rows_filename, bucket=temp_file_bucket,
                    object_key=uninserted_rows_filename_key)
        os.remove(uninserted_rows_filename)


def log_last_execution(**kwargs):
    ti = kwargs["ti"]
    data_config = ti.xcom_pull(
        key="data_config", task_ids="get_data_config"
    )
    current_time = ti.xcom_pull(
        key="latest_collected_record_date_as_string", task_ids="get_and_transform_load_crossref_event_data"
    )

    state_file_name_key = data_config.get('STATE_FILE').get('OBJECT_NAME')
    state_file_bucket = data_config.get('STATE_FILE').get('BUCKET')
    upload_s3_object(bucket=state_file_bucket, object_key=state_file_name_key, object=current_time)

get_data_config_task = create_python_task(
    dag, "get_data_config", get_data_config, retries=1
)

create_table_if_not_exist_task = create_python_task(
    dag, "create_table_if_not_exist", create_bq_table_if_not_exist, retries=1
)
get_and_transform_crossref_event_data_task = create_python_task(
    dag, "get_and_transform_load_crossref_event_data", get_and_transform_load_crossref_event_data
)
write_uninserted_to_object_store_and_cleanup_task = create_python_task(dag, "write_uninserted_to_object_store_and_cleanup", write_uninserted_to_object_store_and_cleanup)
log_last_execution_task = create_python_task(dag, "log_last_execution", log_last_execution, trigger_rule='one_success')

[log_last_execution_task << write_uninserted_to_object_store_and_cleanup_task , log_last_execution_task] << branch_op << get_and_transform_crossref_event_data_task << create_table_if_not_exist_task << get_data_config_task
