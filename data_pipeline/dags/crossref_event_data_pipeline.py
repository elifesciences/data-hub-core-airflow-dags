from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
import logging
from datetime import timedelta
from pathlib import Path
from data_pipeline.utils.load.s3_data_service import upload_file
from data_pipeline.dags.data_pipeline_dag_utils import get_default_args
import os

from data_pipeline.crossref_event_data.extract_crossref_data import write_today, get_crossref_data, get_last_run_day, etl_data_return_errors, write_json_to_file
LOGGER = logging.getLogger(__name__)
DEFAULT_ARGS = get_default_args()

DATASET = 'de_dev'
TABLE = 'crossref_event'
STATE_FILENAME ='/home/michael/date_state' #'/usr/local/airflow/dags/date_state'
TEMP_FILE_DIR = '/home/michael/airflow/tempdir'#'/usr/local/airflow/tempfile'
PUBLISHER_ID = '10.7554' # read publisher id from some config file
TEMP_FILE_EXTENSION = '.file'
CROSSREF_EVENT_BASE_URL = "https://api.eventdata.crossref.org/v1/events?rows=10000"

MESSAGE_KEY = 'message'
EVENT_KEY = 'events'
DEFAULT_NUMBER_OF_PREVIOUS_DAYS_TO_PROCESS = 44

S3_BUCKET =''
S3_OBJECT_PREFIX=''


Path(TEMP_FILE_DIR).mkdir(parents=True, exist_ok=True)

dag = DAG(
    dag_id="Load_Crossref_Event_Into_Bigquery",
    default_args=DEFAULT_ARGS,
    schedule_interval='@once',#'"@hourly",
    dagrun_timeout=timedelta(minutes=30),
)




def create_python_task(dag, task_id, python_callable, trigger_rule='all_success'):
    return PythonOperator(task_id=task_id, dag=dag, python_callable=python_callable, trigger_rule=trigger_rule)

def get_task_run_instance_fullname(task_context):
    return '___'.join ([task_context.get('dag').dag_id, task_context.get('run_id'), task_context.get('task').task_id])

def branch_func(**kwargs):
    ti = kwargs["ti"]
    data_completely_loaded = bool(ti.xcom_pull(
        key="data_completely_loaded", task_ids="get_and_transform_load_crossref_event_data"
    ))
    if data_completely_loaded:
        return 'log_last_execution'
    else:
        return 'write_uninserted_to_object_store_and_cleanup'


def get_and_transform_load_crossref_event_data(**kwargs):

    last_run_date = get_last_run_day(file_name=STATE_FILENAME, default_number_of_previous_day_to_process=DEFAULT_NUMBER_OF_PREVIOUS_DAYS_TO_PROCESS)
    uninserted_rows, uninserted_rows_messages = \
        etl_data_return_errors(base_crossref_url=CROSSREF_EVENT_BASE_URL,from_date_collected_as_string=last_run_date, publisher_id=PUBLISHER_ID,
                               dataset_name=DATASET, table_name=TABLE, message_key=MESSAGE_KEY, event_key=EVENT_KEY)
    task_run_instance_fullname= get_task_run_instance_fullname(kwargs)
    uninserted_rows_filename = Path.joinpath(Path(TEMP_FILE_DIR), '_'.join([task_run_instance_fullname, 'uninserted_rows']))
    uninserted_rows_messages_filename = Path.joinpath(Path(TEMP_FILE_DIR), '_'.join([task_run_instance_fullname, 'uninserted_rows_messages']))

    if len(uninserted_rows)>0:
        write_json_to_file(uninserted_rows, uninserted_rows_filename)
        write_json_to_file(uninserted_rows_messages, uninserted_rows_messages_filename)
        kwargs["ti"].xcom_push(key="uninserted_rows_filename", value=uninserted_rows_filename)
        kwargs["ti"].xcom_push(key="uninserted_rows_messages_file_name", value=uninserted_rows_messages_filename)
        kwargs["ti"].xcom_push(key="data_completely_loaded", value=False)
    else:
        kwargs["ti"].xcom_push(key="data_completely_loaded", value=True)



branch_op = BranchPythonOperator(
    task_id='branch_task',
    provide_context=True,
    python_callable=branch_func,
    #depends_on_past=True,
    dag=dag)



def write_uninserted_to_object_store_and_cleanup(**kwargs):
    ti = kwargs["ti"]
    uninserted_rows_filename = ti.xcom_pull(
        key="uninserted_row_filename", task_ids="get_and_transform_load_crossref_event_data"
    )
    uninserted_rows_messages_file_name = ti.xcom_pull(
        key="uninserted_rows_messages_file_name", task_ids="get_and_transform_load_crossref_event_data"
    )
    if os.path.exists(uninserted_rows_messages_file_name):
        os.remove(uninserted_rows_messages_file_name)
    if os.path.exists(uninserted_rows_filename):
        os.remove(uninserted_rows_filename)


def log_last_execution(**kwargs):
    write_today(STATE_FILENAME)


get_and_transform_crossref_event_data_task = create_python_task(
    dag, "get_and_transform_load_crossref_event_data", get_and_transform_load_crossref_event_data
)
write_uninserted_to_object_store_and_cleanup = create_python_task(dag, "write_uninserted_to_object_store_and_cleanup", write_uninserted_to_object_store_and_cleanup)
log_last_execution_task = create_python_task(dag, "log_last_execution", log_last_execution, trigger_rule='one_success')
#log_last_execution_task2 = create_python_task(dag, "log_last_execution2", log_last_execution)

#dummy_task = DummyOperator(task_id='dummy_task', dag=dag)
#log_last_execution_task << write_uninserted_to_object_store_and_cleanup << branch_op << get_and_transform_crossref_event_data_task
branch_op << get_and_transform_crossref_event_data_task
log_last_execution_task << write_uninserted_to_object_store_and_cleanup << branch_op
log_last_execution_task << branch_op