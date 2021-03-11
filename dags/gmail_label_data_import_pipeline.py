# Note: DagBag.process_file skips files without "airflow" or "DAG" in them

import logging
from datetime import timedelta

from data_pipeline.utils.dags.data_pipeline_dag_utils import (create_dag, create_python_task)

from data_pipeline.gmail_production_data.get_gmail_data import (
    connect_to_email,
    get_label_list,
    write_dataframe_to_file,
)

LOGGER = logging.getLogger(__name__)
DAG_ID = "Get_Gmail_Data"

USER_ID = 'production@elifesciences.org'
SERVICE = connect_to_email(USER_ID)

TARGET_FILE_LABEL = 'DELETE/label_list.csv'
TARGET_FILE_THREAD_MSG_LINK = 'DELETE/thread_message_link.csv'

GMAIL_GET_DATA_DAG = create_dag(
    dag_id=DAG_ID,
    schedule_interval=None,
    dagrun_timeout=timedelta(days=1)
)


def gmail_label_data_etl(**kwargs):
    write_dataframe_to_file(get_label_list(SERVICE, USER_ID), TARGET_FILE_LABEL)


gmail_label_data_etl_task = create_python_task(
    GMAIL_GET_DATA_DAG,
    "gmail_label_data_etl",
    gmail_label_data_etl,
    retries=5
)


(
    gmail_label_data_etl_task
)
