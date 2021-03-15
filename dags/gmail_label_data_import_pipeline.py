# Note: DagBag.process_file skips files without "airflow" or "DAG" in them

import os
import logging
from datetime import timedelta
from tempfile import TemporaryDirectory
from pathlib import Path

from google.cloud import bigquery

from data_pipeline.utils.data_store.bq_data_service import (
    create_table_if_not_exist,
    load_file_into_bq,
)

from data_pipeline.utils.dags.data_pipeline_dag_utils import (
    create_dag,
    create_python_task
)

from data_pipeline.gmail_production_data.get_gmail_data import (
    connect_to_email,
    get_label_list,
    write_dataframe_to_file,
    get_link_message_thread,
)

# DEPLOYMENT_ENV_ENV_NAME = "DEPLOYMENT_ENV"
# DEFAULT_DEPLOYMENT_ENV_VALUE = "ci"


LOGGER = logging.getLogger(__name__)
DAG_ID = "Get_Gmail_Data"

USER_ID = 'production@elifesciences.org'

TARGET_FILE_LABEL_LIST = 'label_list.csv'
TARGET_FILE_THREAD_MESSAGE_LINK = 'thread_message_link.csv'

PROJECT = 'elife-data-pipeline'
DATASET = 'hc_dev'

TABLE_NAME_LABEL_LIST = 'TEST_gmail_production_label_list'
# TABLE_NAME_THREAD_MESSAGE_LINK = "TEST_gmail_production_link_message_thread"

SCHEMA_LABEL_LIST = [
    {
        "mode": "REQUIRED",
        "name": "labelId",
        "type": "STRING"
    },
    {
        "mode": "REQUIRED",
        "name": "labelName",
        "type": "STRING"
    },
    {
        "mode": "NULLABLE",
        "name": "messageListVisibility",
        "type": "STRING"
    },
    {
        "mode": "NULLABLE",
        "name": "labelListVisibility",
        "type": "STRING"
    },
    {
        "mode": "NULLABLE",
        "name": "labelType",
        "type": "STRING"
    },
    {
        "mode": "NULLABLE",
        "name": "labelColour",
        "type": "STRING"
    }
]

GET_GMAIL_DATA_DAG = create_dag(
    dag_id=DAG_ID,
    schedule_interval=None,
    dagrun_timeout=timedelta(days=1)
)


def create_table_gmail_label_list(**__):
    create_table_if_not_exist(
        project_name=PROJECT,
        dataset_name=DATASET,
        table_name=TABLE_NAME_LABEL_LIST,
        json_schema=SCHEMA_LABEL_LIST
    )


def get_gmail_service():
    return connect_to_email(USER_ID)


def gmail_label_data_etl(**__):
    print('os_environ:', os.environ)
    create_table_gmail_label_list()
    print("Table created ")
    with TemporaryDirectory() as tmp_dir:
        filename = Path(tmp_dir)/TARGET_FILE_LABEL_LIST
        write_dataframe_to_file(get_label_list(get_gmail_service(), USER_ID), filename)
        print("File created ")
        load_file_into_bq(
            filename=filename,
            dataset_name=DATASET,
            table_name=TABLE_NAME_LABEL_LIST,
            project_name=PROJECT,
            source_format=bigquery.SourceFormat.CSV
            )
        print("Table loaded")


def gmail_thread_message_link_etl(**__):
    with TemporaryDirectory() as tmp_dir:
        target_file = Path(tmp_dir)/TARGET_FILE_THREAD_MESSAGE_LINK
        write_dataframe_to_file(
            get_link_message_thread(
                get_gmail_service(),
                USER_ID
                ),
            target_file
        )


gmail_label_data_etl_task = create_python_task(
    GET_GMAIL_DATA_DAG,
    "gmail_label_data_etl",
    gmail_label_data_etl,
    retries=5
)

gmail_thread_message_link_etl_task = create_python_task(
    GET_GMAIL_DATA_DAG,
    "gmail_thread_message_link_etl",
    gmail_thread_message_link_etl,
    retries=5
)


# pylint: disable=pointless-statement
(
    gmail_label_data_etl_task
    >> gmail_thread_message_link_etl_task
)
