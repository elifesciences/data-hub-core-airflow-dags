"""
A maintenance workflow that you can deploy into Airflow to periodically clean
out the DagRun, TaskInstance, Log, XCom, Job DB and SlaMiss entries, as well
 as task logs to avoid having too much data in your Airflow MetaStore and disc.
airflow trigger_dag --conf '{"maxDataAgeInDays":30}' airflow-log-cleanup
--conf options:
    maxDataAgeInDays:<INT> - Optional
"""
import os
import logging
from datetime import datetime, timedelta
import dateutil.parser
import airflow
from airflow.models import (
    DAG, DagRun, TaskInstance, Log, XCom, SlaMiss,
    DagModel, Variable
)
from airflow.utils import timezone
from airflow.jobs import BaseJob
from airflow import settings
from airflow.operators.python_operator import PythonOperator
from sqlalchemy import func, and_
from sqlalchemy.orm import load_only


DAG_ID = "Airflow_DB_Maintenance"
START_DATE = airflow.utils.dates.days_ago(1)
SCHEDULE_INTERVAL = "@weekly"
# Whether the job should delete the db entries or not. Included if you want to
# temporarily avoid deleting the db entries.
ENABLE_DELETE = True
# List of all the objects that will be deleted. Comment out the DB objects you
# want to skip.
DATABASE_OBJECTS = [
    {
        "airflow_db_model": DagRun,
        "age_check_column": DagRun.execution_date,
        "keep_last": True,
        "keep_last_filters": [DagRun.external_trigger is False],
        "keep_last_group_by": DagRun.dag_id},
    {
        "airflow_db_model": TaskInstance,
        "age_check_column": TaskInstance.execution_date,
        "keep_last": False,
        "keep_last_filters": None,
        "keep_last_group_by": None
    },
    {
        "airflow_db_model": Log,
        "age_check_column": Log.dttm,
        "keep_last": False,
        "keep_last_filters": None,
        "keep_last_group_by": None
    },
    {
        "airflow_db_model": XCom,
        "age_check_column": XCom.execution_date,
        "keep_last": False,
        "keep_last_filters": None,
        "keep_last_group_by": None
    },
    {
        "airflow_db_model": BaseJob,
        "age_check_column": BaseJob.latest_heartbeat,
        "keep_last": False,
        "keep_last_filters": None,
        "keep_last_group_by": None
    },
    {
        "airflow_db_model": SlaMiss,
        "age_check_column": SlaMiss.execution_date,
        "keep_last": False,
        "keep_last_filters": None,
        "keep_last_group_by": None
    },
    {
        "airflow_db_model": DagModel,
        "age_check_column": DagModel.last_scheduler_run,
        "keep_last": False,
        "keep_last_filters": None,
        "keep_last_group_by": None
    },
]

session = settings.Session()

default_args = {
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'start_date': START_DATE,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    DAG_ID,
    default_args=default_args,
    schedule_interval=SCHEDULE_INTERVAL,
    start_date=START_DATE
)


DEFAULT_MAX_DATA_AGE_IN_DAYS = 1
MAX_CLEANUP_DATA_AGE_NAME = (
    "MAX_CLEANUP_DATA_AGE_IN_DAYS"
)


def get_max_data_cleanup_configuration_function(**context):
    max_data_age_in_days = int(
        Variable.get(
            MAX_CLEANUP_DATA_AGE_NAME,
            os.getenv(
                MAX_CLEANUP_DATA_AGE_NAME,
                DEFAULT_MAX_DATA_AGE_IN_DAYS
            )
        )
    )
    max_date = timezone.utcnow() + timedelta(-max_data_age_in_days)
    context["ti"].xcom_push(key="max_date", value=max_date.isoformat())


get_configuration = PythonOperator(
    task_id='get_configuration',
    python_callable=get_max_data_cleanup_configuration_function,
    provide_context=True,
    dag=dag)


def cleanup_function(**context):

    logging.info("Retrieving max_execution_date from XCom")
    max_date = context["ti"].xcom_pull(
        task_ids=get_configuration.task_id, key="max_date"
    )
    max_date = dateutil.parser.parse(max_date)  # stored as iso8601 str in xcom

    airflow_db_model = context["params"].get("airflow_db_model")
    age_check_column = context["params"].get("age_check_column")
    keep_last = context["params"].get("keep_last")
    keep_last_filters = context["params"].get("keep_last_filters")
    keep_last_group_by = context["params"].get("keep_last_group_by")

    logging.info("Configurations:")
    logging.info("max_date:                 " + str(max_date))

    logging.info("Running Cleanup Process...")
    query = session.query(airflow_db_model).options(
        load_only(age_check_column)
    )
    logging.info("INITIAL QUERY : " + str(query))
    if keep_last:
        subquery = session.query(func.max(DagRun.execution_date))
        if keep_last_filters is not None:
            for entry in keep_last_filters:
                subquery = subquery.filter(entry)

            logging.info("SUB QUERY [keep_last_filters]: " + str(subquery))

        if keep_last_group_by is not None:
            subquery = subquery.group_by(keep_last_group_by)
            logging.info("SUB QUERY [keep_last_group_by]: " + str(subquery))

        subquery = subquery.from_self()

        query = query.filter(
            and_(age_check_column.notin_(subquery)),
            and_(age_check_column <= max_date)
        )

    else:
        query = query.filter(age_check_column <= max_date,)

    entries_to_delete = query.all()

    logging.info("Query: " + str(query))
    logging.info(
        "Process will be Deleting the following " +
        str(airflow_db_model.__name__) + "(s):"
    )
    for entry in entries_to_delete:
        logging.info(
            "\tEntry: " + str(entry) + ", Date: " +
            str(entry.__dict__[str(age_check_column).split(".")[1]])
        )

    logging.info(
        "Process will be Deleting " + str(len(entries_to_delete)) + " " +
        str(airflow_db_model.__name__) + "(s)"
    )

    if ENABLE_DELETE:
        logging.info("Performing Delete...")
        # using bulk delete
        query.delete(synchronize_session=False)
        session.commit()
        logging.info("Finished Performing Delete")
    else:
        logging.warn("You're opted to skip deleting the db entries!!!")

    logging.info("Finished Running Cleanup Process")


for db_object in DATABASE_OBJECTS:

    cleanup_op = PythonOperator(
        task_id='cleanup_' + str(db_object["airflow_db_model"].__name__),
        python_callable=cleanup_function,
        params=db_object,
        provide_context=True,
        dag=dag
    )

    get_configuration.set_downstream(cleanup_op)
