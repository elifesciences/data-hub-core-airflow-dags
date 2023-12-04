import airflow

from dags.s3_csv_import_pipeline import (
    get_dag_id_for_s3_csv_config_dict,
    get_multi_csv_pipeline_config
)
from data_pipeline.s3_csv_data.s3_csv_config_typing import S3CsvConfigDict

from tests.dag_validation_test import (
    dag_should_contain_named_tasks
)


def get_test_csv_pipeline_config_dict() -> S3CsvConfigDict:
    multi_data_config = get_multi_csv_pipeline_config()
    return multi_data_config.s3_csv_config[0]


def test_dag_should_contain_named_tasks(dagbag: airflow.models.dagbag.DagBag):
    s3_csv_config_dict = get_test_csv_pipeline_config_dict()
    dag_should_contain_named_tasks(
        dagbag,
        dag_id=get_dag_id_for_s3_csv_config_dict(s3_csv_config_dict),
        task_list=['csv_etl']
    )
