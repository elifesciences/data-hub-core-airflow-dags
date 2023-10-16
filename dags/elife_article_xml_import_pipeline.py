# Note: DagBag.process_file skips files without "airflow" or "DAG" in them

import logging
from typing import Sequence

from data_pipeline.elife_article_xml.elife_article_xml_config import (
    ElifeArticleXmlConfig
)
from data_pipeline.elife_article_xml.elife_article_xml_pipeline import (
    fetch_elife_article_data_and_load_into_bq_from_config_list
)
from data_pipeline.utils.pipeline_config import (
    get_environment_variable_value,
    get_pipeline_config_for_env_name_and_config_parser
)

from data_pipeline.utils.dags.data_pipeline_dag_utils import (
    create_dag,
    create_python_task
)


class ElifeArticleXmlEnvironmentVariables:
    CONFIG_FILE_PATH = 'ELIFE_ARTICLE_XML_CONFIG_FILE_PATH'
    SCHEDULE_INTERVAL = 'ELIFE_ARTICLE_XML_PIPELINE_SCHEDULE_INTERVAL'


DAG_ID = 'Elife_Article_Xml_Pipeline'


LOGGER = logging.getLogger(__name__)


def get_pipeline_config_list() -> Sequence[ElifeArticleXmlConfig]:
    return get_pipeline_config_for_env_name_and_config_parser(
        ElifeArticleXmlEnvironmentVariables.CONFIG_FILE_PATH,
        ElifeArticleXmlConfig.parse_config_list_from_dict
    )


def fetch_elife_article_data_and_load_into_bigquery_task(**_kwargs):
    fetch_elife_article_data_and_load_into_bq_from_config_list(
        get_pipeline_config_list()
    )


ARTICLE_XML_DAG = create_dag(
    dag_id=DAG_ID,
    schedule=get_environment_variable_value(
        ElifeArticleXmlEnvironmentVariables.SCHEDULE_INTERVAL,
        default_value=None
    )
)

create_python_task(
    ARTICLE_XML_DAG,
    "fetch_elife_article_data_and_load_into_bigquery_task",
    fetch_elife_article_data_and_load_into_bigquery_task,
    retries=5
)
