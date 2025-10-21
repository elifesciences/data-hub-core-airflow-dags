import logging
import pandas as pd
from google.cloud import bigquery

from data_pipeline.finance_data.finance_data_pipeline_config import (
    FinanceDataPipelineConfig,
    FinanceDataSourceConfig,
    FinanceDataTargetConfig,
)
from data_pipeline.utils.pipeline_utils import get_query
from data_pipeline.utils.s3_utils import write_dataframe_to_s3_bucket

LOGGER = logging.getLogger(__name__)


def read_finance_data_from_bigquery(
    project_name: str,
    dataset: str,
    table: str
) -> pd.DataFrame:
    client = bigquery.Client()
    query = get_query(
        project=project_name,
        dataset=dataset,
        table=table
    )
    LOGGER.info('Executing BigQuery query: %s', query)
    query_job = client.query(query)
    return query_job.to_dataframe()


def fetch_finance_data_from_bigquery_and_write_to_s3(
    source_config: FinanceDataSourceConfig,
    target_config: FinanceDataTargetConfig
):
    df = read_finance_data_from_bigquery(
        project_name=source_config.project_name,
        dataset=source_config.dataset,
        table=source_config.table
    )
    write_dataframe_to_s3_bucket(
        df_name=df,
        bucket=target_config.bucket,
        object_name=target_config.object_name
    )


def fetch_finance_data_from_bigquery_and_write_to_s3_from_config_list(
    configs: list[FinanceDataPipelineConfig]
):
    for config in configs:
        fetch_finance_data_from_bigquery_and_write_to_s3(
            source_config=config.source,
            target_config=config.target
        )
        LOGGER.info(
            'Data pipeline %s completed successfully.',
            config.data_pipeline_id
        )
