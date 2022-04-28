import os
import logging
from io import StringIO
import pandas as pd
import requests

import boto3


DEPLOYMENT_ENV_ENV_NAME = "DEPLOYMENT_ENV"
DEFAULT_DEPLOYMENT_ENV = "staging"

DATA_HUB_MONITORING_SLACK_WEBHOOK_URL_ENV_NAME = "DATA_HUB_MONITORING_SLACK_WEBHOOK_URL"

LOGGER = logging.getLogger(__name__)


def read_big_query(query: str, **kwargs):
    return pd.read_gbq(
        query,
        dialect='standard',
        progress_bar_type=None,
        **kwargs
    )


def get_query(
        project: str,
        dataset: str,
        table: str):
    return f'SELECT * FROM {project}.{dataset}.{table}'


def read_dataframe_from_s3_bucket(
    bucket_name: str,
    object_name: str
) -> pd.DataFrame:
    return pd.read_csv(f's3://{bucket_name}/{object_name}')


def write_dataframe_to_s3_bucket(
    df_name: pd.DataFrame,
    bucket_name: str,
    object_name: str
):
    csv_buffer = StringIO()
    df_name.to_csv(csv_buffer, index=False)
    s3_resource = boto3.resource('s3')
    s3_resource.Object(bucket_name, object_name).put(Body=csv_buffer.getvalue())
    csv_buffer.truncate(0)


def get_out_dated_status(
        status_df: pd.DataFrame) -> pd.DataFrame:
    status_df = status_df.dropna()
    return status_df[
        (status_df['status'] == 'Out of date')
    ]


def send_slack_message(message: str, webhook_url: str):
    LOGGER.info('slack webhook url: %s', webhook_url)
    response = requests.post(webhook_url, json={
        'text': message
    })
    response.raise_for_status()


def get_formatted_out_dated_status_slack_message(  # pylint: disable=invalid-name
        out_dated_status_df: pd.DataFrame) -> str:
    deployment_env = os.getenv(
        DEPLOYMENT_ENV_ENV_NAME,
        DEFAULT_DEPLOYMENT_ENV
    )
    return f'[{deployment_env}] Data pipeline out dated tables: %s' % '\n '.join([
        f"`{row['name']}` is `{row['status']}` since `{row['latest_imported_timestamp']}`."
        for row in out_dated_status_df.to_dict(orient='rows')
    ])


def get_formatted_message_not_exist_out_dated_table() -> str:
    deployment_env = os.getenv(
        DEPLOYMENT_ENV_ENV_NAME,
        DEFAULT_DEPLOYMENT_ENV
    )
    return f'[{deployment_env}] All tables are `up to date`! '


def send_slack_notification(
        out_dated_status_df: pd.DataFrame, webhook_url: str):
    if len(out_dated_status_df) > 0:
        send_slack_message(
            get_formatted_out_dated_status_slack_message(out_dated_status_df),
            webhook_url
        )
    else:
        send_slack_message(
            get_formatted_message_not_exist_out_dated_table(),
            webhook_url
        )


def run_data_hub_pipeline_health_check(
        project: str,
        dataset: str,
        table: str
):
    query = get_query(
        project=project,
        dataset=dataset,
        table=table
    )

    status_df = read_big_query(query, project_id=project)
    webhook_url = os.getenv(
        DATA_HUB_MONITORING_SLACK_WEBHOOK_URL_ENV_NAME
    )
    with pd.option_context("display.max_rows", 100, "display.max_columns", 10):
        LOGGER.info('status_df:\n%s', status_df)
        out_dated_status_df = get_out_dated_status(status_df)
        if len(out_dated_status_df) > 0:  # pylint: disable=len-as-condition
            LOGGER.info('changed: %s', out_dated_status_df.to_dict(orient='rows'))
            if webhook_url:
                send_slack_notification(out_dated_status_df, webhook_url)
        else:
            LOGGER.info('no changes')
            if webhook_url:
                send_slack_notification(out_dated_status_df, webhook_url)
