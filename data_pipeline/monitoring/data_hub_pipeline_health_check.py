import os
import logging
from io import StringIO
import pandas as pd
import requests

import boto3


DEPLOYMENT_ENV_ENV_NAME = "DEPLOYMENT_ENV"
DEFAULT_DEPLOYMENT_ENV = "staging"

DATA_HUB_MONITORING_SLACK_WEBHOOK_URL_ENV_NAME = "DATA_HUB_MONITORING_SLACK_WEBHOOK_URL"
DEFAULT_DATA_HUB_MONITORING_SLACK_WEBHOOK_URL = (
    "https://hooks.slack.com/services/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"
)


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
    return 'SELECT * FROM {project}.{dataset}.{table}'.format(
        project=project,
        dataset=dataset,
        table=table
    )


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


def get_merged_status_df(
        previous_status_df: pd.DataFrame,
        current_status_df: pd.DataFrame) -> pd.DataFrame:
    return current_status_df.merge(
        previous_status_df,
        on='name',
        how='outer',
        suffixes=('_current', '_previous')
    )


def get_changed_status_df(
        merged_status_df: pd.DataFrame) -> pd.DataFrame:
    merged_status_df = merged_status_df.dropna()
    return merged_status_df[
        (merged_status_df['status_current'] != merged_status_df['status_previous'])
    ]


def send_slack_message(message: str):
    webhook_url = os.getenv(
        DATA_HUB_MONITORING_SLACK_WEBHOOK_URL_ENV_NAME,
        DEFAULT_DATA_HUB_MONITORING_SLACK_WEBHOOK_URL
    )
    LOGGER.info('slack webhook url: %s', webhook_url)
    response = requests.post(webhook_url, json={
        'text': message
    })
    response.raise_for_status()


def get_formatted_changed_status_slack_message(  # pylint: disable=invalid-name
        changed_status_df: pd.DataFrame) -> str:
    deployment_env = os.getenv(
        DEPLOYMENT_ENV_ENV_NAME,
        DEFAULT_DEPLOYMENT_ENV
    )
    return f'[{deployment_env}] Data availability status update: %s' % ', '.join([
        '`%s` changed from `%s` to `%s`' % (
            row['name'], row['status_previous'], row['status_current']
        )
        for row in changed_status_df.to_dict(orient='rows')
    ])


def send_slack_notification(
        changed_status_df: pd.DataFrame):
    send_slack_message(get_formatted_changed_status_slack_message(
        changed_status_df
    ))


def run_data_hub_pipeline_health_check(
        project: str,
        dataset: str,
        table: str,
        bucket_name: str,
        object_name: str
):

    query = get_query(
        project=project,
        dataset=dataset,
        table=table
    )

    previous_status_df = read_dataframe_from_s3_bucket(
        bucket_name=bucket_name,
        object_name=object_name
    )
    status_df = read_big_query(query, project_id=project)
    with pd.option_context("display.max_rows", 100, "display.max_columns", 10):
        LOGGER.info('status_df:\n%s', status_df)
        if previous_status_df is not None:
            merged_status_df = get_merged_status_df(
                previous_status_df=previous_status_df,
                current_status_df=status_df
            )
            LOGGER.info('merged: %s', merged_status_df.to_dict(orient='rows'))
            changed_status_df = get_changed_status_df(merged_status_df)
            if len(changed_status_df) > 0:  # pylint: disable=len-as-condition
                LOGGER.info('changed: %s', changed_status_df.to_dict(orient='rows'))
                send_slack_notification(changed_status_df)
            else:
                LOGGER.info('no changes')

    write_dataframe_to_s3_bucket(
        df_name=status_df,
        bucket_name=bucket_name,
        object_name=object_name
    )
