import os
import logging
from pathlib import Path

import pandas as pd
import requests

import boto3

# import data_hub_notifier.configure_warnings  # noqa pylint: disable=unused-import

LOGGER = logging.getLogger(__name__)


def read_big_query(query: str, **kwargs):
    return pd.read_gbq(
        query,
        dialect='standard',
        progress_bar_type=None,
        **kwargs
    )


def get_query(
        project_id: str,
        dataset: str,
        table_name: str):
    return 'SELECT * FROM {project_id}.{dataset}.{table_name}'.format(
        project_id=project_id,
        dataset=dataset,
        table_name=table_name
    )


def read_state_df(status_file: str) -> pd.DataFrame:
    if not os.path.exists(status_file):
        return None
    return pd.read_csv(status_file, sep='\t')


def save_state_df(status_df: pd.DataFrame, status_file: str):
    status_file_path = Path(status_file)
    status_file_path.parent.mkdir(parents=True, exist_ok=True)  # pylint: disable=no-member
    status_df.to_csv(status_file_path, sep='\t', index=False)


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
    webhook_url = os.environ['DATA_HUB_NOTIFIER_SLACK_WEBHOOK_URL']
    response = requests.post(webhook_url, json={
        'text': message
    })
    response.raise_for_status()


def get_formatted_changed_status_slack_message(  # pylint: disable=invalid-name
        changed_status_df: pd.DataFrame) -> str:
    return 'Data availability status update: %s' % ', '.join([
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


def run(
        project_id: str,
        dataset: str,
        table_name: str,
        status_file: str):
    query = get_query(
        project_id=project_id,
        dataset=dataset,
        table_name=table_name
    )
    previous_status_df = read_state_df(status_file=status_file)
    status_df = read_big_query(query, project_id=project_id)
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
    save_state_df(status_df, status_file=status_file)


def upload_s3_object(bucket: str, object_key: str, data_object) -> bool:
    s3_client = boto3.client("s3")
    s3_client.put_object(Body=data_object, Bucket=bucket, Key=object_key)
    return True


def main():
    run(
        project_id='elife-data-pipeline',
        dataset='de_proto',
        table_name='v_tech_data_status',
        status_file='./data/status.tsv'
    )
