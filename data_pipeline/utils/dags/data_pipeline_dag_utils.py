import logging
# import json
from datetime import timedelta
from typing import Optional

import airflow

from data_pipeline.utils.airflow_compat import days_ago


LOGGER = logging.getLogger(__name__)


def get_default_args():
    return {
        "start_date": days_ago(1),
        "retries": 10,
        "retry_delay": timedelta(minutes=1),
        "retry_exponential_backoff": True
    }


def create_dag(
        default_args: Optional[dict] = None,
        catchup: bool = False,
        **kwargs):
    if default_args is None:
        default_args = get_default_args()
    return airflow.DAG(
        default_args=default_args,
        catchup=catchup,
        **kwargs
    )
