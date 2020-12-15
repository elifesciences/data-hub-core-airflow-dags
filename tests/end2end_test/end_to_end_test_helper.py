import json
import logging
import os
import re
from typing import List
from urllib.parse import urljoin

import requests
from google.cloud import bigquery

# pylint: disable=no-else-return
LOGGER = logging.getLogger(__name__)


class AirflowAPI:

    def __init__(self):
        airflow_host = os.getenv("AIRFLOW_HOST")
        airflow_port = os.getenv("AIRFLOW_PORT", "8080")
        self.airflow_url = "http://%s:%s" % (airflow_host, airflow_port)

    # pylint: disable=no-self-use
    def send_request(self, url, method="GET", json_param=None):
        params = {
            "url": url,
        }
        if json_param is not None:
            params["json"] = json_param
        # pylint: disable=not-callable
        resp = getattr(requests, method.lower())(**params)
        if not resp.ok:
            # It is justified here because there might be many resp types.
            # noinspection PyBroadException
            try:
                data = resp.json()
            except Exception:  # pylint: disable=broad-except
                data = {
                    "error": 'failed to request url=%r, method=%s, status=%s, response: %r' % (
                        url, method, resp.status_code, resp.text
                    )
                }
            raise OSError(data.get("error", "Server error"))

        return resp.json()

    def unpause_dag(self, dag_id):
        return requests.get(
            "%s/api/experimental/dags/%s/paused/false" %
            (self.airflow_url, dag_id)
        )

    def pause_dag(self, dag_id):
        return requests.get(
            "%s/api/experimental/dags/%s/paused/true" %
            (self.airflow_url, dag_id)
        )

    def trigger_dag(self, dag_id, conf=None):
        self.unpause_dag(dag_id)
        endpoint = "/api/experimental/dags/{}/dag_runs".format(dag_id)
        url = urljoin(self.airflow_url, endpoint)
        data = self.send_request(url, method="POST",
                                 json_param={"conf": conf, })

        pattern = r"\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d"
        return re.findall(pattern, data["message"])[0]

    def dag_state(self, dag_id, execution_date):
        return requests.get(
            "%s/api/experimental/dags/%s/dag_runs/%s"
            % (self.airflow_url, dag_id, execution_date)
        )

    def is_triggered_dag_running(self, dag_id):
        response = requests.get(
            "%s/api/experimental/dags/%s/dag_runs"
            % (self.airflow_url, dag_id)
        )
        dag_runs = json.loads(response.text)
        states = [
            dag_run.get("state").lower() == "running"
            for dag_run in dag_runs
        ]
        return all(states)

    def is_dag_running(self, dag_id, execution_date):
        if not (execution_date and dag_id):
            return False
        response = self.dag_state(dag_id, execution_date)
        json_response = json.loads(response.text)
        if json_response.get("state").lower() == "running":
            return True
        return False

    def get_dag_status(self, dag_id, execution_date):
        response = self.dag_state(dag_id, execution_date)
        json_response = json.loads(response.text)
        return json_response.get("state").lower()


def simple_query(project: str, dataset: str, table: str, query: str) \
        -> List[dict]:
    bigquery_client = bigquery.Client(project=project)
    _query = \
        query.format(project=project, dataset=dataset, table=table).strip()
    LOGGER.debug("running query:\n%s", _query)
    query_job = bigquery_client.query(_query)
    rows = [dict(row) for row in query_job]
    LOGGER.debug("rows: %s", rows)
    return rows
