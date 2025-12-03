import json
import logging
import os
from typing import List, Optional
from urllib.parse import urljoin

import requests
from google.cloud import bigquery

LOGGER = logging.getLogger(__name__)


class AirflowAPI:

    def __init__(self):
        airflow_host = os.getenv('AIRFLOW_HOST')
        airflow_port = os.getenv('AIRFLOW_PORT', '8080')
        self.airflow_url = f'http://{airflow_host}:{airflow_port}'
        self.headers = {'Content-Type': 'application/json', 'Accept': 'application/json'}
        self._jwt_token: Optional[str] = None

    def get_new_jwt_token(self) -> str:
        '''
        Fetch JWT token from /auth/token endpoint.
        No credentials needed if SIMPLE_AUTH_MANAGER_ALL_ADMINS=True
        '''
        url = f'{self.airflow_url}/auth/token'
        LOGGER.info('Fetching JWT token from %s', url)
        response = requests.get(url, timeout=30)
        if not response.ok:
            raise OSError(
                f'Failed to request JWT token url={url}'
                f', status={response.status_code}'
                f', response: {response.text}'
            )
        data = response.json()
        token = data.get('access_token')
        if not token:
            raise ValueError(f'No access_token in response: {data}')
        return token

    def get_jwt_token(self) -> str:
        if not self._jwt_token:
            self._jwt_token = self.get_new_jwt_token()
        return self._jwt_token

    def send_request(
        self,
        url: str,
        method: str = 'GET',
        json_param: Optional[dict] = None,
        timeout: int = 60
    ) -> dict:
        LOGGER.info('Sending %s request to url=%s', method, url)
        resp = requests.request(
            method=method.lower(),
            url=url,
            json=json_param,
            headers={
                **self.headers,
                'Authorization': f'Bearer {self.get_jwt_token()}'
            },
            timeout=timeout
        )
        if not resp.ok:
            raise OSError(
                f'Failed to request url={url}'
                f', method={method}, status={resp.status_code}'
                f', response: {resp.text}'
            )
        return resp.json()

    def unpause_dag(self, dag_id: str) -> None:
        url = f'{self.airflow_url}/api/v2/dags/{dag_id}'
        self.send_request(url, method='PATCH', json_param={'is_paused': False})

    def unpause_and_trigger_dag_and_return_dag_run_id(
        self,
        dag_id: str,
        conf: Optional[dict] = None
    ) -> str:
        self.unpause_dag(dag_id)

        endpoint = f'/api/v2/dags/{dag_id}/dagRuns'
        url = urljoin(self.airflow_url + '/', endpoint)
        payload = {
            'logical_date': None,
            'conf': conf or {},
        }
        data = self.send_request(url, method='POST', json_param=payload)
        dag_run_id = data.get('dag_run_id')
        if not dag_run_id:
            raise ValueError(f'No dag_run_id in response: {data}')
        return dag_run_id

    def dag_state(self, dag_id: str, dag_run_id: str) -> dict:
        url = f'{self.airflow_url}/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}'
        return self.send_request(url, method='GET')

    def is_any_dag_run_queued_or_running(self, dag_id: str) -> bool:
        response = requests.get(
            f'{self.airflow_url}/api/experimental/dags/{dag_id}/dag_runs',
            timeout=10
        )
        dag_runs = json.loads(response.text)
        LOGGER.info('DAG runs response: %r', dag_runs)
        states = [
            dag_run.get('state').lower() in ('running', 'queued')
            for dag_run in dag_runs
        ]
        return any(states)

    def do_all_dag_runs_end_with_success(self, dag_id: str) -> bool:
        response = requests.get(
            f'{self.airflow_url}/api/experimental/dags/{dag_id}/dag_runs',
            timeout=10
        )
        dag_runs = json.loads(response.text)
        LOGGER.info('DAG runs response: %r', dag_runs)
        states = [
            dag_run.get('state').lower() == 'success'
            for dag_run in dag_runs
        ]
        return all(states)

    def get_dag_status(self, dag_id: str, dag_run_id) -> str:
        json_response = self.dag_state(dag_id=dag_id, dag_run_id=dag_run_id)
        LOGGER.info('json_response: %s', json_response)
        state = json_response.get('state')
        assert state is not None
        return state.lower()


def simple_query(project: str, dataset: str, table: str, query: str) \
        -> List[dict]:
    bigquery_client = bigquery.Client(project=project)
    _query = query.format(project=project, dataset=dataset, table=table).strip()
    LOGGER.info('running query:\n%s', _query)
    query_job = bigquery_client.query(_query)
    rows = [dict(row) for row in query_job]
    LOGGER.debug('rows: %s', rows)
    return rows
