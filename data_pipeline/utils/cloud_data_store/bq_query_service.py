import logging
from typing import List

from google.cloud import bigquery


LOGGER = logging.getLogger(__name__)


class BqQuery:
    def __init__(
            self,
            bigquery_client: bigquery.Client,
            project: str,
            dataset: str):
        self.bigquery_client = bigquery_client
        self.project = project
        self.dataset = dataset

    def simple_query(self, query: str) -> List[dict]:
        _query = query.format(project=self.project, dataset=self.dataset).strip()
        LOGGER.debug("running query:\n%s", _query)
        query_job = self.bigquery_client.query(_query)
        rows = [dict(row) for row in query_job]
        LOGGER.debug("rows: %s", rows)
        return rows

