import logging
from unittest.mock import MagicMock, patch
from typing import Iterable, Iterator
import boto3
import pytest


@pytest.fixture(scope='session', autouse=True)
def setup_logging():
    logging.basicConfig(level='INFO')
    for name in ['tests', 'dags', 'data_pipeline']:
        logging.getLogger(name).setLevel('DEBUG')


@pytest.fixture()
def mock_env() -> Iterable[dict]:
    env_dict: dict = {}
    with patch('os.environ', env_dict):
        yield env_dict


@pytest.fixture(name="mock_s3_client_function", autouse=True)
def _mock_s3_client_function() -> Iterator[MagicMock]:
    with patch.object(boto3, "client") as mock:
        yield mock
