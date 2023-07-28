import logging
from unittest.mock import patch
from typing import Iterable
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
