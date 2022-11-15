import logging
from pathlib import Path
from unittest.mock import patch
from typing import ContextManager, Iterable

import pytest
from py._path.local import LocalPath


@pytest.fixture(scope='session', autouse=True)
def setup_logging():
    logging.basicConfig(level='INFO')
    for name in ['tests', 'dags', 'data_pipeline']:
        logging.getLogger(name).setLevel('DEBUG')


@pytest.fixture()
def temp_dir(tmpdir: LocalPath) -> Path:
    return Path(tmpdir)


@pytest.fixture()
def mock_env() -> Iterable[ContextManager[dict]]:
    with patch('os.environ', {}) as env_dict:
        yield env_dict
