from unittest.mock import patch
from typing import Iterable

import pytest


@pytest.fixture()
def mock_env() -> Iterable[dict]:
    env_dict: dict = {}
    with patch('os.environ', env_dict):
        yield env_dict
