from typing import Iterator
from unittest.mock import MagicMock, patch

import pytest


@pytest.fixture(name='discovery_build_mock', autouse=True)
def discovery_build_mock() -> Iterator[MagicMock]:
    with patch('apiclient.discovery.build') as mock:
        yield mock
