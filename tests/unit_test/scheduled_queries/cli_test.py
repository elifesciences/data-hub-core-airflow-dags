from unittest.mock import patch, MagicMock

import pytest

from data_pipeline.scheduled_queries import cli
from data_pipeline.scheduled_queries.cli import (
    main
)


@pytest.fixture(name='get_multi_scheduled_queries_pipeline_config_mock', autouse=True)
def _get_multi_scheduled_queries_pipeline_config_mock():
    with patch.object(cli, 'get_multi_scheduled_queries_pipeline_config') as mock:
        yield mock


class TestMain:
    def test_should_load_config(
        self,
        get_multi_scheduled_queries_pipeline_config_mock: MagicMock
    ):
        main()
        get_multi_scheduled_queries_pipeline_config_mock.assert_called()
