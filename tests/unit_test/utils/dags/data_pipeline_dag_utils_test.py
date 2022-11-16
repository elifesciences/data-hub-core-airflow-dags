from unittest.mock import MagicMock, patch

import pytest

from data_pipeline.utils.pipeline_config import ConfigKeys
from data_pipeline.utils.dags import data_pipeline_dag_utils
from data_pipeline.utils.dags.data_pipeline_dag_utils import (
    get_suffix_for_config,
    trigger_data_pipeline_dag
)


DAG_ID_1 = 'dag1'
DAG_CONFIG_1: dict = {}


@pytest.fixture(name="mock_simple_trigger_dag")
def _simple_trigger_dag():
    with patch.object(data_pipeline_dag_utils, "simple_trigger_dag") as mock:
        yield mock


class TestGetSuffixForConfig:
    def test_should_return_underscore_with_id(self):
        assert get_suffix_for_config({
            ConfigKeys.DATA_PIPELINE_CONFIG_ID: '123'
        }) == '_123'

    def test_should_fallback_to_empty_string(self):
        assert get_suffix_for_config({}) == ''


class TestTriggerDataPipelineDag:
    def test_should_pass_provided_suffix(self, mock_simple_trigger_dag: MagicMock):
        trigger_data_pipeline_dag(DAG_ID_1, DAG_CONFIG_1, suffix='123')
        mock_simple_trigger_dag.assert_called_with(DAG_ID_1, DAG_CONFIG_1, suffix='123')

    def test_should_pass_suffix_from_config(self, mock_simple_trigger_dag: MagicMock):
        config = {
            ConfigKeys.DATA_PIPELINE_CONFIG_ID: '123'
        }
        trigger_data_pipeline_dag(DAG_ID_1, config)
        mock_simple_trigger_dag.assert_called_with(DAG_ID_1, config, suffix='_123')
