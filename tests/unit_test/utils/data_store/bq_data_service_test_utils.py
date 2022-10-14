from typing import Iterable
from unittest.mock import MagicMock


def create_load_given_json_list_data_from_tempdir_to_bq_mock() -> MagicMock:
    mock = MagicMock(name='load_given_json_list_data_from_tempdir_to_bq_mock')
    mock.calls_json_lists = []

    def _mock_side_effect(*_args, json_list: Iterable[dict], **_kwargs):
        consumed_json_list = list(json_list)
        mock.calls_json_lists.append(consumed_json_list)
        mock.latest_call_json_list = consumed_json_list

    mock.side_effect = _mock_side_effect
    return mock
