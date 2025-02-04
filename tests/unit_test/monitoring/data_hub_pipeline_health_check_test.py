from datetime import datetime
import pandas as pd

from data_pipeline.monitoring.data_hub_pipeline_health_check import (
    get_formatted_out_dated_status_slack_message,
    DEPLOYMENT_ENV_ENV_NAME
)


TIMESTAMP_1 = datetime.fromisoformat('2001-02-03T04:05:06')


class TestGetFormattedOutDatedStatusSlackMessage:
    def test_should_format_out_of_date_slack_message(self, mock_env: dict):
        mock_env[DEPLOYMENT_ENV_ENV_NAME] = 'test'
        slack_message = get_formatted_out_dated_status_slack_message(
            pd.DataFrame([{
                'name': 'Name 1',
                'status': 'Status 1',
                'latest_imported_timestamp': TIMESTAMP_1
            }])
        )
        assert slack_message == (
            '[test] Data pipeline out dated tables:'
            ' `Name 1` is `Status 1` since `2001-02-03 04:05:06`.'
        )
