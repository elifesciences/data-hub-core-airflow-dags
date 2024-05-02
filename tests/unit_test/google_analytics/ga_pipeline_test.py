from unittest.mock import patch
from datetime import datetime
import pytest
from data_pipeline.google_analytics import ga_pipeline as ga_pipeline_module
from data_pipeline.google_analytics.ga_pipeline import (
    etl_google_analytics_for_date_range,
    transform_response_to_bq_compatible_record
)
from data_pipeline.google_analytics.ga_config import (
    GoogleAnalyticsConfig
)


@pytest.fixture(name='load_given_json_list_data_from_tempdir_to_bq_mock', autouse=True)
def _load_given_json_list_data_from_tempdir_to_bq_mock():
    with patch.object(
        ga_pipeline_module,
        'load_given_json_list_data_from_tempdir_to_bq'
    ) as mock:
        yield mock


@pytest.fixture(name='mock_update_state')
def _update_state():
    with patch.object(
        ga_pipeline_module, 'update_state'
    ) as mock:
        yield mock


@pytest.fixture(name='mock_google_analytics_client')
def _ga_client():
    with patch.object(
        ga_pipeline_module, 'GoogleAnalyticsClient'
    ) as mock:
        yield mock


@pytest.fixture(name='mock_get_current_timestamp_as_string')
def _get_current_timestamp_as_string():
    with patch.object(
        ga_pipeline_module, 'get_current_timestamp_as_string'
    ) as mock:
        yield mock


GA_CONFIG = {
    'pipelineID': 'pipeline_id',
    'dataset': 'dataset',
    'table': 'table',
    'viewId': 'viewId',
    'dimensions': ['dimension'],
    'metrics': ['metrics'],
    'recordAnnotations': [{
        'recordAnnotationFieldName': 'recordAnnotationFieldName',
        'recordAnnotationValue': 'recordAnnotationValue'
    }],
    'defaultStartDate': '2001-01-01',
    'stateFile': {
        'bucketName': 'bucket_1',
        'objectName': 'object_1'
    }
}


TEST_DATA = {
    'reports': [{
        'data': {
            'rows': [
                {
                    'metrics': [
                        {'values': ['12566', '1172250.0']}
                    ],
                    'dimensions': ['New Visitor']
                }, {
                    'metrics': [
                        {'values': ['12798', '2215033.0']}
                    ],
                    'dimensions': ['Returning Visitor']
                }
            ],
            'maximums': [
                {'values': ['12798', '2215033.0']}
            ],
            'minimums': [
                {'values': ['12566', '1172250.0']}
            ],
            'isDataGolden': True,
            'totals': [{'values': ['25364', '3387283.0']}],
            'rowCount': 2
        },
        'columnHeader': {
            'dimensions': ['ga:userType'],
            'metricHeader': {
                'metricHeaderEntries': [
                    {'type': 'INTEGER', 'name': 'ga:sessions'},
                    {'type': 'TIME', 'name': 'ga:sessionDuration'}
                ]
            }
        }
    }]
}


class TestTransformResponseToBQCompatibleRecords:
    def test_should_return_empty_list_when_input_is_empty(self):
        with pytest.raises(AssertionError):
            list(transform_response_to_bq_compatible_record({}))

    def test_should_transform_response_to_bq_compatible_record(self):

        response = list(transform_response_to_bq_compatible_record(TEST_DATA))

        expected_data = [
            {'ga_userType': 'New Visitor', 'date_range': '0',
             'ga_sessions': '12566', 'ga_sessionDuration': '1172250.0'},
            {'ga_userType': 'Returning Visitor', 'date_range': '0',
             'ga_sessions': '12798', 'ga_sessionDuration': '2215033.0'}
        ]

        assert len(response) == 1
        assert response[0]['records'] == expected_data


class TestETLGA:
    # pylint: disable=too-many-arguments
    def test_should_etl_ga(
        self,
        mock_update_state,
        load_given_json_list_data_from_tempdir_to_bq_mock,
        mock_google_analytics_client,
        mock_get_current_timestamp_as_string
    ):
        mock_get_current_timestamp_as_string.return_value = (
            "2020-02-01"
        )
        mock_google_analytics_client.return_value.get_report.return_value = (
            TEST_DATA
        )
        start_date = datetime.strptime('2020-01-01', '%Y-%m-%d')

        end_date = datetime.strptime('2020-02-01', '%Y-%m-%d')
        ga_config = GoogleAnalyticsConfig.from_dict(
            GA_CONFIG,
            'project_name',
            'timestamp_field_name'
        )
        etl_google_analytics_for_date_range(
            ga_config,
            start_date=start_date,
            end_date=end_date
        )

        load_given_json_list_data_from_tempdir_to_bq_mock.assert_called()
        mock_update_state.assert_called()
