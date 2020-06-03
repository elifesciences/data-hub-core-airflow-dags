from unittest.mock import patch
import pytest
from data_pipeline.utils.csv import (
    metadata_schema as common_csv_util_module
)

from data_pipeline.utils.csv.metadata_schema import (
    get_record_metadata_schema,
    extend_nested_table_schema_if_new_fields_exist,
)

from data_pipeline.utils import (
    update_deployment_env_placeholder
)
from data_pipeline.s3_csv_data.s3_csv_config import S3BaseCsvConfig


@pytest.fixture(name="mock_extend_table_schema_with_nested_schema")
def _extend_table_schema_with_nested_schema():
    with patch.object(
            common_csv_util_module,
            "extend_table_schema_with_nested_schema") as mock:
        yield mock


class TestData:

    CONFIG_DICT = {
        "dataPipelineId": "data-pipeline",
        "importedTimestampFieldName": "timestamp",
        "headerLineIndex": 0,
        "datasetName": "{ENV}",
        "tableWriteAppend": "false",
        "objectKeyPattern": [
        ],
        "stateFile": {
            "bucketName": "{ENV}_bucket",
            "objectName": "{ENV}_object_prefix"
        }
    }

    @staticmethod
    def get_config(additional_config: dict = None):
        additional_config = (
            additional_config
            if additional_config else {}
        )
        conf_dict = {
            **TestData.CONFIG_DICT,
            **additional_config
        }

        config = S3BaseCsvConfig(
            conf_dict,
            "gcp_project",
            "deployment_env"
        )
        return config


class TestMetadataSchema:
    def test_should_return_no_metadata_if_config_has_no_metadata_info(self):
        config = TestData.get_config()
        metadata_schema = get_record_metadata_schema(config)
        assert len(metadata_schema) == 0

    def test_should_return_metadata_when_csv_config_has_metadata_info(self):

        metadata = {
            "fixedSheetRecordMetadata": [{
                "metadataSchemaFieldName": "fixed_record_metadata_name",
                "fixedSheetValue": "value"
            }],
            "inSheetRecordMetadata": [{
                "metadataSchemaFieldName": "in_sheet_metadata_name",
                "metadataLineIndex": 1
            }]
        }
        config = TestData.get_config(metadata)

        metadata_schema = get_record_metadata_schema(config)
        expected_schema = [
            {'name': 'fixed_record_metadata_name', 'type': 'STRING'},
            {'name': 'in_sheet_metadata_name', 'type': 'STRING'}
        ]

        assert metadata_schema == expected_schema


def test_should_try_extend_table_schema(
        mock_extend_table_schema_with_nested_schema
):
    metadata = {
        "fixedSheetRecordMetadata": [{
            "metadataSchemaFieldName": "fixed_record_metadata_name",
            "fixedSheetValue": "value"
        }],
        "inSheetRecordMetadata": [{
            "metadataSchemaFieldName": "in_sheet_metadata_name",
            "metadataLineIndex": 1
        }]
    }
    config = TestData.get_config(metadata)
    prov_schema = [
        {
            "type": "RECORD", "name": "provenance",
            "fields":
                [
                    {"name": "bucket_name", "type": "STRING"},
                ]
        }
    ]
    csv_header_list = ["header_1", "header_2"]
    extend_nested_table_schema_if_new_fields_exist(
        csv_header_list,
        config,
        prov_schema
    )
    generated_schema = [
        {"type": "STRING", "name": "header_1"},
        {"type": "STRING", "name": "header_2"},
        {"name": "fixed_record_metadata_name", "type": "STRING"},
        {"name": "in_sheet_metadata_name", "type": "STRING"},
        {"type": "RECORD", "name": "provenance",
         "fields": [{"name": "bucket_name", "type": "STRING"}]}
    ]

    mock_extend_table_schema_with_nested_schema.assert_called_with(
        config.gcp_project,
        config.dataset_name,
        config.table_name,
        generated_schema
    )


def test_should_replace_env_placeholder_in_config_dict():
    deployment_env_placeholder = "{ENV}"
    deployment_env = "ci"
    conf_dict = {
        "dataPipelineId": "pipeline",
        "datasetName": "{ENV}-data",
        "tableWriteAppend": "false",
        "list_of_string": ["{ENV}_a", "dont_update"],
        "stateFile": {
            "nested_layer_2": {
                "bucketName": "{ENV}_b_name",
                "objectName": "{ENV}_obj_prefix"
            }
        }
    }
    updated_dict = update_deployment_env_placeholder(
        conf_dict,
        deployment_env,
        deployment_env_placeholder
    )

    expected_updated_dict = {
        "dataPipelineId": "pipeline",
        "datasetName": "ci-data",
        "tableWriteAppend": "false",
        "list_of_string": ["ci_a", "dont_update"],
        "stateFile": {
            "nested_layer_2": {
                "bucketName": "ci_b_name",
                "objectName": "ci_obj_prefix"
            }
        }
    }
    assert updated_dict == expected_updated_dict
