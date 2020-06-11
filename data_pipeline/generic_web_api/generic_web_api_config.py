from data_pipeline.generic_web_api.url_builder import (
    compose_url_param_from_parameter_values_in_env_var,
    get_url_builder_class
)
from data_pipeline.generic_web_api.web_api_auth import WebApiAuthentication
from data_pipeline.utils.pipeline_config import (
    update_deployment_env_placeholder
)


# pylint: disable=too-many-instance-attributes,too-many-arguments,
# pylint: disable=too-many-locals
class MultiWebApiConfig:
    def __init__(
            self,
            multi_web_api_etl_config: dict,
    ):
        self.gcp_project = multi_web_api_etl_config.get("gcpProjectName")
        self.import_timestamp_field_name = multi_web_api_etl_config.get(
            "importedTimestampFieldName"
        )
        self.web_api_config = {
            ind: {
                **web_api,
                "gcpProjectName": self.gcp_project,
                "importedTimestampFieldName": self.import_timestamp_field_name
            }
            for ind, web_api in enumerate(
                multi_web_api_etl_config.get("webApi")
            )
        }


class WebApiConfig:
    def __init__(
            self,
            web_api_config: dict,
            gcp_project: str = None,
            imported_timestamp_field_name: str = None,
            deployment_env: str = None,
            deployment_env_placeholder: str = "{ENV}"
    ):
        api_config = update_deployment_env_placeholder(
            web_api_config, deployment_env,
            deployment_env_placeholder
        ) if deployment_env else web_api_config
        self.config_as_dict = api_config
        self.gcp_project = (
            gcp_project or
            api_config.get("gcpProjectName")
        )
        self.import_timestamp_field_name = (
            api_config.get(
                "importedTimestampFieldName",
                imported_timestamp_field_name
            )
        )
        self.dataset_name = api_config.get(
            "dataset", ""
        )
        self.table_name = api_config.get(
            "table", ""
        )
        self.table_write_append_enabled = api_config.get(
            "tableWriteAppend", False
        )
        self.schema_file_s3_bucket = (
            api_config.get("schemaFile", {}).get("bucketName")
        )
        self.schema_file_object_name = api_config.get(
            "schemaFile", {}
        ).get("objectName")
        self.state_file_bucket_name = api_config.get(
            "stateFile", {}).get("bucketName")
        self.state_file_object_name = api_config.get(
            "stateFile", {}).get("objectName")
        url_excluding_configurable_parameters = api_config.get(
            "dataUrl"
        ).get("urlExcludingConfigurableParameters")
        configurable_parameters = api_config.get(
            "dataUrl"
        ).get("configurableParameters", {})
        self.default_start_date = configurable_parameters.get(
            "defaultStartDate", None)
        page_number_param = configurable_parameters.get(
            "pageParameterName", None
        )
        offset_param = configurable_parameters.get(
            "offsetParameterName", None
        )
        page_size_param = configurable_parameters.get(
            "pageSizeParameterName", None
        )
        result_sort_param = configurable_parameters.get(
            "resultSortParameterName", None
        )
        result_sort_param_value = configurable_parameters.get(
            "resultSortParameterValue", None
        )
        compose_able_static_parameters = (
            compose_url_param_from_parameter_values_in_env_var(
                api_config.get(
                    "dataUrl"
                ).get("parametersFromEnv", [])
            )
        )
        self.default_start_date = configurable_parameters.get(
            "defaultStartDate", None)
        self.page_size = configurable_parameters.get(
            "defaultPageSize", None
        )
        from_date_param = configurable_parameters.get(
            "fromDateParameterName", None)
        to_date_param = configurable_parameters.get(
            "toDateParameterName", None)
        url_date_format = configurable_parameters.get(
            "dateFormat", None)
        next_page_cursor = configurable_parameters.get(
            "nextPageCursorParameterName", None
        )
        type_specific_param = api_config.get(
            "urlSourceType", {}
        ).get(
            'sourceTypeSpecificValues', {}
        )
        dynamic_url_builder = get_url_builder_class(
            api_config.get(
                "urlSourceType", {}
            ).get(
                'name', ''
            )
        )
        self.url_builder = dynamic_url_builder(
            url_excluding_configurable_parameters,
            from_date_param,
            to_date_param,
            url_date_format,
            next_page_cursor,
            page_number_param,
            offset_param,
            page_size_param,
            self.page_size,
            compose_able_static_parameters,
            result_sort_param,
            result_sort_param_value,
            **type_specific_param
        )
        self.items_key_path_from_response_root = [
            ResponsePathKey(item)
            for item in api_config.get("response", {}).get(
                "itemsKeyFromResponseRoot", [])
        ]
        self.total_item_count_key_path_from_response_root = (
            api_config.get("response", {}).get(
                "totalItemsCountKeyFromResponseRoot", None
            )
        )
        self.next_page_cursor_key_path_from_response_root = (
            api_config.get("response", {}).get(
                "nextPageCursorKeyFromResponseRoot", None
            )
        )
        self.item_timestamp_key_path_from_item_root = [
            ResponsePathKey(item)
            for item in api_config.get("response", {}).get(
                "recordTimestamp", {}).get(
                    "itemTimestampKeyFromItemRoot", []
                )
        ]
        self.item_timestamp_format = (
            api_config.get("response", {}).get(
                "recordTimestamp", {}).get(
                    "timestampFormat", None)
        )
        auth_type = api_config.get("authentication", {}).get(
            "auth_type", None
        )
        auth_conf_list = api_config.get("authentication", {}).get(
            "orderedAuthenticationParamValues", []
        )
        self.authentication = WebApiAuthentication(
            auth_type, auth_conf_list
        ) if auth_type and auth_conf_list else None


class ResponsePathKey:
    def __init__(self, path_level: str):
        self.key = path_level if isinstance(path_level, str) else None
        self.is_variable = (
            True if isinstance(path_level, dict) and
            path_level.get('isVariable')
            else None
        )
