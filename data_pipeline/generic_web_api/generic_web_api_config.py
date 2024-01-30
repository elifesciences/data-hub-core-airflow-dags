import os
from dataclasses import dataclass, field, replace
from typing import Mapping, Optional, Sequence, cast

from google.cloud.bigquery import WriteDisposition

from data_pipeline.generic_web_api.request_builder import (
    get_web_api_request_builder_class,
    WebApiDynamicRequestBuilder
)
from data_pipeline.generic_web_api.web_api_auth import WebApiAuthentication
from data_pipeline.utils.pipeline_config import (
    BigQueryIncludeExcludeSourceConfig,
    ConfigKeys,
    MappingConfig,
    get_resolved_parameter_values_from_file_path_env_name,
    update_deployment_env_placeholder
)
from data_pipeline.generic_web_api.generic_web_api_config_typing import (
    MultiWebApiConfigDict,
    ParameterFromEnvConfigDict,
    WebApiBaseConfigDict,
    WebApiConfigDict,
    WebApiConfigurableParametersConfigDict,
    WebApiRequestBuilderConfigDict,
    WebApiResponseConfigDict
)
from data_pipeline.utils.record_processing import RecordProcessingStepFunction
from data_pipeline.utils.record_processing_functions import (
    get_single_record_processing_step_function_for_function_names,
    identity_record_processing_step_function
)


def get_resolved_parameter_values_from_env_name(
    parameters_from_env_name: Sequence[ParameterFromEnvConfigDict]
):
    # Note: this functionality doesn't seem to be used anymore
    #       instead we are using get_resolved_parameter_values_from_file_path_env_name
    params = {
        param.get("parameterName"): os.environ[param["envName"]]
        for param in parameters_from_env_name
        if os.getenv(param["envName"])
    }
    return params


def get_web_api_config_id(
    web_api_config_props: WebApiBaseConfigDict,
    index: int
) -> str:
    web_api_config_id: Optional[str] = (
        cast(Optional[str], web_api_config_props.get(ConfigKeys.DATA_PIPELINE_CONFIG_ID))
    )
    if not web_api_config_id:
        table_name = web_api_config_props.get('table')
        if table_name:
            web_api_config_id = table_name + '_' + str(index)
        else:
            web_api_config_id = str(index)
    return web_api_config_id


# pylint: disable=too-many-instance-attributes,too-many-arguments,
# pylint: disable=too-many-locals
class MultiWebApiConfig:
    def __init__(
        self,
        multi_web_api_etl_config: MultiWebApiConfigDict,
    ):
        self.gcp_project = multi_web_api_etl_config.get("gcpProjectName")
        self.import_timestamp_field_name = multi_web_api_etl_config.get(
            "importedTimestampFieldName"
        )
        self.web_api_config: Mapping[int, WebApiConfigDict] = {
            ind: cast(WebApiConfigDict, {
                **web_api,
                ConfigKeys.DATA_PIPELINE_CONFIG_ID: get_web_api_config_id(web_api, index=ind),
                "gcpProjectName": self.gcp_project,
                "importedTimestampFieldName": self.import_timestamp_field_name
            })
            for ind, web_api in enumerate(
                multi_web_api_etl_config["webApi"]
            )
        }
        self.web_api_config_dict_by_pipeline_id = {
            web_api_config_dict['dataPipelineId']: web_api_config_dict
            for web_api_config_dict in self.web_api_config.values()
        }


@dataclass(frozen=True)
class WebApiResponseConfig:
    items_key_path_from_response_root: Sequence[str] = field(default_factory=list)
    total_item_count_key_path_from_response_root: Sequence[str] = field(default_factory=list)
    next_page_cursor_key_path_from_response_root: Sequence[str] = field(default_factory=list)
    item_timestamp_key_path_from_item_root: Sequence[str] = field(default_factory=list)
    fields_to_return: Optional[Sequence[str]] = None
    record_processing_step_functions: RecordProcessingStepFunction = (
        identity_record_processing_step_function
    )

    @staticmethod
    def from_dict(
        web_api_response_config: Optional[WebApiResponseConfigDict]
    ) -> 'WebApiResponseConfig':
        if not web_api_response_config:
            return WebApiResponseConfig()
        return WebApiResponseConfig(
            items_key_path_from_response_root=(
                web_api_response_config.get("itemsKeyFromResponseRoot", [])
            ),
            total_item_count_key_path_from_response_root=(
                web_api_response_config.get("totalItemsCountKeyFromResponseRoot", [])
            ),
            next_page_cursor_key_path_from_response_root=(
                web_api_response_config.get("nextPageCursorKeyFromResponseRoot", [])
            ),
            item_timestamp_key_path_from_item_root=(
                web_api_response_config.get("recordTimestamp", {})
                .get("itemTimestampKeyFromItemRoot", [])
            ),
            fields_to_return=web_api_response_config.get('fieldsToReturn'),
            record_processing_step_functions=(
                get_single_record_processing_step_function_for_function_names(
                    web_api_response_config.get('recordProcessingSteps')
                )
            )
        )


@dataclass(frozen=True)
class WebApiConfig:
    config_as_dict: WebApiConfigDict
    import_timestamp_field_name: str
    dataset_name: str
    table_name: str
    table_write_disposition: str
    headers: MappingConfig
    dynamic_request_builder: WebApiDynamicRequestBuilder
    gcp_project: str
    response: WebApiResponseConfig
    schema_file_s3_bucket: Optional[str] = None
    schema_file_object_name: Optional[str] = None
    state_file_bucket_name: Optional[str] = None
    state_file_object_name: Optional[str] = None
    default_start_date: Optional[str] = None
    start_to_end_date_diff_in_days: Optional[int] = None
    page_size: Optional[int] = None
    authentication: Optional[WebApiAuthentication] = None
    source: Optional[BigQueryIncludeExcludeSourceConfig] = None
    batch_size: Optional[int] = None

    @staticmethod
    def from_dict(
        web_api_config: WebApiConfigDict,
        deployment_env: Optional[str] = None,
        deployment_env_placeholder: str = "{ENV}"
    ):
        api_config = (
            cast(
                WebApiConfigDict,
                update_deployment_env_placeholder(
                    cast(dict, web_api_config),
                    deployment_env,
                    deployment_env_placeholder
                )
            )
            if deployment_env
            else web_api_config
        )

        data_url_config_dict = api_config["dataUrl"]
        url_excluding_configurable_parameters = (
            data_url_config_dict["urlExcludingConfigurableParameters"]
        )
        configurable_parameters: WebApiConfigurableParametersConfigDict = (
            data_url_config_dict.get("configurableParameters", {})
        )

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
        static_parameters = (
            {
                **(get_resolved_parameter_values_from_env_name(
                    data_url_config_dict.get("parametersFromEnv", [])
                )),
                **(get_resolved_parameter_values_from_file_path_env_name(
                    data_url_config_dict.get("parametersFromFile", [])
                )),
            }
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
        request_builder_config_dict = cast(
            WebApiRequestBuilderConfigDict,
            api_config.get(
                'requestBuilder', api_config.get('urlSourceType', {})
            )
        )
        request_builder_parameters = request_builder_config_dict.get(
            'parameters', request_builder_config_dict.get('sourceTypeSpecificValues', {})
        )
        dynamic_request_builder_class = get_web_api_request_builder_class(
            request_builder_config_dict.get('name', '')
        )

        page_size = (
            configurable_parameters.get("defaultPageSize", None)
        )

        dynamic_request_builder = dynamic_request_builder_class(
            url_excluding_configurable_parameters=url_excluding_configurable_parameters,
            from_date_param=from_date_param,
            to_date_param=to_date_param,
            date_format=url_date_format,
            next_page_cursor=next_page_cursor,
            page_number_param=page_number_param,
            offset_param=offset_param,
            page_size_param=page_size_param,
            page_size=page_size,
            static_parameters=static_parameters,
            sort_key=result_sort_param,
            sort_key_value=result_sort_param_value,
            request_builder_parameters=request_builder_parameters
        )

        auth_type = api_config.get("authentication", {}).get(
            "auth_type", None
        )
        auth_conf_list = api_config.get("authentication", {}).get(
            "orderedAuthenticationParamValues", []
        )
        authentication = WebApiAuthentication(
            auth_type, auth_conf_list
        ) if auth_type and auth_conf_list else None

        return WebApiConfig(
            config_as_dict=api_config,
            gcp_project=api_config["gcpProjectName"],
            import_timestamp_field_name=api_config["importedTimestampFieldName"],
            dataset_name=api_config.get("dataset", ""),
            table_name=api_config.get("table", ""),
            table_write_disposition=(
                WriteDisposition.WRITE_APPEND
                if api_config.get("tableWriteAppend", True)
                else WriteDisposition.WRITE_TRUNCATE
            ),
            schema_file_s3_bucket=(
                api_config.get("schemaFile", {}).get("bucketName")
            ),
            schema_file_object_name=(
                api_config.get("schemaFile", {}).get("objectName")
            ),
            state_file_bucket_name=(
                api_config.get("stateFile", {}).get("bucketName")
            ),
            state_file_object_name=(
                api_config.get("stateFile", {}).get("objectName")
            ),
            headers=MappingConfig.from_dict(api_config.get('headers', {})),
            default_start_date=(
                configurable_parameters.get("defaultStartDate", None)
            ),
            page_size=page_size,
            dynamic_request_builder=dynamic_request_builder,
            start_to_end_date_diff_in_days=(
                configurable_parameters.get("daysDiffFromStartTillEnd", None)
            ),
            response=WebApiResponseConfig.from_dict(api_config.get('response')),
            authentication=authentication,
            source=BigQueryIncludeExcludeSourceConfig.from_optional_dict(api_config.get('source')),
            batch_size=api_config.get('batchSize')
        )

    def _replace(self, **kwargs) -> 'WebApiConfig':
        # Similar method to namedtuple._replace
        return replace(self, **kwargs)
