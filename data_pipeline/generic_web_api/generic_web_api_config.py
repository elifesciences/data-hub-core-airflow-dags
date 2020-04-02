import os
from datetime import datetime


from urllib import parse


# pylint: disable=too-many-instance-attributes,too-many-arguments,
class WebApiConfig:
    def __init__(
            self,
            web_api_config: dict,
            gcp_project: str = None,
            imported_timestamp_field_name: str = None
    ):
        self.config_as_dict = web_api_config
        self.gcp_project = (
            gcp_project or
            web_api_config.get("gcpProjectName")
        )
        self.import_timestamp_field_name = (
            imported_timestamp_field_name or
            web_api_config.get(
                "importedTimestampFieldName"
            )
        )
        self.dataset_name = web_api_config.get(
            "datasetName", ""
        )
        self.table_name = web_api_config.get(
            "tableName", ""
        )
        self.table_write_append_enabled = web_api_config.get(
            "tableWriteAppend", False
        )

        self.schema_file_s3_bucket = (
            web_api_config.get("schemaFile", {}).get("bucket")
        )
        self.schema_file_object_name = web_api_config.get(
            "schemaFile", {}
        ).get("objectName")

        self.state_file_bucket_name = web_api_config.get(
            "stateFile", {}).get("bucketName")
        self.state_file_object_name = web_api_config.get(
            "stateFile", {}).get("objectName")
        url_excluding_dynamic_parameters = web_api_config.get(
            "datataUrl"
        ).get("urlExcludingDynamicParameters")
        dynamic_parameters = web_api_config.get(
            "datataUrl"
        ).get("dynamicParameters", {})
        page_number_param = dynamic_parameters.get(
            "page", None
        )
        from_date_param = dynamic_parameters.get(
            "fromDate", None)
        to_date_param= dynamic_parameters.get(
            "toDate", None)
        url_date_format = dynamic_parameters.get(
            "dateFormat", None)
        next_page_cursor = dynamic_parameters.get(
            "nextPageCursor", None
        )
        self.url_composer = DynamicURLComposer(
            url_excluding_dynamic_parameters,
            from_date_param,
            to_date_param,
            url_date_format,
            next_page_cursor,
            page_number_param
        )
        self.items_key_hierarchy_from_response_root_as_list = (
            web_api_config.get("response", {}).get(
                "itemsKeyFromResponseRoot", None
            )
        )
        self.total_item_count_key_hierarchy_from_response_root_as_list = (
            web_api_config.get("response", {}).get(
                "totalItemsCountKeyFromResponseRoot", None
            )
        )
        self.next_page_cursor_key_hierarchy_from_response_root_as_list = (
            web_api_config.get("response", {}).get(
                "nextPageCursorKeyFromResponseRoot", None
            )
        )
        self.item_timestamp_key_hierarchy_from_item_root_as_list = (
            web_api_config.get("response", {}).get(
                "recordTimestamp", {}).get(
                "itemTimestampKeyFromItemRoot", None
            )
        )
        self.item_timestamp_format = (
            web_api_config.get("response", {}).get(
                "recordTimestamp", {}).get(
                "timestampFormat", None
            )
        )


class DynamicURLComposer:
    def __init__(
            self,
            url_excluding_dynamic_parameters: str,
            from_date_param: str = None,
            to_date_param: str = None,
            date_format: str = None,
            next_page_cursor: str = None,
            page_number_param: str = None
    ):
        self.url_excluding_dynamic_parameters = url_excluding_dynamic_parameters
        self.from_date_param = from_date_param
        self.to_date_param = to_date_param
        self.date_format = date_format
        self.next_page_cursor = next_page_cursor
        self.page_number_param = page_number_param

    def get_url(
            self,
            from_date: datetime = None,
            to_date: datetime = None,
            page_number: int = None,
            cursor: str = None
    ):
        param_dict = {
            self.from_date_param: datetime_to_string(from_date, self.date_format),
            self.next_page_cursor: cursor,
            self.to_date_param: datetime_to_string(to_date, self.date_format),
            self.page_number_param: str(page_number)
        }
        url = self.url_excluding_dynamic_parameters
        url_separator = "" if "?" in url else "?"
        params = "&".join(
            ["%s=%s" % (k, parse.quote(str(v))) for k, v in param_dict.items() if v and k]
        )
        return url + url_separator + params


def datetime_to_string(
        datetime_obj: datetime = None,
        datetime_format: str = None
):
    return datetime_obj.strftime(datetime_format) if datetime_obj else None


class WebApiAuthentication:
    def __init__(
            self,
            auth_type: str,
            auth_env_var: str = None,
            auth_file_location: str = None
    ):
        self.authentication_type = auth_type
        self.auth_key = os.getenv(
            auth_env_var,
            read_file_content(
                auth_file_location
            )
        ) if auth_type == 'basic' else None


def read_file_content(file_location: str):
    with open(file_location, 'r') as open_file:
        data = open_file.readlines()
    return data

