import os
import json
from datetime import datetime
from urllib import parse

from data_pipeline.utils.data_pipeline_timestamp import datetime_to_string


def compose_url_param_from_parameter_values_in_env_var(
        compose_able_static_parameters: list):
    params = {
        param.get("parameterName"): os.getenv(param.get("envName"))
        for param in compose_able_static_parameters
        if os.getenv(param.get("envName"))
    }
    return params


class UrlComposeParam:
    # pylint: disable=too-many-arguments
    def __init__(
            self,
            from_date: datetime = None,
            to_date: datetime = None,
            page_number: int = None,
            cursor: str = None,
            page_size: int = None,
            page_offset: int = None
    ):
        self.from_date = from_date
        self.to_date = to_date
        self.page_number = page_number
        self.cursor = cursor
        self.page_size = page_size
        self.page_offset = page_offset


# pylint: disable=too-many-instance-attributes,too-many-arguments
class DynamicURLBuilder:
    def __init__(
            self,
            url_excluding_configurable_parameters: str,
            from_date_param: str = None,
            to_date_param: str = None,
            date_format: str = None,
            next_page_cursor: str = None,
            page_number_param: str = None,
            offset_param: str = None,
            page_size_param: str = None,
            page_size: int = None,
            compose_able_url_key_val: dict = None,
            **kwargs
    ):
        self.url_excluding_configurable_parameters = (
            url_excluding_configurable_parameters
        )
        self.from_date_param = from_date_param
        self.to_date_param = to_date_param
        self.date_format = date_format
        self.next_page_cursor = next_page_cursor
        self.page_number_param = page_number_param
        self.offset_param = offset_param
        self.page_size_param = page_size_param
        self.page_size = page_size
        self.compose_able_url_key_val = compose_able_url_key_val
        self.type_specific_params = kwargs

    def _get_url_separator(self):
        url = self.url_excluding_configurable_parameters
        if "?" in url:
            if url.strip().endswith("&") or url.strip().endswith("?"):
                url_separator = ""
            else:
                url_separator = "&"
        else:
            url_separator = "?"
        return url_separator

    def compose_url(self, parameters_key_value: dict):
        url = self.url_excluding_configurable_parameters
        url_separator = self._get_url_separator()
        params = parse.urlencode(
            {
                key: value
                for key, value in parameters_key_value.items() if key and value
            }
        )

        return url + url_separator + params

    def get_url(
            self,
            url_compose_param: UrlComposeParam,
    ):
        start_date = datetime_to_string(
            url_compose_param.from_date, self.date_format
        )
        end_date = datetime_to_string(
            url_compose_param.to_date, self.date_format
        )
        param_dict = dict((key, value) for key, value in [
            (self.from_date_param, start_date),
            (self.next_page_cursor, url_compose_param.cursor),
            (self.to_date_param, end_date),
            (self.page_number_param, url_compose_param.page_number),
            (self.offset_param, url_compose_param.page_offset),
            (
                self.page_size_param,
                url_compose_param.page_size or self.page_size
            )
            ] if key and value)
        param_dict = {
            **param_dict,
            **self.compose_able_url_key_val
        }

        return self.compose_url(param_dict)


def get_url_builder_class(url_source_type: str = None):
    url_builder = DynamicURLBuilder

    if url_source_type.strip().lower() == 'civi':
        url_builder = DynamicCiviURLBuilder
    return url_builder


class DynamicCiviURLBuilder(DynamicURLBuilder):

    def get_url(
            self,
            url_compose_param: UrlComposeParam,
    ):
        start_date = datetime_to_string(
            url_compose_param.from_date, self.date_format
        )
        options = dict((key, value) for key, value in [
            (self.offset_param, url_compose_param.page_offset),
            (
                self.page_size_param,
                url_compose_param.page_size or self.page_size
            )
            ] if key and value)
        start_date_param = {
            self.from_date_param: {">=": start_date}
        } if start_date else {}
        field_to_return_param = self.get_fields_to_return()
        url_query_json_arg = {
            "sequential": 1,
            **start_date_param,
            **field_to_return_param,
            "options": options
        }
        url_query_json_arg_as_str = json.dumps(
            url_query_json_arg
        )
        param_dict = {
            **self.compose_able_url_key_val
        }
        url_no_options = self.compose_url(param_dict)
        return url_no_options + "&json=" + url_query_json_arg_as_str

    def get_fields_to_return(self):
        field_to_return_param = {}
        field_to_return_list = self.type_specific_params.get(
            "fieldsToReturn")
        if field_to_return_list:
            field_to_return_param = {
                "return": ",".join(field_to_return_list)
            }
        return field_to_return_param
