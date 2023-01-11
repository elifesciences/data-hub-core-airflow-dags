import logging
import os
import json
from dataclasses import dataclass
from datetime import datetime
from typing import NamedTuple, Optional
from urllib import parse

from data_pipeline.utils.data_pipeline_timestamp import datetime_to_string
from data_pipeline.utils.pipeline_config import (
    get_resolved_parameter_values_from_file_path_env_name
)


LOGGER = logging.getLogger(__name__)


def compose_url_param_from_parameter_values_in_env_var(
        compose_able_static_parameters: list):
    params = {
        param.get("parameterName"): os.getenv(param.get("envName"))
        for param in compose_able_static_parameters
        if os.getenv(param.get("envName"))
    }
    return params


def compose_url_param_from_param_vals_filepath_in_env_var(
        compose_able_static_parameters: list):
    return get_resolved_parameter_values_from_file_path_env_name(
        compose_able_static_parameters
    )


class UrlComposeParam(NamedTuple):
    from_date: Optional[datetime] = None
    to_date: Optional[datetime] = None
    page_number: Optional[int] = None
    cursor: Optional[str] = None
    page_size: Optional[int] = None
    page_offset: Optional[int] = None


# pylint: disable=too-many-instance-attributes,too-many-arguments
@dataclass(frozen=True)
class DynamicURLBuilder:
    url_excluding_configurable_parameters: str
    compose_able_url_key_val: dict
    from_date_param: Optional[str] = None
    to_date_param: Optional[str] = None
    date_format: Optional[str] = None
    next_page_cursor: Optional[str] = None
    page_number_param: Optional[str] = None
    offset_param: Optional[str] = None
    page_size_param: Optional[str] = None
    page_size: Optional[int] = None
    sort_key: Optional[str] = None
    sort_key_value: Optional[str] = None
    type_specific_params: Optional[dict] = None

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
            ),
            (self.sort_key, self.sort_key_value)
            ] if key and value)
        param_dict = {
            **param_dict,
            **self.compose_able_url_key_val
        }

        return self.compose_url(param_dict)


def get_url_builder_class(url_source_type: str = ''):
    url_builder = DynamicURLBuilder

    if url_source_type.strip().lower() == 'civi':
        url_builder = DynamicCiviURLBuilder
    if url_source_type == 'biorxiv_medrxiv_api':
        url_builder = DynamicBioRxivMedRxivURLBuilder
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
            ),
            ("sort", self.sort_key_value)
        ] if key and value)
        start_date_param = {
            self.from_date_param: {">=": start_date}
        } if start_date else {}

        field_to_return_param = self.get_fields_to_return()
        url_query_json_arg: dict = {
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


class DynamicBioRxivMedRxivURLBuilder(DynamicURLBuilder):
    #  setting none configurable parameters with dummy values
    def __init__(self, **kwargs):
        super().__init__(**{
            **kwargs,
            'offset_param': 'dummy-offset',
            'from_date_param': 'dummy-from-interval-date',
            'to_date_param': 'dummy-until-interval-date'
        })

    def get_url(
        self,
        url_compose_param: UrlComposeParam
    ):
        assert url_compose_param.from_date is not None
        assert url_compose_param.to_date is not None
        assert url_compose_param.page_offset is not None
        return '/'.join([
            self.url_excluding_configurable_parameters,
            url_compose_param.from_date.strftime(r'%Y-%m-%d'),
            url_compose_param.to_date.strftime(r'%Y-%m-%d'),
            str(url_compose_param.page_offset)
        ])
