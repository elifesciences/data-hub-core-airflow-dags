import logging
import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Iterable, Mapping, NamedTuple, Optional, Sequence, Type
from urllib import parse
from typing_extensions import NotRequired, TypedDict

from data_pipeline.utils.data_pipeline_timestamp import datetime_to_string
from data_pipeline.utils.pipeline_utils import replace_placeholders
from data_pipeline.utils.web_api import (
    DEFAULT_WEB_API_RETRY_CONFIG,
    DISABLED_WEB_API_RETRY_CONFIG,
    WebApiRetryConfig
)


LOGGER = logging.getLogger(__name__)


class WebApiDynamicRequestParameters(NamedTuple):
    from_date: Optional[datetime] = None
    to_date: Optional[datetime] = None
    page_number: Optional[int] = None
    cursor: Optional[str] = None
    page_size: Optional[int] = None
    page_offset: Optional[int] = None
    source_values: Optional[Iterable[dict]] = None
    placeholder_values: Optional[dict] = None


# pylint: disable=too-many-instance-attributes,too-many-arguments
@dataclass(frozen=True)
class WebApiDynamicRequestBuilder:
    url_excluding_configurable_parameters: str
    static_parameters: dict
    from_date_param: Optional[str] = None
    to_date_param: Optional[str] = None
    date_format: Optional[str] = None
    next_page_cursor: Optional[str] = None
    allow_same_next_page_cursor: bool = False
    page_number_param: Optional[str] = None
    offset_param: Optional[str] = None
    page_size_param: Optional[str] = None
    page_size: Optional[int] = None
    sort_key: Optional[str] = None
    sort_key_value: Optional[str] = None
    method: str = 'GET'
    max_source_values_per_request: Optional[int] = None
    request_builder_parameters: Optional[dict] = None
    retry_config: WebApiRetryConfig = DEFAULT_WEB_API_RETRY_CONFIG

    def get_json(  # pylint: disable=unused-argument
        self,
        dynamic_request_parameters: WebApiDynamicRequestParameters
    ) -> Optional[Any]:
        return None

    def _get_url_separator(self) -> str:
        url = self.url_excluding_configurable_parameters
        if "?" in url:
            if url.strip().endswith("&") or url.strip().endswith("?"):
                url_separator = ""
            else:
                url_separator = "&"
        else:
            url_separator = "?"
        return url_separator

    def compose_url(
        self,
        parameters_key_value: dict,
        placeholder_values: Optional[dict] = None
    ) -> str:
        url = self.url_excluding_configurable_parameters
        url_separator = self._get_url_separator()
        params = parse.urlencode(
            {
                key: value
                for key, value in parameters_key_value.items() if key and value
            }
        )
        return replace_placeholders(url, placeholder_values) + url_separator + params

    def get_url(
        self,
        dynamic_request_parameters: WebApiDynamicRequestParameters,
    ) -> str:
        start_date = datetime_to_string(
            dynamic_request_parameters.from_date, self.date_format
        )

        end_date = datetime_to_string(
            dynamic_request_parameters.to_date, self.date_format
        )
        param_dict = dict((key, value) for key, value in [
            (self.from_date_param, start_date),
            (self.next_page_cursor, dynamic_request_parameters.cursor),
            (self.to_date_param, end_date),
            (self.page_number_param, dynamic_request_parameters.page_number),
            (self.offset_param, dynamic_request_parameters.page_offset),
            (
                self.page_size_param,
                dynamic_request_parameters.page_size or self.page_size
            ),
            (self.sort_key, self.sort_key_value)
            ] if key and value)
        param_dict = {
            **param_dict,
            **self.static_parameters
        }

        return self.compose_url(param_dict)


CiviFieldsToReturnDict = TypedDict(
    'CiviFieldsToReturnDict',
    {
        'return': NotRequired[str]
    }
)


class CiviWebApiDynamicRequestBuilder(WebApiDynamicRequestBuilder):
    def get_url(
        self,
        dynamic_request_parameters: WebApiDynamicRequestParameters,
    ) -> str:
        start_date = datetime_to_string(
            dynamic_request_parameters.from_date, self.date_format
        )
        options = dict((key, value) for key, value in [
            (self.offset_param, dynamic_request_parameters.page_offset),
            (
                self.page_size_param,
                dynamic_request_parameters.page_size or self.page_size
            ),
            ("sort", self.sort_key_value)
        ] if key and value)
        start_date_param = {
            self.from_date_param: {">=": start_date}
        } if start_date else {}

        field_to_return_param = self.get_fields_to_return_dict()
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
            **self.static_parameters
        }
        url_no_options = self.compose_url(param_dict)
        return url_no_options + "&json=" + url_query_json_arg_as_str

    def get_fields_to_return_dict(self) -> CiviFieldsToReturnDict:
        field_to_return_param: CiviFieldsToReturnDict = {}
        assert self.request_builder_parameters is not None
        field_to_return_list = self.request_builder_parameters.get(
            "fieldsToReturn"
        )
        if field_to_return_list:
            field_to_return_param = {
                "return": ",".join(field_to_return_list)
            }
        return field_to_return_param


class BioRxivWebApiDynamicRequestBuilder(WebApiDynamicRequestBuilder):
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
        dynamic_request_parameters: WebApiDynamicRequestParameters
    ) -> str:
        assert dynamic_request_parameters.from_date is not None
        assert dynamic_request_parameters.to_date is not None
        assert dynamic_request_parameters.page_offset is not None
        return '/'.join([
            self.url_excluding_configurable_parameters,
            dynamic_request_parameters.from_date.strftime(r'%Y-%m-%d'),
            dynamic_request_parameters.to_date.strftime(r'%Y-%m-%d'),
            str(dynamic_request_parameters.page_offset)
        ])


class CrossrefMetadataWebApiDynamicRequestBuilder(WebApiDynamicRequestBuilder):
    def __init__(self, **kwargs):
        super().__init__(**{
            **kwargs,
            # We need to disable retry due to Crossref API with cursor not being stateless
            'retry_config': DISABLED_WEB_API_RETRY_CONFIG,
            # We are allowing the next page cursor to be the same as the previous cursor
            'allow_same_next_page_cursor': True
        })

    def get_url(
        self,
        dynamic_request_parameters: WebApiDynamicRequestParameters
    ) -> str:
        if not dynamic_request_parameters.cursor:
            dynamic_request_parameters = dynamic_request_parameters._replace(
                cursor='*'
            )
        LOGGER.debug('dynamic_request_parameters: %r', dynamic_request_parameters)
        start_date = datetime_to_string(
            dynamic_request_parameters.from_date, self.date_format
        )
        end_date = datetime_to_string(
            dynamic_request_parameters.to_date, self.date_format
        )
        filter_dict = {
            key: value
            for key, value in [
                (self.from_date_param, start_date),
                (self.to_date_param, end_date),
            ] if key and value
        }
        filter_value = ','.join([
            f'{key}:{value}'
            for key, value in filter_dict.items()
        ])
        param_dict = {
            key: value
            for key, value in [
                ('filter', filter_value),
                (self.next_page_cursor, dynamic_request_parameters.cursor),
                (self.page_number_param, dynamic_request_parameters.page_number),
                (self.offset_param, dynamic_request_parameters.page_offset),
                (
                    self.page_size_param,
                    dynamic_request_parameters.page_size or self.page_size
                ),
                (self.sort_key, self.sort_key_value)
            ]
            if key and value
        }
        param_dict = {
            **param_dict,
            **self.static_parameters
        }
        return self.compose_url(
            parameters_key_value=param_dict,
            placeholder_values=dynamic_request_parameters.placeholder_values
        )


class S2TitleAbstractEmbeddingsWebApiDynamicRequestBuilder(WebApiDynamicRequestBuilder):
    def __init__(self, **kwargs):
        super().__init__(**{
            **kwargs,
            'method': 'POST',
            'max_source_values_per_request': 16
        })

    def get_json(
        self,
        dynamic_request_parameters: WebApiDynamicRequestParameters
    ) -> Sequence[dict]:
        assert dynamic_request_parameters.source_values is not None
        return [
            {
                'paper_id': source_value['paper_id'],
                'title': source_value['title'],
                'abstract': source_value['abstract']
            }
            for source_value in dynamic_request_parameters.source_values
        ]


WEB_API_REQUEST_BUILDER_CLASS_BY_NAME_MAP: Mapping[str, Type[WebApiDynamicRequestBuilder]] = {
    'civi': CiviWebApiDynamicRequestBuilder,
    'biorxiv_medrxiv_api': BioRxivWebApiDynamicRequestBuilder,
    's2_title_abstract_embeddings_api': S2TitleAbstractEmbeddingsWebApiDynamicRequestBuilder,
    'crossref_metadata_api': CrossrefMetadataWebApiDynamicRequestBuilder
}


def get_web_api_request_builder_class(
    request_builder_name: str = ''
) -> Type[WebApiDynamicRequestBuilder]:
    return WEB_API_REQUEST_BUILDER_CLASS_BY_NAME_MAP.get(
        request_builder_name.strip().lower(),
        WebApiDynamicRequestBuilder
    )
