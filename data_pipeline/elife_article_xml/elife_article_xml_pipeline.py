import base64
from typing import Iterable, Any, Optional, Tuple
import logging
import requests
import xml.etree.ElementTree as ET

from data_pipeline.elife_article_xml.elife_article_xml_config import (
    ElifeArticleXmlSourceConfig
)
from data_pipeline.utils.json import (
    get_recursively_transformed_object,
    remove_key_with_null_value
)
from data_pipeline.utils.xml import parse_xml_and_return_it_as_dict

LOGGER = logging.getLogger(__name__)


def get_json_response_from_url(url: str) -> Any:
    response = requests.get(url=url)
    response.raise_for_status()
    return response.json()


def get_url_of_xml_file_directory_from_repo(
    source_config: ElifeArticleXmlSourceConfig,
) -> str:
    response_json = get_json_response_from_url(url=source_config.git_repo_url)
    for folder in response_json['tree']:
        if folder['path'] == source_config.directory_name:
            return folder['url']


def iter_xml_file_url_from_git_directory(
    source_config: ElifeArticleXmlSourceConfig,
    processed_file_url_list: Iterable[str]
) -> Iterable[str]:
    response_json = get_json_response_from_url(
        url=get_url_of_xml_file_directory_from_repo(source_config=source_config)
    )
    for article_xml_url in response_json['tree']:
        if article_xml_url['size'] > 0:
            if article_xml_url['url'] not in processed_file_url_list:
                yield article_xml_url['url']


def iter_decoded_xml_file_content(
    article_xml_url_list: Iterable[str]
) -> Iterable[str]:
    for article_xml_url in article_xml_url_list:
        response_json = get_json_response_from_url(url=article_xml_url)
        if response_json['encoding'] == 'base64':
            yield base64.b64decode(response_json['content']).decode('utf-8')
        else:
            LOGGER.info('File is not decoded base64, file url: %s', article_xml_url)


def get_bq_compatible_transformed_key_value(
    key: str,
    value: Any
) -> Tuple[Optional[str], Optional[Any]]:
    return (
        key.replace('-', '_'),
        value
    )


def get_bq_compatible_json_dict(json_dict: dict) -> dict:
    return get_recursively_transformed_object(
        remove_key_with_null_value(json_dict),
        key_value_transform_fn=get_bq_compatible_transformed_key_value
    )


def get_article_json_data_from_xml_string_content(
    xml_string: str
) -> dict:
    xml_root = ET.fromstring(xml_string)
    parsed_dict = parse_xml_and_return_it_as_dict(xml_root)
    parsed_dict = get_bq_compatible_json_dict(parsed_dict)
    LOGGER.info(parsed_dict)
    return parsed_dict
