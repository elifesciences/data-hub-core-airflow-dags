import base64
from typing import Iterable, Any
import requests
import logging


from data_pipeline.elife_article_xml.elife_article_xml_config import (
    RelatedArticlesSourceConfig
)

LOGGER = logging.getLogger(__name__)


def get_json_response_from_url(url: str) -> Any:
    response = requests.get(url=url)
    response.raise_for_status()
    return response.json()


def get_url_of_xml_file_directory_from_repo(
    source_config: RelatedArticlesSourceConfig,
) -> str:
    response_json = get_json_response_from_url(url=source_config.git_repo_url)
    for folder in response_json['tree']:
        if folder['path'] == source_config.directory_name:
            return folder['url']


def iter_xml_file_url_from_git_directory(
    source_config: RelatedArticlesSourceConfig
) -> Iterable[str]:
    response_json = get_json_response_from_url(
        url=get_url_of_xml_file_directory_from_repo(source_config=source_config)
    )
    for article_xml_url in response_json['tree']:
        if article_xml_url['size'] > 0:
            yield article_xml_url['url']


def iter_decoded_xml_file_content(
    article_xml_url_list: Iterable[str]
) -> Iterable[str]:
    # dont forget to update the list
    for article_xml_url in article_xml_url_list:
        response_json = get_json_response_from_url(url=article_xml_url)
        if response_json['encoding'] == 'base64':
            yield base64.b64decode(response_json['content']).decode('utf-8')
        else:
            LOGGER.info('File is not decoded base64, file url: %s', article_xml_url)


def fetch_related_article_from_elife_article_xml_repo_and_load_into_bigquery(
    source_config: RelatedArticlesSourceConfig
):

    article_xml_url_list = iter_xml_file_url_from_git_directory(source_config=source_config)
    iter_decoded_xml_file_content(article_xml_url_list)
