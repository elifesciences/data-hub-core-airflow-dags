import base64
from typing import Iterable, Any, Optional, Sequence, Tuple
import logging
import xml.etree.ElementTree as ET
import requests


from data_pipeline.elife_article_xml.elife_article_xml_config import (
    ElifeArticleXmlConfig,
    ElifeArticleXmlSourceConfig
)
from data_pipeline.utils.data_store.bq_data_service import (
    does_bigquery_table_exist,
    get_single_column_value_list_from_bq_query,
    load_given_json_list_data_from_tempdir_to_bq
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


def iter_unprocessed_xml_file_url_from_git_directory(
    source_config: ElifeArticleXmlSourceConfig,
    processed_file_url_list: Sequence[str]
) -> Iterable[str]:
    response_json = get_json_response_from_url(
        url=get_url_of_xml_file_directory_from_repo(source_config=source_config)
    )
    for article_xml_url in response_json['tree']:
        if article_xml_url['size'] > 0:
            if article_xml_url['url'] not in processed_file_url_list:
                yield article_xml_url['url']


def iter_xml_file_url_and_decoded_content(
    article_xml_url_list: Iterable[str]
) -> Iterable[Tuple]:
    for article_xml_url in article_xml_url_list:
        response_json = get_json_response_from_url(url=article_xml_url)
        assert response_json['encoding'] == 'base64'
        yield (article_xml_url, base64.b64decode(response_json['content']).decode('utf-8'))


def get_bq_compatible_transformed_key_value(
    key: str,
    value: Any
) -> Tuple[Optional[str], Optional[Any]]:
    if 'href' in key:
        return 'href', value
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
    if parsed_dict:
        if 'front' in parsed_dict['article']:
            if 'article_meta' in parsed_dict['article']['front'][0]:
                article_meta_dict = get_bq_compatible_json_dict(
                    parsed_dict['article']['front'][0]['article_meta'][0]
                )
                LOGGER.info(article_meta_dict)
                if article_meta_dict:
                    for key in article_meta_dict.copy().keys():
                        if key not in ('related_article', 'article_id'):
                            article_meta_dict.pop(key, None)
                    LOGGER.info(article_meta_dict)
                return article_meta_dict
    return {}


def fetch_and_iter_related_article_from_elife_article_xml_repo(
    config: ElifeArticleXmlConfig
) -> Iterable[dict]:
    dataset_name = config.target.dataset_name
    project_name = config.target.project_name
    table_name = config.target.table_name
    if does_bigquery_table_exist(
        project_name=project_name,
        dataset_name=dataset_name,
        table_name=table_name
    ):
        processed_file_url_list = get_single_column_value_list_from_bq_query(
            project_name=project_name,
            query=f'''
                SELECT articles.article_xml.article_xml_url
                FROM
                `{project_name}.{dataset_name}.{table_name}` AS articles
            '''
        )
    else:
        processed_file_url_list = []

    article_xml_url_list = iter_unprocessed_xml_file_url_from_git_directory(
        source_config=config.source,
        processed_file_url_list=processed_file_url_list
    )

    for xml_file_url_and_content in iter_xml_file_url_and_decoded_content(article_xml_url_list):
        yield {
            'article_xml': {
                'article_xml_url': xml_file_url_and_content[0],
                'article_xml_content': get_article_json_data_from_xml_string_content(
                    xml_file_url_and_content[1]
                )
            }
        }


def fetch_related_article_from_elife_article_xml_repo_and_load_into_bq(
    config: ElifeArticleXmlConfig
):
    article_data_list = fetch_and_iter_related_article_from_elife_article_xml_repo(config)
    load_given_json_list_data_from_tempdir_to_bq(
            project_name=config.target.project_name,
            dataset_name=config.target.dataset_name,
            table_name=config.target.table_name,
            json_list=article_data_list
        )
