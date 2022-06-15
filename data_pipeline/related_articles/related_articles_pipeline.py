from typing import Iterable
import requests


from data_pipeline.related_articles.related_articles_config import (
    RelatedArticlesSourceConfig
)

def get_url_of_xml_file_directory_from_repo(
    source_config: RelatedArticlesSourceConfig,
) -> str:
    response = requests.get(
        url=source_config.git_repo_url,
        headers=source_config.headers
    )
    response.raise_for_status()
    reponse_json = response.json()
    for folder in reponse_json['tree']:
        if folder['path']==source_config.directory_name:
            return folder['url']


def iter_xml_file_url_from_git_directory(
    source_config: RelatedArticlesSourceConfig
) -> Iterable[str]:
    article_xml_directory_url = get_url_of_xml_file_directory_from_repo(source_config=source_config)
    response = requests.get(
        url=article_xml_directory_url,
        headers=source_config.headers
    )
    response.raise_for_status()
    reponse_json = response.json()
    for article_xml_url in reponse_json['tree']:
        if article_xml_url['size'] > 0:
            yield article_xml_url['url']
