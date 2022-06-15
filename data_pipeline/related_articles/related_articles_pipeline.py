import requests

from data_pipeline.related_articles.related_artcicles_config import (
    RelatedArticlesSourceConfig
)

def get_url_of_directory_from_repo_tree(
    source_config: RelatedArticlesSourceConfig,
) -> str:
    response = requests.get(
        url=source_config.git_repo_url,
        headers=source_config.headers
    )
    response.raise_for_status()
    reponse_json = response.json()
    for folder in reponse_json['tree']:
        if folder['path']=='articles':
            return folder['url']

