from typing import NamedTuple


class RelatedArticlesSourceConfig(NamedTuple):
    git_repo_url: str
    headers: dict

    @staticmethod
    def from_dict(source_config_dict: dict) -> 'RelatedArticlesSourceConfig':
        return RelatedArticlesSourceConfig(
            git_repo_url=source_config_dict['gitRepoUrl'],
            headers=source_config_dict['headers']
        )
