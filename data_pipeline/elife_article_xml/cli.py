import logging
from typing import Sequence
from data_pipeline.elife_article_xml.elife_article_xml_config import ElifeArticleXmlConfig
from data_pipeline.elife_article_xml.elife_article_xml_pipeline import (
    fetch_elife_article_data_and_load_into_bq_from_config_list
)
from data_pipeline.utils.pipeline_config import (
    get_pipeline_config_for_env_name_and_config_parser
)

LOGGER = logging.getLogger(__name__)


class ElifeArticleXmlEnvironmentVariables:
    CONFIG_FILE_PATH = 'ELIFE_ARTICLE_XML_CONFIG_FILE_PATH'
    SCHEDULE_INTERVAL = 'ELIFE_ARTICLE_XML_PIPELINE_SCHEDULE_INTERVAL'


def get_elife_article_xml_config_list() -> Sequence[ElifeArticleXmlConfig]:
    return get_pipeline_config_for_env_name_and_config_parser(
        ElifeArticleXmlEnvironmentVariables.CONFIG_FILE_PATH,
        ElifeArticleXmlConfig.parse_config_list_from_dict
    )


def main():
    configs = get_elife_article_xml_config_list()
    fetch_elife_article_data_and_load_into_bq_from_config_list(configs)
    LOGGER.info('Data fetch and load process completed successfully.')


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main()
