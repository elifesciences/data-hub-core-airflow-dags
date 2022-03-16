from data_pipeline.europepmc.europepmc_labslink_config import (
    EuropePmcLabsLinkConfig
)


SQL_QUERY_1 = 'query 1'


BIGQUERY_SOURCE_CONFIG_DICT_1 = {
    'sqlQuery': SQL_QUERY_1
}

SOURCE_CONFIG_DICT_1 = {
    'bigQuery': BIGQUERY_SOURCE_CONFIG_DICT_1
}

FTP_TARGET_CONFIG_DICT_1 = {
    'host': 'host1',
    'username': 'username1'
}

TARGET_CONFIG_DICT_1 = {
    'ftp': FTP_TARGET_CONFIG_DICT_1
}

ITEM_CONFIG_DICT_1 = {
    'source': SOURCE_CONFIG_DICT_1,
    'target': TARGET_CONFIG_DICT_1
}


def get_config_for_item_config_dict(item_dict: dict) -> dict:
    return {'europePmcLabsLink': [item_dict]}


CONFIG_DICT_1 = get_config_for_item_config_dict(ITEM_CONFIG_DICT_1)


class TestEuropeLabsLinkPmcConfig:
    def test_should_read_bigquery_sql_query(self):
        config = EuropePmcLabsLinkConfig.from_dict(CONFIG_DICT_1)
        assert config.source.bigquery.sql_query == SQL_QUERY_1

    def test_should_read_target_ftp_host_and_username(self):
        config = EuropePmcLabsLinkConfig.from_dict(CONFIG_DICT_1)
        assert config.target.ftp.host == FTP_TARGET_CONFIG_DICT_1['host']
        assert config.target.ftp.username == FTP_TARGET_CONFIG_DICT_1['username']
