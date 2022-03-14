from typing import NamedTuple, Optional, Sequence


DEFAULT_BATCH_SIZE = 1000


class EuropePmcSearchConfig(NamedTuple):
    query: Optional[str] = None

    @staticmethod
    def from_dict(search_config_dict: dict) -> 'EuropePmcSearchConfig':
        return EuropePmcSearchConfig(
            query=search_config_dict.get('query')
        )


class EuropePmcSourceConfig(NamedTuple):
    api_url: str
    search: EuropePmcSearchConfig
    fields_to_return: Optional[Sequence[str]] = None

    @staticmethod
    def from_dict(source_config_dict: dict) -> 'EuropePmcSourceConfig':
        return EuropePmcSourceConfig(
            api_url=source_config_dict['apiUrl'],
            search=EuropePmcSearchConfig.from_dict(
                source_config_dict['search']
            ),
            fields_to_return=source_config_dict.get('fieldsToReturn')
        )


class BigQueryTargetConfig(NamedTuple):
    project_name: str
    dataset_name: str
    table_name: str

    @staticmethod
    def from_dict(target_config_dict: dict) -> 'BigQueryTargetConfig':
        return BigQueryTargetConfig(
            project_name=target_config_dict['projectName'],
            dataset_name=target_config_dict['datasetName'],
            table_name=target_config_dict['tableName']
        )


class EuropePmcInitialStateConfig(NamedTuple):
    start_date_str: str

    @staticmethod
    def from_dict(initial_state_config_dict: dict) -> 'EuropePmcInitialStateConfig':
        return EuropePmcInitialStateConfig(
            start_date_str=initial_state_config_dict['startDate']
        )


class StateFileConfig(NamedTuple):
    bucket_name: str
    object_name: str

    @staticmethod
    def from_dict(state_file_config_dict: dict) -> 'StateFileConfig':
        return StateFileConfig(
            bucket_name=state_file_config_dict['bucketName'],
            object_name=state_file_config_dict['objectName']
        )


class EuropePmcStateConfig(NamedTuple):
    initial_state: EuropePmcInitialStateConfig
    state_file: StateFileConfig

    @staticmethod
    def from_dict(state_config_dict: dict) -> 'EuropePmcStateConfig':
        return EuropePmcStateConfig(
            initial_state=EuropePmcInitialStateConfig.from_dict(
                state_config_dict['initialState']
            ),
            state_file=StateFileConfig.from_dict(
                state_config_dict['stateFile']
            )
        )


class EuropePmcConfig(NamedTuple):
    source: EuropePmcSourceConfig
    target: BigQueryTargetConfig
    state: EuropePmcStateConfig
    batch_size: int = DEFAULT_BATCH_SIZE

    @staticmethod
    def _from_item_dict(item_config_dict: dict) -> 'EuropePmcConfig':
        return EuropePmcConfig(
            source=EuropePmcSourceConfig.from_dict(
                item_config_dict['source']
            ),
            target=BigQueryTargetConfig.from_dict(
                item_config_dict['target']
            ),
            state=EuropePmcStateConfig.from_dict(
                item_config_dict['state']
            ),
            batch_size=item_config_dict.get('batchSize') or DEFAULT_BATCH_SIZE
        )

    @staticmethod
    def from_dict(config_dict: dict) -> 'EuropePmcConfig':
        item_config_list = config_dict['europePmc']
        return EuropePmcConfig._from_item_dict(
            item_config_list[0]
        )
