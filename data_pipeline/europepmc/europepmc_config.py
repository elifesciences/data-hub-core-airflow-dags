from typing import Mapping, NamedTuple, Optional, Sequence

from data_pipeline.utils.pipeline_config import BigQueryTargetConfig, StateFileConfig


DEFAULT_BATCH_SIZE = 1000


class EuropePmcSearchConfig(NamedTuple):
    query: Optional[str] = None
    extra_params: Optional[Mapping[str, str]] = None

    @staticmethod
    def from_dict(search_config_dict: dict) -> 'EuropePmcSearchConfig':
        return EuropePmcSearchConfig(
            query=search_config_dict.get('query'),
            extra_params={
                key: value
                for key, value in search_config_dict.items()
                if key != 'query'
            }
        )


DEFAULT_EXTRACT_INDIVIDUAL_RESULTS_FROM_RESPONSE = True


class EuropePmcSourceConfig(NamedTuple):
    api_url: str
    search: EuropePmcSearchConfig
    fields_to_return: Optional[Sequence[str]] = None
    extract_individual_results_from_response: bool = (
        DEFAULT_EXTRACT_INDIVIDUAL_RESULTS_FROM_RESPONSE
    )
    max_days: Optional[int] = None

    @staticmethod
    def from_dict(source_config_dict: dict) -> 'EuropePmcSourceConfig':
        source_config = EuropePmcSourceConfig(
            api_url=source_config_dict['apiUrl'],
            search=EuropePmcSearchConfig.from_dict(
                source_config_dict['search']
            ),
            fields_to_return=source_config_dict.get('fieldsToReturn'),
            extract_individual_results_from_response=source_config_dict.get(
                'extractIndividualResultsFromResponse',
                DEFAULT_EXTRACT_INDIVIDUAL_RESULTS_FROM_RESPONSE
            ),
            max_days=source_config_dict.get('maxDays')
        )
        assert (
            source_config.extract_individual_results_from_response
            or not source_config.fields_to_return
        ), 'writing whole response does not support fields_to_return'
        return source_config


class EuropePmcInitialStateConfig(NamedTuple):
    start_date_str: str

    @staticmethod
    def from_dict(initial_state_config_dict: dict) -> 'EuropePmcInitialStateConfig':
        return EuropePmcInitialStateConfig(
            start_date_str=initial_state_config_dict['startDate']
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
        config = EuropePmcConfig(
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
        return config

    @staticmethod
    def parse_config_list_from_dict(config_dict: dict) -> Sequence['EuropePmcConfig']:
        item_config_dict_list = config_dict['europePmc']
        return [
            EuropePmcConfig._from_item_dict(item_config_dict)
            for item_config_dict in item_config_dict_list
        ]
