from dataclasses import dataclass
import logging


LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class MonitoringConfig:
    project_name: str
    dataset_name: str
    table_name: str

    @staticmethod
    def from_dict(
        config_dict: dict
    ) -> 'MonitoringConfig':
        return MonitoringConfig(
            project_name=config_dict['project'],
            dataset_name=config_dict['dataset'],
            table_name=config_dict['table']
        )
