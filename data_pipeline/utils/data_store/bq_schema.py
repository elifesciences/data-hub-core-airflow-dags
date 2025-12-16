import re
import logging

LOGGER = logging.getLogger(__name__)


class EtlModuleConstant:
    DEFAULT_DATA_COLLECTION_START_DATE = "2000-01-01"
    # config for the crossref data
    CROSSREF_DATA_COLLECTED_TIMESTAMP_KEY = "timestamp"
    CROSSREF_TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
    MESSAGE_NEXT_CURSOR_KEY = "next-cursor"
    # config for bigquery schema
    BQ_SCHEMA_FIELD_NAME_KEY = "name"
    BQ_SCHEMA_SUBFIELD_KEY = "fields"
    BQ_SCHEMA_FIELD_TYPE_KEY = "type"
    # date format used for y application for maintaining download state
    STATE_FILE_DATE_FORMAT = "%Y-%m-%d"


def convert_bq_schema_field_list_to_dict(json_list,) -> dict:
    return {
        bq_schema_field.get(EtlModuleConstant.BQ_SCHEMA_FIELD_NAME_KEY):
            bq_schema_field
        for bq_schema_field in json_list
    }


def standardize_field_name(field_name):
    return re.sub(r"\W", "_", field_name)
