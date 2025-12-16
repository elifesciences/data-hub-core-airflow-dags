import re
import logging

LOGGER = logging.getLogger(__name__)


class EtlModuleConstant:
    BQ_SCHEMA_FIELD_NAME_KEY = "name"
    BQ_SCHEMA_SUBFIELD_KEY = "fields"
    BQ_SCHEMA_FIELD_TYPE_KEY = "type"


def convert_bq_schema_field_list_to_dict(json_list,) -> dict:
    return {
        bq_schema_field.get(EtlModuleConstant.BQ_SCHEMA_FIELD_NAME_KEY):
            bq_schema_field
        for bq_schema_field in json_list
    }


def standardize_field_name(field_name):
    return re.sub(r"\W", "_", field_name)
