import re
import datetime
from datetime import timezone
from datetime import timedelta
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


def get_date_of_days_before_as_string(number_of_days_before: int) -> str:
    dtobj = (
        datetime.datetime.now(timezone.utc) -
        timedelta(number_of_days_before)
    )
    return dtobj.strftime(EtlModuleConstant.STATE_FILE_DATE_FORMAT)


def convert_datetime_to_date_string(
        datetime_obj: datetime.datetime,
        time_format: str = EtlModuleConstant.STATE_FILE_DATE_FORMAT
) -> str:

    return datetime_obj.strftime(time_format)


def parse_datetime_from_str(
        date_as_string: str,
        time_format: str = EtlModuleConstant.STATE_FILE_DATE_FORMAT
):

    return datetime.datetime.strptime(date_as_string.strip(), time_format)


def convert_bq_schema_field_list_to_dict(json_list,) -> dict:
    return {
        bq_schema_field.get(EtlModuleConstant.BQ_SCHEMA_FIELD_NAME_KEY):
            bq_schema_field
        for bq_schema_field in json_list
    }


def standardize_field_name(field_name):
    return re.sub(r"\W", "_", field_name)
