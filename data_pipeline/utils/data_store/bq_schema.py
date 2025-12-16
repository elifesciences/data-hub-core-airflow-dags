import re
import logging
from typing import Mapping, Sequence

from typing_extensions import NotRequired, TypedDict

LOGGER = logging.getLogger(__name__)


class BigQueryFieldSchema(TypedDict):
    name: str
    type: str
    fields: NotRequired[Sequence['BigQueryFieldSchema']]


def convert_bq_schema_field_list_to_dict(
    bq_schema_field_list: Sequence[BigQueryFieldSchema]
) -> Mapping[str, BigQueryFieldSchema]:
    return {
        bq_schema_field['name']: bq_schema_field
        for bq_schema_field in bq_schema_field_list
    }


def standardize_field_name(field_name):
    return re.sub(r'\W', '_', field_name)
