import logging
import os
from math import ceil
from typing import Any, Iterable, List, Optional, Sequence
from tempfile import TemporaryDirectory
import pandas as pd
from jinjasql import JinjaSql
from google.cloud import bigquery
from google.cloud.bigquery import LoadJobConfig, Client, table as bq_table
from google.cloud.bigquery.schema import SchemaField
from google.cloud.exceptions import NotFound
from google.cloud.bigquery import WriteDisposition
from google.cloud.bigquery.table import RowIterator
from bigquery_schema_generator.generate_schema import SchemaGenerator
from data_pipeline.utils.pipeline_file_io import write_jsonl_to_file

LOGGER = logging.getLogger(__name__)

MAX_ROWS_INSERTABLE = 1000


def get_bq_client(project: str):
    return Client(project=project)


# pylint: disable=too-many-arguments
def load_file_into_bq(
        filename: str,
        dataset_name: str,
        table_name: str,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_mode=WriteDisposition.WRITE_APPEND,
        auto_detect_schema=False,
        rows_to_skip=0,
        project_name: Optional[str] = None,
):
    if os.path.isfile(filename) and os.path.getsize(filename) == 0:
        LOGGER.info("File %s is empty.", filename)
        return
    client = get_bq_client(project=project_name)
    dataset_ref = client.dataset(dataset_name)
    table_ref = dataset_ref.table(table_name)
    job_config = LoadJobConfig()
    job_config.write_disposition = write_mode
    job_config.autodetect = auto_detect_schema
    job_config.source_format = source_format
    if source_format is bigquery.SourceFormat.CSV:
        job_config.skip_leading_rows = rows_to_skip

    with open(filename, "rb") as source_file:
        job = client.load_table_from_file(
            source_file, destination=table_ref, job_config=job_config
        )

        # Waits for table cloud_data_store to complete
        job.result()
        LOGGER.info(
            "Loaded %s rows into %s:%s.",
            job.output_rows,
            dataset_name,
            table_name
        )


def load_tuple_list_into_bq(
        tuple_list_to_insert: List[tuple], dataset_name: str, table_name: str
) -> List[dict]:
    client = Client()
    table_ref = client.dataset(dataset_name).table(table_name)
    table = client.get_table(table_ref)  # API request

    errors = []

    for indx in range(ceil(len(tuple_list_to_insert) / MAX_ROWS_INSERTABLE)):
        errors.extend(
            load_tuple_list_page_into_bq(
                client,
                table,
                tuple_list_to_insert[
                    indx * MAX_ROWS_INSERTABLE:
                    (indx + 1) * MAX_ROWS_INSERTABLE
                ],
            )
        )
    LOGGER.info("Loaded  data into %s:%s.", dataset_name, table_name)
    return errors


def load_tuple_list_page_into_bq(
        client: Client, table: bq_table, tuple_list_to_insert: List[tuple],
) -> List[dict]:

    errors = client.insert_rows(table, tuple_list_to_insert)
    return errors


def create_table(
        project_name: str,
        dataset_name: str,
        table_name: str,
        json_schema: list
):
    client = get_bq_client(project=project_name)
    table_id = compose_full_table_name(
        project_name, dataset_name, table_name
    )
    schema = get_schemafield_list_from_json_list(json_schema)
    table = bigquery.Table(table_id, schema=schema)
    table = client.create_table(table, True)  # API request
    LOGGER.info(
        "Created table %s.%s.%s",
        table.project,
        table.dataset_id,
        table.table_id
    )


def create_table_if_not_exist(
        project_name: str,
        dataset_name: str,
        table_name: str,
        json_schema: list
):
    if not does_bigquery_table_exist(project_name, dataset_name, table_name):
        create_table(project_name, dataset_name, table_name, json_schema)


def does_bigquery_table_exist(
        project_name: str, dataset_name: str, table_name: str
) -> bool:
    table_id = compose_full_table_name(project_name, dataset_name, table_name)
    client = get_bq_client(project=project_name)
    try:
        client.get_table(table_id)
        return True
    except NotFound:
        return False


def compose_full_table_name(
        project_name: str, dataset_name: str, table_name: str
) -> str:
    return ".".join([project_name, dataset_name, table_name])


def get_schemafield_list_from_json_list(
        json_schema: List[dict]
) -> List[SchemaField]:
    schema = [SchemaField.from_api_repr(x) for x in json_schema]
    return schema


def get_table_schema_field_names(
        project_name: str,
        dataset_name: str,
        table_name: str
):
    return [
        field.name for field in
        get_table_schema(project_name, dataset_name, table_name)
    ]


def get_table_schema(
        project_name: str,
        dataset_name: str,
        table_name: str
):
    client = get_bq_client(project=project_name)
    dataset_ref = client.dataset(dataset_name, project=project_name)
    table_ref = dataset_ref.table(table_name)
    try:
        table = client.get_table(table_ref)  # API Request
        return [field.name for field in table.schema]
    except NotFound:
        return []


def extend_table_schema_with_nested_schema(
        project_name: str, dataset_name: str,
        table_name: str, new_fields: list
):
    client = get_bq_client(project=project_name)
    dataset_ref = client.dataset(dataset_name, project=project_name)
    table_ref = dataset_ref.table(table_name)
    table = client.get_table(table_ref)  # Make an API request.
    original_schema = table.schema
    original_schema_dict = [
        schema_field.to_api_repr()
        for schema_field in original_schema
    ]
    new_schema_dict = get_new_merged_schema(
        original_schema_dict, new_fields
    )
    new_schema = [
        SchemaField.from_api_repr(schema_field_dict)
        for schema_field_dict in new_schema_dict
    ]

    table.schema = new_schema
    client.update_table(table, ["schema"])  # Make an API request.


def get_new_merged_schema(
        existing_schema: list,
        update_schema: list,
):
    new_schema = []
    existing_schema_dict = {
        schema_object.get("name").lower(): schema_object
        for schema_object in existing_schema
    }
    update_schema_dict = {
        schema_object.get("name").lower(): schema_object
        for schema_object in update_schema
    }
    merged_dict = {
        **update_schema_dict,
        **existing_schema_dict
    }
    set_intersection = (
        set(existing_schema_dict.keys()).intersection(
            set(update_schema_dict.keys())
        )
    )

    fields_to_recurse = [
        obj_key
        for obj_key in set_intersection
        if existing_schema_dict.get(obj_key).get("fields") and
        isinstance(existing_schema_dict.get(obj_key).get("fields"), list)
    ]
    new_schema.extend(
        [
            merged_dict.get(key)
            for key, value in merged_dict.items()
            if key not in fields_to_recurse
        ]
    )
    for field_to_recurse in fields_to_recurse:
        field = existing_schema_dict.get(field_to_recurse).copy()
        field["fields"] = get_new_merged_schema(
            existing_schema_dict.get(field_to_recurse).get("fields", []),
            update_schema_dict.get(field_to_recurse).get("fields", []),
        )
        new_schema.append(
            field
        )

    return new_schema


def generate_schema_from_file(
        full_file_location: str,
        quoted_values_are_strings: bool = True
):
    with open(full_file_location, encoding='UTF-8') as file_reader:
        generator = SchemaGenerator(
            input_format="json",
            quoted_values_are_strings=quoted_values_are_strings
        )
        schema_map, _ = generator.deduce_schema(
            file_reader
        )
        schema = generator.flatten_schema(schema_map)
    return schema


def create_or_extend_table_schema(
        gcp_project,
        dataset_name,
        table_name,
        full_file_location,
        quoted_values_are_strings: bool = True
):
    schema = generate_schema_from_file(
        full_file_location,
        quoted_values_are_strings
    )

    if does_bigquery_table_exist(
            gcp_project,
            dataset_name,
            table_name,
    ):
        extend_table_schema_with_nested_schema(
            gcp_project,
            dataset_name,
            table_name,
            schema
        )
    else:
        create_table(
            gcp_project,
            dataset_name,
            table_name,
            schema
        )


def delete_table_from_bq(
        project_name: str,
        dataset_name: str,
        table_name: str
):
    client = get_bq_client(project=project_name)
    table_id = compose_full_table_name(
        project_name, dataset_name, table_name
    )
    client.delete_table(table_id, not_found_ok=True)
    LOGGER.info("Deleted table %s", table_id)


def load_from_temp_table_to_actual_table(
        project_name: str,
        dataset_name: str,
        table_name: str,
        temp_table_name: str,
        column_name: str
):
    client = get_bq_client(project=project_name)
    table_id = compose_full_table_name(
        project_name, dataset_name, table_name
    )

    if does_bigquery_table_exist(
            project_name=project_name,
            dataset_name=dataset_name,
            table_name=table_name):
        sql = (
            f"""
            SELECT t.* EXCEPT(seqnum)
            FROM (SELECT *,
                    ROW_NUMBER() OVER (PARTITION BY {column_name}
                                    ORDER BY imported_timestamp DESC
                                ) AS seqnum
                FROM `{project_name}.{dataset_name}.{temp_table_name}`
                ) t
            WHERE seqnum = 1
            AND {column_name} NOT IN (SELECT DISTINCT {column_name}
                        FROM `{project_name}.{dataset_name}.{table_name}`)
            """
        )
    else:
        sql = (
            f"""
            SELECT *
            FROM `{project_name}.{dataset_name}.{temp_table_name}`
            """
        )

    job_config = bigquery.QueryJobConfig(
        allow_large_results=True,
        destination=table_id,
        write_disposition='WRITE_APPEND'
    )

    query_job = client.query(sql, job_config=job_config)  # Make an API request.
    query_job.result()  # Wait for the job to complete.

    LOGGER.info("Loaded table %s", table_id)


def get_bq_result_from_bq_query(
    project_name: str,
    query: str
) -> RowIterator:
    client = get_bq_client(project=project_name)
    query_job = client.query(query)  # Make an API request.
    bq_result = query_job.result()  # Waits for query to finish
    LOGGER.debug('bq_result: %r', bq_result)
    return bq_result


def get_query_with_exclusion(
    query: str,
    key_field_name: str,
    exclude_query: Optional[str] = None
) -> str:
    if not exclude_query:
        return query
    return f'SELECT * FROM (\n{query}\n)\nWHERE {key_field_name} NOT IN (\n{exclude_query}\n)'


def iter_dict_from_bq_query(
    project_name: str,
    query: str
) -> Iterable[dict]:
    bq_result = get_bq_result_from_bq_query(project_name=project_name, query=query)
    for row in bq_result:
        LOGGER.debug('row: %r', row)
        yield dict(row.items())


def get_single_column_value_list_from_bq_query(
    project_name: str,
    query: str
) -> Sequence[str]:
    results = get_bq_result_from_bq_query(project_name=project_name, query=query)
    return [row[0] for row in results]


def get_distinct_values_from_bq(
            project_name: str,
            dataset_name: str,
            column_name: str,
            table_name_source: str,
            table_name_for_exclusion: Optional[str] = None,
            array_column_for_exclusion: Optional[str] = None,
            array_table_name: Optional[str] = None,
        ) -> pd.DataFrame:

    sql = """
        SELECT DISTINCT {{ column_name }} AS column
        FROM  `{{ project_name }}.{{ dataset_name }}.{{ table_name_source }}`
        WHERE {{ column_name }} IS NOT NULL
        {% if array_table_name %}
        AND {{ array_column_for_exclusion }} NOT IN
            (
                SELECT t_array.{{ array_column_for_exclusion }}
                FROM `{{ project_name }}.{{ dataset_name }}.{{ table_name_for_exclusion }}`
                LEFT JOIN UNNEST({{ array_table_name }}) AS t_array
            )
        {% elif table_name_for_exclusion %}
        AND {{ column_name }} NOT IN
            (
                SELECT {{ column_name }}
                FROM `{{ project_name }}.{{ dataset_name }}.{{ table_name_for_exclusion }}`
            )
        {% endif %}
    """
    params = {
        'project_name': project_name,
        'dataset_name': dataset_name,
        'column_name': column_name,
        'table_name_source': table_name_source,
        'table_name_for_exclusion': table_name_for_exclusion,
        'array_column_for_exclusion': array_column_for_exclusion,
        'array_table_name': array_table_name,
        }

    jin = JinjaSql(param_style='pyformat')
    query, bind_params = jin.prepare_query(sql, params)
    client = get_bq_client(project=project_name)
    query_job = client.query(query % bind_params)  # Make an API request.
    results = query_job.result()  # Waits for query to finish

    return pd.Series([row.column for row in results]).to_frame().reset_index()


def get_max_value_from_bq_table(
            project_name: str,
            dataset_name: str,
            column_name: str,
            table_name: str,
        ) -> str:

    sql = """
        SELECT
        MAX({{ column_name }}) AS max_value
        FROM `{{ project_name }}.{{ dataset_name }}.{{ table_name }}`
    """
    params = {
        'project_name': project_name,
        'dataset_name': dataset_name,
        'table_name': table_name,
        'column_name': column_name
        }

    jin = JinjaSql(param_style='pyformat')
    query, bind_params = jin.prepare_query(sql, params)
    client = get_bq_client(project=project_name)
    query_job = client.query(query % bind_params)  # Make an API request.
    results = query_job.result()  # Waits for query to finish

    return list(results)[0].max_value


def copy_bq_table(
        source_project_name: str,
        source_dataset_name: str,
        source_table_name: str,
        target_project_name: str,
        target_dataset_name: str,
        target_table_name: str,
):

    client = get_bq_client(project=source_project_name)

    source_table_id = compose_full_table_name(
        source_project_name, source_dataset_name, source_table_name
    )
    target_table_id = compose_full_table_name(
        target_project_name, target_dataset_name, target_table_name
    )

    job = client.copy_table(source_table_id, target_table_id)
    job.result()  # Wait for the job to complete.

    LOGGER.info(
        "Copied table  from %s to %s",
        source_table_id,
        target_table_id
    )


def load_given_json_list_data_from_tempdir_to_bq(
    project_name: str,
    dataset_name: str,
    table_name: str,
    json_list: Iterable[Any]
):
    with TemporaryDirectory() as tmp_dir:
        filename = os.path.join(tmp_dir, 'tmp_file.json')
        write_jsonl_to_file(
            json_list=json_list,
            full_temp_file_location=filename,
        )
        if os.path.getsize(filename) > 0:
            create_or_extend_table_schema(
                gcp_project=project_name,
                dataset_name=dataset_name,
                table_name=table_name,
                full_file_location=filename,
                quoted_values_are_strings=True
            )
            load_file_into_bq(
                project_name=project_name,
                dataset_name=dataset_name,
                table_name=table_name,
                filename=filename
            )
            LOGGER.info('Loaded table: %s', table_name)
        else:
            LOGGER.info('No updates found for the table: %s', table_name)
