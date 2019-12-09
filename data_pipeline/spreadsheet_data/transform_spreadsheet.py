import re


def standardize_field_name(field_name):
    return re.sub(r'\W', '_', field_name)


def process_record(record: list,
                   record_metadata: dict,
                   col_index_to_field_name_mapping: dict
                   ):
    record_length = len(record)
    record_as_dict = {field_name: record[record_index]
                      for record_index, field_name
                      in col_index_to_field_name_mapping.items()
                      if record_index< record_length}
    record_as_dict.update(record_metadata)
    return record_as_dict


def get_schema_mapping(csv_header, schema_field_names: list = None):
    if schema_field_names:
        s_name_set = {name.lower() for name in schema_field_names}
        return {
            col_name:
            standardize_field_name(col_name)
            for col_name in csv_header
            if standardize_field_name(col_name).lower() in s_name_set
        }
    else:
        return {
            col_name:
            standardize_field_name(col_name)
            for col_name in csv_header
        }


def get_col_index_to_field_name_mapping(csv_header: list,
                                        schema_field_names: list,
                                        partial_schema_mapping: dict):

    unmapped_schema_field_names = [
        schema_field_name
        for schema_field_name in schema_field_names
        if schema_field_name not in set(partial_schema_mapping.values())
    ]

    s_mapping = get_schema_mapping(csv_header, unmapped_schema_field_names)
    partial_schema_mapping.update(s_mapping)
    col_index_to_field_name_mapping = {
        index: partial_schema_mapping.get(col_name)
        for index, col_name in enumerate(csv_header)
        if col_name in set(partial_schema_mapping.keys())
    }
    return col_index_to_field_name_mapping


def process_record_list(record_list,
                        record_metadata,
                        col_index_to_field_name_mapping
                        ):
    for record in record_list:
        n_record = process_record(record=record,
                                  record_metadata=record_metadata,
                                  col_index_to_field_name_mapping=col_index_to_field_name_mapping
                                  )
        yield n_record
