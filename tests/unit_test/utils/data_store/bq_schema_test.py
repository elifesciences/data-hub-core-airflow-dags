from data_pipeline.utils.data_store.bq_schema import (
    convert_bq_schema_field_list_to_dict
)


def test_should_convert_bq_schema_field_list_to_dict():
    source_data = [
        {'mode': 'NULLABLE', 'name': '_type', 'type': 'STRING'},
        {
            'fields': [
                {'mode': 'NULLABLE', 'name': '_type', 'type': 'STRING'},
                {'mode': 'NULLABLE', 'name': 'name', 'type': 'STRING'},
            ],
            'mode': 'NULLABLE',
            'name': 'affiliation',
            'type': 'RECORD',
        },
        {'mode': 'NULLABLE', 'name': 'familyName', 'type': 'STRING'}
    ]
    expected_converted_data = {
        '_type': {'mode': 'NULLABLE', 'name': '_type', 'type': 'STRING'},
        'affiliation': {
            'fields': [
                {'mode': 'NULLABLE', 'name': '_type', 'type': 'STRING'},
                {'mode': 'NULLABLE', 'name': 'name', 'type': 'STRING'},
            ],
            'mode': 'NULLABLE',
            'name': 'affiliation',
            'type': 'RECORD',
        },
        'familyName': {
            'mode': 'NULLABLE', 'name': 'familyName',
            'type': 'STRING'
        }
    }

    returned_data = convert_bq_schema_field_list_to_dict(source_data)

    assert returned_data == expected_converted_data
