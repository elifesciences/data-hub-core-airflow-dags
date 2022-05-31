from datetime import datetime

from data_pipeline.utils.json import (
    get_json_compatible_value,
    get_recursive_json_compatible_value
)


# reformatting to avoid issues with changes to formatting
DATETIME_STR_1 = datetime.fromisoformat('2021-02-03T04:05:06+00:00').isoformat()


class TestGetJsonCompatibleValue:
    def test_should_not_change_str(self):
        assert get_json_compatible_value('test123') == 'test123'

    def test_should_format_datetime(self):
        assert get_json_compatible_value(datetime.fromisoformat(DATETIME_STR_1)) == DATETIME_STR_1


class TestGetRecursiveJsonCompatibleValue:
    def test_should_format_datetime_within_a_dict(self):
        assert get_recursive_json_compatible_value(
            {'key': datetime.fromisoformat(DATETIME_STR_1)}
        ) == {'key': DATETIME_STR_1}

    def test_should_format_datetime_within_a_list(self):
        assert get_recursive_json_compatible_value(
            [datetime.fromisoformat(DATETIME_STR_1)]
        ) == [DATETIME_STR_1]
