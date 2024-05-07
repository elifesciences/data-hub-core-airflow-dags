from dateutil.relativedelta import relativedelta

from data_pipeline.google_analytics.ga_config import parse_date_delta_dict


class TestParseDateDeltaDict:
    def test_should_parse_return_empty_delta_by_default(self):
        date_delta = parse_date_delta_dict({})
        assert date_delta == relativedelta()
        assert not date_delta

    def test_should_parse_days(self):
        date_delta = parse_date_delta_dict({'days': 123})
        assert date_delta == relativedelta(days=123)
        assert date_delta

    def test_should_parse_months(self):
        date_delta = parse_date_delta_dict({'months': 123})
        assert date_delta == relativedelta(months=123)
        assert date_delta

    def test_should_parse_years(self):
        date_delta = parse_date_delta_dict({'years': 123})
        assert date_delta == relativedelta(years=123)
        assert date_delta
