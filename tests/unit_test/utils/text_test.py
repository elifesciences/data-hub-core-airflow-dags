from data_pipeline.utils.text import format_byte_count


class TestFormatByteCount:
    def test_should_format_byte_count_with_thousands_separator(self):
        assert format_byte_count(1234568) == '1,234,568 bytes'
