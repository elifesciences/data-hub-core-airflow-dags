import json
from pathlib import Path

from data_pipeline.utils.pipeline_file_io import (
    is_remote_path,
    iter_write_jsonl_to_file,
    get_temp_local_file_if_remote,
    write_jsonl_to_file
)


SUPPLEMENTARY_PLANE_CHAR = '\U0001d460'  # 𝑠 U+1D460, mathematical italic s


class TestWriteJsonlToFile:
    def test_should_write_record_as_jsonl(self, tmp_path: Path):
        output_file = str(tmp_path / 'out.jsonl')
        write_jsonl_to_file([{'key': 'value'}], output_file)
        lines = Path(output_file).read_text(encoding='utf-8').splitlines()
        assert lines == ['{"key": "value"}']

    def test_should_write_multiple_records_as_separate_lines(self, tmp_path: Path):
        output_file = str(tmp_path / 'out.jsonl')
        write_jsonl_to_file([{'key': 'value 1'}, {'key': 'value 2'}], output_file)
        lines = Path(output_file).read_text(encoding='utf-8').splitlines()
        assert [json.loads(line) for line in lines] == [
            {'key': 'value 1'},
            {'key': 'value 2'}
        ]

    def test_should_preserve_supplementary_plane_unicode_characters(self, tmp_path: Path):
        output_file = str(tmp_path / 'out.jsonl')
        write_jsonl_to_file([{'text': SUPPLEMENTARY_PLANE_CHAR}], output_file)
        raw_bytes = Path(output_file).read_bytes()
        assert b'\\ud835' not in raw_bytes
        assert SUPPLEMENTARY_PLANE_CHAR.encode('utf-8') in raw_bytes


class TestIterWriteJsonlToFile:
    def test_should_write_record_as_jsonl(self, tmp_path: Path):
        output_file = str(tmp_path / 'out.jsonl')
        list(iter_write_jsonl_to_file([{'key': 'value'}], output_file))
        lines = Path(output_file).read_text(encoding='utf-8').splitlines()
        assert lines == ['{"key": "value"}']

    def test_should_yield_each_record(self, tmp_path: Path):
        output_file = str(tmp_path / 'out.jsonl')
        records = [{'key': 'value 1'}, {'key': 'value 2'}]
        result = list(iter_write_jsonl_to_file(records, output_file))
        assert result == records

    def test_should_preserve_supplementary_plane_unicode_characters(self, tmp_path: Path):
        output_file = str(tmp_path / 'out.jsonl')
        list(iter_write_jsonl_to_file([{'text': SUPPLEMENTARY_PLANE_CHAR}], output_file))
        raw_bytes = Path(output_file).read_bytes()
        assert b'\\ud835' not in raw_bytes
        assert SUPPLEMENTARY_PLANE_CHAR.encode('utf-8') in raw_bytes


class TestIsRemotePath:
    def test_should_return_false_if_local_absolute_path(self):
        assert not is_remote_path('/path/to/something')

    def test_should_return_false_if_local_relative_path(self):
        assert not is_remote_path('./path/to/something')

    def test_should_return_true_if_remote_s3_path(self):
        assert is_remote_path('s3://bucket/object/key')


class TestGetTempLocalFileIfRemote:
    def test_should_return_passed_in_path_if_local(self):
        with get_temp_local_file_if_remote('/path/to/something') as local_path:
            assert local_path == '/path/to/something'
