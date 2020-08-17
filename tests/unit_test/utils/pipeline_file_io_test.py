from data_pipeline.utils.pipeline_file_io import (
    is_remote_path,
    get_temp_local_file_if_remote
)


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
