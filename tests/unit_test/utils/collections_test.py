from data_pipeline.utils.collections import iter_batches_iterable


class TestIterBatchesIterable:
    def test_should_batch_list(self):
        assert list(iter_batches_iterable(
            [0, 1, 2, 3, 4],
            2
        )) == [[0, 1], [2, 3], [4]]

    def test_should_batch_iterable(self):
        assert list(iter_batches_iterable(
            iter([0, 1, 2, 3, 4]),
            2
        )) == [[0, 1], [2, 3], [4]]
