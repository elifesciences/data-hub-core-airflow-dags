from typing import Iterable, Union, T

import pytest
from data_pipeline.utils.collections import (
    iter_batches_iterable,
    iter_item_until_exception
)


def _iter_item_or_raise_exception(
    iterable: Iterable[Union[T, BaseException]]
) -> Iterable[T]:
    for item in iterable:
        if isinstance(item, BaseException):
            raise item
        yield item


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


class TestIterItemUntilException:
    def test_should_yield_no_item_if_passed_in_iterable_was_empty(self):
        assert not list(iter_item_until_exception(iter([]), BaseException))

    def test_should_yield_from_passed_in_iterable_if_no_exception_was_raised(self):
        assert list(iter_item_until_exception(iter([1, 2]), BaseException)) == [1, 2]

    def test_should_yield_items_from_iterable_until_matched_exception(self):
        assert list(iter_item_until_exception(
            _iter_item_or_raise_exception([1, 2, BaseException(), 4]),
            BaseException
        )) == [1, 2]

    def test_should_reraise_exception_if_the_exception_does_not_match(self):
        with pytest.raises(BaseException):
            list(iter_item_until_exception(
                _iter_item_or_raise_exception([1, 2, BaseException(), 4]),
                RuntimeError
            ))
