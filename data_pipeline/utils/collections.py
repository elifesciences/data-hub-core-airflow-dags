from itertools import islice
from typing import Iterable, T

import logging

LOGGER = logging.getLogger(__name__)


def iter_batches_iterable(
        long_list: Iterable[T],
        batch_size: int) -> Iterable[Iterable[T]]:
    iterator = iter(long_list)
    while True:
        batch = list(islice(iterator, batch_size))
        if not batch:
            return
        yield batch


def iter_item_until_exception(
    iterable: Iterable[T],
    exception_type: BaseException
) -> Iterable[T]:
    try:
        yield from iterable
    except exception_type as err:
        LOGGER.info('Stopping iteration. Exception occured is: %r', err)
