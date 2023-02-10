from itertools import islice
from collections import deque
from typing import Any, Deque, Iterable, Type, TypeVar

import logging

LOGGER = logging.getLogger(__name__)

T = TypeVar('T')


def chain_queue_and_iterable(queue: deque, iterable):
    while queue:
        yield queue.popleft()
    yield from iterable


def consume_iterable(iterable: Iterable[Any]) -> None:
    for _ in iterable:
        pass


def iter_batch_iterable(
    iterable: Iterable[T],
    batch_size: int
) -> Iterable[Iterable[T]]:
    iterator = iter(iterable)
    while True:
        batch_iterable = islice(iterator, batch_size)
        try:
            peeked_value_queue: Deque[T] = deque()
            peeked_value_queue.append(next(batch_iterable))
            # by using and consuming a queue we are allowing the memory
            # for the peeked value to be released
            batch_iterable = chain_queue_and_iterable(peeked_value_queue, batch_iterable)
        except StopIteration:
            # reached end, avoid yielding an empty iterable
            return
        yield batch_iterable


def iter_item_until_exception(
    iterable: Iterable[T],
    exception_type: Type[BaseException]
) -> Iterable[T]:
    try:
        yield from iterable
    except exception_type as err:
        LOGGER.info('Stopping iteration. Exception occured is: %r', err)
