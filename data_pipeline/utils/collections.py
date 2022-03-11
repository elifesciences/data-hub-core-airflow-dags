from itertools import islice
from typing import Iterable, T


def iter_batches_iterable(
        long_list: Iterable[T],
        batch_size: int) -> Iterable[Iterable[T]]:
    it = iter(long_list)
    while True:
        batch = list(islice(it, batch_size))
        if not batch:
            return
        yield batch
