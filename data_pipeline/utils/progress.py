from typing import Optional


class ProgressMonitor:
    def __init__(
        self,
        total: Optional[int] = None,
        message_prefix: str = 'progress: '
    ):
        self.total = total
        self.current = 0
        self.message_prefix = message_prefix

    def increment(self, delta: int = 1):
        self.current += delta

    def set_total(self, total: Optional[int]):
        self.total = total

    def is_incomplete(self) -> bool:
        return bool(self.total and self.current < self.total)

    def get_progress_message(self) -> str:
        if self.total:
            percent = 100 * self.current / self.total
            return f'{self.message_prefix}{self.current:,} of {self.total:,} ({percent:.1f}%)'
        return f'{self.message_prefix}{self.current} (unknown total)'

    def __repr__(self):
        return f'{type(self).__name__}(current={self.current}, total={self.total})'

    def __str__(self):
        return self.get_progress_message()
