from datetime import datetime, timedelta, timezone


# copied and adapted from airflow for backwards compatibility
def days_ago(days: int, hour: int = 0, minute: int = 0, second: int = 0, microsecond: int = 0):
    """
    Get a datetime object representing `n` days ago. By default the time is
    set to midnight.
    """
    today = datetime.now(timezone.utc).replace(
        hour=hour,
        minute=minute,
        second=second,
        microsecond=microsecond
    )
    return today - timedelta(days=days)
