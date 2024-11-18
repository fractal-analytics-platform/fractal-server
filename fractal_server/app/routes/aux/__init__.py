from datetime import datetime


def has_timezone(timestamp: datetime) -> bool:
    """
    Returns `True` if `timestamp` is timezone aware
    """
    return (timestamp.tzinfo is not None) and (
        timestamp.tzinfo.utcoffset(timestamp) is not None
    )
