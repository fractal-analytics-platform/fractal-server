from datetime import datetime
from typing import Optional

from fastapi import HTTPException
from fastapi import status


def _has_timezone(*timestamps: list[Optional[datetime]]) -> None:
    """
    Raise 422 if any (not-null) `timestamp` is not timezone aware.
    """
    for timestamp in (ts for ts in timestamps if ts is not None):
        if (timestamp.tzinfo is None) or (
            timestamp.tzinfo.utcoffset(timestamp) is None
        ):
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=f"{timestamp=} is not timezone aware.",
            )
