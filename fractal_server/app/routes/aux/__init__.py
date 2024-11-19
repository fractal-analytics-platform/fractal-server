from datetime import datetime

from fastapi import HTTPException
from fastapi import status


def _raise_if_naive_datetime(*timestamps: tuple[datetime | None]) -> None:
    """
    Raise 422 if any not-null argument is a naive `datetime` object:
    https://docs.python.org/3/library/datetime.html#determining-if-an-object-is-aware-or-naive
    """
    for timestamp in filter(None, timestamps):
        if (timestamp.tzinfo is None) or (
            timestamp.tzinfo.utcoffset(timestamp) is None
        ):
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=f"{timestamp=} is naive. You must provide a timezone.",
            )
