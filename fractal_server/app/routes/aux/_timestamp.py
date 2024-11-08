from datetime import datetime
from datetime import timezone

from fastapi import HTTPException
from fastapi import status

from fractal_server.config import get_settings
from fractal_server.syringe import Inject


def _convert_to_db_timestamp(dt: datetime) -> datetime:
    """
    This function takes a timezone-aware datetime and converts it to UTC.
    If using SQLite, it also removes the timezone information in order to make
    the datetime comparable with datetimes in the database.
    """
    if dt.tzinfo is None:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"The timestamp provided has no timezone information: {dt}",
        )
    _dt = dt.astimezone(timezone.utc)
    if Inject(get_settings).DB_ENGINE == "sqlite":
        return _dt.replace(tzinfo=None)
    return _dt
