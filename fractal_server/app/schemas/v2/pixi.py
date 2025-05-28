from pathlib import Path

from pydantic import BaseModel
from pydantic.types import AwareDatetime

from fractal_server.types import AbsolutePathStr


class PixiVersionCreate(BaseModel):
    version: str
    path: AbsolutePathStr


class PixiVersionRead(BaseModel):
    id: int
    version: str
    path: Path
    timestamp_created: AwareDatetime
