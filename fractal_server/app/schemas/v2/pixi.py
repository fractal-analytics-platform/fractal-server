from pydantic import BaseModel

from fractal_server.types import AbsolutePathStr
from fractal_server.types import SemanticVersioning


class PixiVersionCreate(BaseModel):
    version: SemanticVersioning
    path: AbsolutePathStr


class PixiVersionRead(BaseModel):
    version: str
