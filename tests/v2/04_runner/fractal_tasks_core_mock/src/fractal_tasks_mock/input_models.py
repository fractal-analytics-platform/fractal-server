from typing import Literal
from typing import Optional

from pydantic import BaseModel
from pydantic import Field


class InitArgsRegistration(BaseModel):
    ref_path: str


class InitArgsCellVoyager(BaseModel):
    raw_path: str
    acquisition: Optional[int]


class InitArgsIllumination(BaseModel):
    raw_path: str
    subsets: dict[Literal["C_index"], int] = Field(default_factory=dict)
