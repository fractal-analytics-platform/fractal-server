from typing import Literal
from typing import Optional

from pydantic import BaseModel
from pydantic import Field


class InitArgsRegistration(BaseModel):
    """
    Dummy model description.

    Attributes:
        ref_path: dummy attribute description.
    """

    ref_path: str


class InitArgsCellVoyager(BaseModel):
    """
    Dummy model description.

    Attributes:
        raw_path: dummy attribute description.
        acquisition: dummy attribute description.
    """

    raw_path: str
    acquisition: Optional[int] = None


class InitArgsIllumination(BaseModel):
    """
    Dummy model description.

    Attributes:
        raw_path: dummy attribute description.
        subsets: dummy attribute description.
    """

    raw_path: str
    subsets: dict[Literal["C_index"], int] = Field(default_factory=dict)


class InitArgsMIP(BaseModel):
    """
    Dummy model description.

    Attributes:
        new_path: dummy attribute description.
        new_plate: dummy attribute description.
    """

    new_path: str
    new_plate: str  # FIXME: remove this
