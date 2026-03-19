from typing import Literal
from typing import Optional

from pydantic import BaseModel
from pydantic import Field


class InitArgsRegistration(BaseModel):
    """
    Dummy model description.
    """

    ref_zarr_url: str
    """
    dummy attribute description.
    """


class InitArgsCellVoyager(BaseModel):
    """
    Dummy model description.
    """

    raw_zarr_url: str
    """
    dummy attribute description.
    """
    acquisition: Optional[int] = None
    """
    dummy attribute description.
    """


class InitArgsGeneric(BaseModel):
    """
    Dummy model description.
    """

    argument: int
    """
    dummy attribute description.
    """
    ind: int
    """
    dummy attribute description.
    """


class InitArgsIllumination(BaseModel):
    """
    Dummy model description.
    """

    raw_zarr_url: str
    """
    dummy attribute description.
    """
    subsets: dict[Literal["C_index"], int] = Field(default_factory=dict)
    """
    dummy attribute description.
    """


class InitArgsMIP(BaseModel):
    """
    Dummy model description.
    """

    new_zarr_url: str
    """
    dummy attribute description.
    """
    new_plate: str
    """
    dummy attribute description.
    """
