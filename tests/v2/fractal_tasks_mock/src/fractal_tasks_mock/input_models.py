from typing import Literal
from typing import Optional

from pydantic import BaseModel
from pydantic import Field


class InitArgsRegistration(BaseModel):
    """
    Dummy model description.

    Attributes:
        ref_zarr_url: dummy attribute description.
    """

    ref_zarr_url: str


class InitArgsCellVoyager(BaseModel):
    """
    Dummy model description.

    Attributes:
        raw_zarr_url: dummy attribute description.
        acquisition: dummy attribute description.
    """

    raw_zarr_url: str
    acquisition: Optional[int] = None


class InitArgsGeneric(BaseModel):
    """
    Dummy model description.

    Attributes:
        argument: dummy attribute description.
    """

    argument: int


class InitArgsIllumination(BaseModel):
    """
    Dummy model description.

    Attributes:
        raw_zarr_url: dummy attribute description.
        subsets: dummy attribute description.
    """

    raw_zarr_url: str
    subsets: dict[Literal["C_index"], int] = Field(default_factory=dict)


class InitArgsMIP(BaseModel):
    """
    Dummy model description.

    Attributes:
        new_zarr_url: dummy attribute description.
        new_plate: dummy attribute description.
    """

    new_zarr_url: str
    new_plate: str  # FIXME: remove this
