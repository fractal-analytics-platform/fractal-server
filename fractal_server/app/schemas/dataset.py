from typing import Any
from typing import Optional

from pydantic import BaseModel
from pydantic import Field
from pydantic import validator

from ._validators import val_absolute_path
from ._validators import valstr


__all__ = (
    "DatasetUpdate",
    "DatasetCreate",
    "DatasetRead",
    "ResourceCreate",
    "ResourceRead",
    "ResourceUpdate",
)


class _ResourceBase(BaseModel):
    """
    Base class for Resource
    """

    path: str


class ResourceCreate(_ResourceBase):
    # Validators
    _path = validator("path", allow_reuse=True)(val_absolute_path("path"))


class ResourceUpdate(_ResourceBase):
    # Validators
    _path = validator("path", allow_reuse=True)(val_absolute_path("path"))


class ResourceRead(_ResourceBase):
    id: int
    dataset_id: int


class _DatasetBase(BaseModel):
    """
    Base class for Dataset

    Attributes:
        name: TBD
        type: TBD
        meta: TBD
        read_only: TBD
    """

    name: str
    type: Optional[str]
    meta: dict[str, Any] = Field(default={})
    read_only: bool = False


class DatasetUpdate(_DatasetBase):
    name: Optional[str]
    meta: Optional[dict[str, Any]] = None
    read_only: Optional[bool]

    # Validators
    _name = validator("name", allow_reuse=True)(valstr("name"))
    _type = validator("type", allow_reuse=True)(valstr("type"))


class DatasetCreate(_DatasetBase):
    # Validators
    _name = validator("name", allow_reuse=True)(valstr("name"))
    _type = validator("type", allow_reuse=True)(valstr("type"))


class DatasetRead(_DatasetBase):
    id: int
    resource_list: list[ResourceRead]
    project_id: int
    read_only: bool
