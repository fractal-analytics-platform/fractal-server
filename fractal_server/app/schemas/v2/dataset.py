from datetime import datetime
from typing import Optional

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field
from pydantic import field_serializer
from pydantic import field_validator
from pydantic import model_validator
from pydantic.types import AwareDatetime

from .._validators import cant_set_none
from .._validators import NonEmptyString
from .._validators import root_validate_dict_keys
from .project import ProjectReadV2
from fractal_server.images import SingleImage
from fractal_server.images.models import AttributeFiltersType
from fractal_server.urls import normalize_url


class DatasetCreateV2(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: NonEmptyString

    zarr_dir: Optional[str] = None

    attribute_filters: AttributeFiltersType = Field(default_factory=dict)

    # Validators

    _dict_keys = model_validator(mode="before")(
        classmethod(root_validate_dict_keys)
    )

    @field_validator("zarr_dir")
    @classmethod
    def normalize_zarr_dir(cls, v: Optional[str]) -> Optional[str]:
        if v is not None:
            return normalize_url(v)
        return v


class DatasetReadV2(BaseModel):
    id: int
    name: str

    project_id: int
    project: ProjectReadV2

    timestamp_created: AwareDatetime

    zarr_dir: str

    @field_serializer("timestamp_created")
    def serialize_datetime(v: datetime) -> str:
        return v.isoformat()


class DatasetUpdateV2(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: Optional[NonEmptyString] = None
    zarr_dir: Optional[str] = None

    # Validators

    _dict_keys = model_validator(mode="before")(
        classmethod(root_validate_dict_keys)
    )

    @field_validator("name")
    @classmethod
    def _cant_set_none(cls, v):
        return cant_set_none(v)

    @field_validator("zarr_dir")
    @classmethod
    def normalize_zarr_dir(cls, v: Optional[str]) -> Optional[str]:
        if v is not None:
            return normalize_url(v)
        return v


class DatasetImportV2(BaseModel):
    """
    Class for `Dataset` import.

    We are dropping `model_config = ConfigDict(extra="forbid")` so that any
    kind of legacy filters can be included in the payload, and ignored in the
    API.

    Attributes:
        name:
        zarr_dir:
        images:
    """

    name: str
    zarr_dir: str
    images: list[SingleImage] = Field(default_factory=list)

    @field_validator("zarr_dir")
    @classmethod
    def normalize_zarr_dir(cls, v: str) -> str:
        return normalize_url(v)


class DatasetExportV2(BaseModel):
    """
    Class for `Dataset` export.

    Attributes:
        name:
        zarr_dir:
        images:
    """

    name: str
    zarr_dir: str
    images: list[SingleImage]
