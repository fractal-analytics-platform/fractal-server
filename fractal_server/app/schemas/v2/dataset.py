from datetime import datetime
from typing import Optional

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field
from pydantic import field_serializer
from pydantic import model_validator
from pydantic.types import AwareDatetime

from ....types import AttributeFilters
from ....types import NonEmptyString
from ....types import ZarrDir
from ....types.validators import validate_dict_keys
from .project import ProjectReadV2
from fractal_server.images import SingleImage


class DatasetCreateV2(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: NonEmptyString

    zarr_dir: Optional[ZarrDir] = None

    attribute_filters: AttributeFilters = Field(default_factory=dict)

    @model_validator(mode="before")
    @classmethod
    def _validate_dict_keys(cls, values: dict):
        return validate_dict_keys(values)


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

    name: NonEmptyString = None
    zarr_dir: Optional[ZarrDir] = None

    @model_validator(mode="before")
    @classmethod
    def _validate_dict_keys(cls, values: dict):
        return validate_dict_keys(values)


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
    zarr_dir: ZarrDir
    images: list[SingleImage] = Field(default_factory=list)


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
