from datetime import datetime

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field
from pydantic import field_serializer
from pydantic.types import AwareDatetime

from fractal_server.app.schemas.v2.project import ProjectReadV2
from fractal_server.images import SingleImage
from fractal_server.types import AttributeFilters
from fractal_server.types import NonEmptyStr
from fractal_server.types import ZarrDirStr


class DatasetCreateV2(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: NonEmptyStr

    zarr_dir: ZarrDirStr | None = None

    attribute_filters: AttributeFilters = Field(default_factory=dict)


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

    name: NonEmptyStr = None
    zarr_dir: ZarrDirStr | None = None


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
    zarr_dir: ZarrDirStr
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
