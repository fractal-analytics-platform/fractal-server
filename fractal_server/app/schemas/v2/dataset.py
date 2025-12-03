from datetime import datetime

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field
from pydantic import field_serializer
from pydantic import model_validator
from pydantic.types import AwareDatetime

from fractal_server.app.schemas.v2.project import ProjectRead
from fractal_server.images import SingleImage
from fractal_server.types import AbsolutePathStr
from fractal_server.types import NonEmptyStr
from fractal_server.types import RelativePathStr
from fractal_server.types import ZarrDirStr


class DatasetCreate(BaseModel):
    """
    DatasetCreate

    Attributes:
        name:
        project_dir:
        zarr_subfolder:
    """

    model_config = ConfigDict(extra="forbid")

    name: NonEmptyStr
    project_dir: AbsolutePathStr | None = None
    zarr_subfolder: RelativePathStr | None = None

    @model_validator(mode="before")
    @classmethod
    def validate_zarr_dir(cls, values):
        project_dir = values.get("project_dir")
        zarr_subfolder = values.get("zarr_subfolder")

        if (project_dir is None) and (zarr_subfolder is not None):
            raise ValueError(
                "Cannot provide `zarr_subfolder` without `project_dir`"
            )
        return values


class DatasetRead(BaseModel):
    """
    DatasetRead

    Attributes:
        id:
        name:
        project_id:
        project:
        timestamp_created:
        zarr_dir:
    """

    id: int
    name: str

    project_id: int
    project: ProjectRead

    timestamp_created: AwareDatetime

    zarr_dir: str

    @field_serializer("timestamp_created")
    def serialize_datetime(v: datetime) -> str:
        return v.isoformat()


class DatasetUpdate(BaseModel):
    """
    DatasetUpdate

    Attributes:
        name:
        zarr_dir:
    """

    model_config = ConfigDict(extra="forbid")

    name: NonEmptyStr = None
    zarr_dir: ZarrDirStr | None = None


class DatasetImport(BaseModel):
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


class DatasetExport(BaseModel):
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
