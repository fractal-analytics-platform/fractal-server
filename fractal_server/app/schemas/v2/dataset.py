from datetime import datetime
from typing import Any
from typing import Optional

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field
from pydantic import field_serializer
from pydantic import field_validator
from pydantic import model_validator
from pydantic.types import AwareDatetime

from .._filter_validators import validate_attribute_filters
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
    _attribute_filters = field_validator("attribute_filters")(
        classmethod(validate_attribute_filters)
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
    attribute_filters: AttributeFiltersType

    @field_serializer("timestamp_created")
    def serialize_datetime(v: datetime) -> str:
        return v.isoformat()


class DatasetUpdateV2(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: Optional[NonEmptyString] = None
    zarr_dir: Optional[str] = None
    attribute_filters: Optional[dict[str, list[Any]]] = None

    # Validators

    _dict_keys = model_validator(mode="before")(
        classmethod(root_validate_dict_keys)
    )
    _attribute_filters = field_validator("attribute_filters")(
        classmethod(validate_attribute_filters)
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

    Attributes:
        name:
        zarr_dir:
        images:
        filters:
        attribute_filters:
    """

    model_config = ConfigDict(extra="forbid")

    name: str
    zarr_dir: str
    images: list[SingleImage] = Field(default_factory=list)

    filters: Optional[dict[str, Any]] = None
    attribute_filters: AttributeFiltersType = Field(default_factory=dict)

    @model_validator(mode="before")
    @classmethod
    def update_legacy_filters(cls, values: dict):
        """
        Transform legacy filters (created with fractal-server<2.11.0)
        into attribute/type filters
        """
        if values.get("filters") is not None:
            if "attribute_filters" in values.keys():
                raise ValueError(
                    "Cannot set filters both through the legacy field "
                    "('filters') and the new ones ('attribute_filters')."
                )

            else:
                # Convert legacy filters.types into new filters
                values["attribute_filters"] = {
                    key: [value]
                    for key, value in values["filters"]
                    .get("attributes", {})
                    .items()
                }
                values["filters"] = None

        return values

    _attribute_filters = field_validator("attribute_filters")(
        classmethod(validate_attribute_filters)
    )

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
        attribute_filters:
    """

    name: str
    zarr_dir: str
    images: list[SingleImage]
    attribute_filters: AttributeFiltersType
