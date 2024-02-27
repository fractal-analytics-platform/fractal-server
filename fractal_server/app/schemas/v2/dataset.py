from datetime import datetime
from typing import Any
from typing import Optional
from typing import Union

from pydantic import BaseModel
from pydantic import Field
from pydantic import validator

from .._validators import val_absolute_path  # noqa F401
from .._validators import valstr
from .._validators import valutc
from ..dumps import WorkflowTaskDump  # FIXME V2
from ..project import ProjectRead
from ..workflow import WorkflowTaskStatusType  # FIXME V2


def val_scalar_dict(attribute: str):
    def val(
        dict_str_any: dict[str, Any],
    ) -> dict[str, Union[int, float, str, bool, None]]:
        for key, value in dict_str_any.items():
            if not isinstance(value, (int, float, str, bool, type(None))):
                raise ValueError(
                    f"{attribute}[{key}] must be a scalar (int, float, str, "
                    f"bool, or None). Given {value} ({type(value)})"
                )
        return dict_str_any

    return val


class SingleImage(BaseModel):
    path: str
    attributes: dict[str, Any] = Field(default_factory=dict)

    _attributes = validator("attributes", allow_reuse=True)(
        val_scalar_dict("attributes")
    )

    def match_filter(self, filters: Optional[dict[str, Any]] = None):
        if filters is None:
            return True
        for key, value in filters.items():
            if value is None:
                continue
            if self.attributes.get(key) != value:
                return False
        return True


class _DatasetHistoryItemV2(BaseModel):
    """
    Class for an item of `Dataset.history`.
    """

    workflowtask: WorkflowTaskDump  # FIXME V2
    status: WorkflowTaskStatusType
    parallelization: Optional[dict]


class DatasetStatusReadV2(BaseModel):
    """
    Response type for the
    `/project/{project_id}/dataset/{dataset_id}/status/` endpoint
    """

    status: Optional[
        dict[
            int,
            WorkflowTaskStatusType,
        ]
    ] = None


# CRUD


class DatasetCreateV2(BaseModel):

    name: str

    meta: dict[str, Any] = {}
    history: list[_DatasetHistoryItemV2] = []
    read_only: bool = False

    images: list[SingleImage] = []
    filters: dict[str, Any] = {}

    buffer: Optional[dict[str, Any]]
    parallelization_list: Optional[list[dict[str, Any]]]

    # Validators
    _name = validator("name", allow_reuse=True)(valstr("name"))
    _filters = validator("filters", allow_reuse=True)(
        val_scalar_dict("filters")
    )


class DatasetReadV2(BaseModel):

    id: int
    name: str

    project_id: int
    project: ProjectRead

    meta: dict[str, Any]
    history: list[_DatasetHistoryItemV2]
    read_only: bool

    timestamp_created: datetime

    images: list[SingleImage]
    filters: dict[str, Any]
    buffer: Optional[dict[str, Any]]
    parallelization_list: Optional[list[dict[str, Any]]]

    # Validators
    _timestamp_created = validator("timestamp_created", allow_reuse=True)(
        valutc("timestamp_created")
    )
    _filters = validator("filters", allow_reuse=True)(
        val_scalar_dict("filters")
    )


class DatasetUpdateV2(BaseModel):

    name: Optional[str]

    meta: Optional[dict[str, Any]]
    history: Optional[list[_DatasetHistoryItemV2]]
    read_only: Optional[bool]

    images: Optional[list[SingleImage]]
    filters: Optional[dict[str, Any]]
    buffer: Optional[dict[str, Any]]
    parallelization_list: Optional[list[dict[str, Any]]]

    # Validators
    _name = validator("name", allow_reuse=True)(valstr("name"))
    _filters = validator("filters", allow_reuse=True)(
        val_scalar_dict("filters")
    )
