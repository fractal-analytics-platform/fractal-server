from datetime import datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field
from pydantic import field_serializer
from pydantic import field_validator
from pydantic import model_validator
from pydantic import ValidationInfo
from pydantic.types import AwareDatetime
from pydantic.types import StrictStr

from .._filter_validators import validate_attribute_filters
from .._filter_validators import validate_type_filters
from .._validators import cant_set_none
from .._validators import NonEmptyString
from .._validators import root_validate_dict_keys
from .dumps import DatasetDumpV2
from .dumps import ProjectDumpV2
from .dumps import WorkflowDumpV2
from fractal_server.images.models import AttributeFiltersType


class JobStatusTypeV2(str, Enum):
    """
    Define the available job statuses

    Attributes:
        SUBMITTED:
            The job was created. This does not guarantee that it was also
            submitted to an executor (e.g. other errors could have prevented
            this), nor that it is actually running (e.g. SLURM jobs could be
            still in the queue).
        DONE:
            The job successfully reached its end.
        FAILED:
            The workflow terminated with an error.
    """

    SUBMITTED = "submitted"
    DONE = "done"
    FAILED = "failed"


class JobCreateV2(BaseModel):
    model_config = ConfigDict(extra="forbid")

    first_task_index: Optional[int] = None
    last_task_index: Optional[int] = None
    slurm_account: Optional[StrictStr] = None
    worker_init: Optional[NonEmptyString] = None

    attribute_filters: AttributeFiltersType = Field(default_factory=dict)
    type_filters: dict[str, bool] = Field(default_factory=dict)

    # Validators

    @field_validator("worker_init")
    @classmethod
    def _cant_set_none(cls, v):
        return cant_set_none(v)

    _dict_keys = model_validator(mode="before")(
        classmethod(root_validate_dict_keys)
    )
    _attribute_filters = field_validator("attribute_filters")(
        classmethod(validate_attribute_filters)
    )
    _type_filters = field_validator("type_filters")(
        classmethod(validate_type_filters)
    )

    @field_validator("first_task_index")
    @classmethod
    def first_task_index_non_negative(cls, v):
        """
        Check that `first_task_index` is non-negative.
        """
        if v is not None and v < 0:
            raise ValueError(
                f"first_task_index cannot be negative (given: {v})"
            )
        return v

    @field_validator("last_task_index")
    @classmethod
    def first_last_task_indices(cls, v, info: ValidationInfo):
        """
        Check that `last_task_index` is non-negative, and that it is not
        smaller than `first_task_index`.
        """
        if v is not None and v < 0:
            raise ValueError(
                f"last_task_index cannot be negative (given: {v})"
            )

        first_task_index = info.data.get("first_task_index")
        last_task_index = v
        if first_task_index is not None and last_task_index is not None:
            if first_task_index > last_task_index:
                raise ValueError(
                    f"{first_task_index=} cannot be larger than "
                    f"{last_task_index=}"
                )
        return v


class JobReadV2(BaseModel):
    id: int
    project_id: Optional[int] = None
    project_dump: ProjectDumpV2
    user_email: str
    slurm_account: Optional[str] = None
    workflow_id: Optional[int] = None
    workflow_dump: WorkflowDumpV2
    dataset_id: Optional[int] = None
    dataset_dump: DatasetDumpV2
    start_timestamp: AwareDatetime
    end_timestamp: Optional[AwareDatetime] = None
    status: str
    log: Optional[str] = None
    working_dir: Optional[str] = None
    working_dir_user: Optional[str] = None
    first_task_index: Optional[int] = None
    last_task_index: Optional[int] = None
    worker_init: Optional[str] = None
    attribute_filters: AttributeFiltersType
    type_filters: dict[str, bool]

    @field_serializer("start_timestamp")
    def serialize_datetime_start(v: datetime) -> str:
        return v.isoformat()

    @field_serializer("end_timestamp")
    def serialize_datetime_end(v: Optional[datetime]) -> Optional[str]:
        if v is None:
            return None
        else:
            return v.isoformat()


class JobUpdateV2(BaseModel):
    model_config = ConfigDict(extra="forbid")

    status: JobStatusTypeV2
