from datetime import datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field
from pydantic import field_serializer
from pydantic import model_validator
from pydantic.types import AwareDatetime
from pydantic.types import NonNegativeInt
from pydantic.types import StrictStr

from ....types._common_validators import root_validate_dict_keys
from ....types._validated_types import AttributeFilters
from ....types._validated_types import DictStrBool
from ....types._validated_types import NonEmptyString
from .dumps import DatasetDumpV2
from .dumps import ProjectDumpV2
from .dumps import WorkflowDumpV2


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

    first_task_index: Optional[NonNegativeInt] = None
    last_task_index: Optional[NonNegativeInt] = None
    slurm_account: Optional[StrictStr] = None
    worker_init: NonEmptyString = None

    attribute_filters: AttributeFilters = Field(default_factory=dict)
    type_filters: DictStrBool = Field(default_factory=dict)

    @model_validator(mode="before")
    @classmethod
    def root_validate(cls, values):
        values = root_validate_dict_keys(values)
        first_task_index = values.get("first_task_index")
        last_task_index = values.get("last_task_index")

        if first_task_index is not None and last_task_index is not None:
            if first_task_index > last_task_index:
                raise ValueError(
                    f"{first_task_index=} cannot be larger than "
                    f"{last_task_index=}"
                )
        return values


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
    attribute_filters: AttributeFilters
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
