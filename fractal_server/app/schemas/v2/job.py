from datetime import datetime
from enum import StrEnum

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field
from pydantic import field_serializer
from pydantic import model_validator
from pydantic.types import AwareDatetime
from pydantic.types import NonNegativeInt
from pydantic.types import StrictStr

from fractal_server.app.schemas.v2.dumps import DatasetDumpV2
from fractal_server.app.schemas.v2.dumps import ProjectDumpV2
from fractal_server.app.schemas.v2.dumps import WorkflowDumpV2
from fractal_server.types import AttributeFilters
from fractal_server.types import NonEmptyStr
from fractal_server.types import TypeFilters


class JobStatusTypeV2(StrEnum):
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

    first_task_index: NonNegativeInt | None = None
    last_task_index: NonNegativeInt | None = None
    slurm_account: StrictStr | None = None
    worker_init: NonEmptyStr = None

    attribute_filters: AttributeFilters = Field(default_factory=dict)
    type_filters: TypeFilters = Field(default_factory=dict)

    @model_validator(mode="before")
    @classmethod
    def validate_first_last_indices(cls, values):
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
    project_id: int | None = None
    project_dump: ProjectDumpV2
    user_email: str
    slurm_account: str | None = None
    workflow_id: int | None = None
    workflow_dump: WorkflowDumpV2
    dataset_id: int | None = None
    dataset_dump: DatasetDumpV2
    start_timestamp: AwareDatetime
    end_timestamp: AwareDatetime | None = None
    status: str
    log: str | None = None
    working_dir: str | None = None
    working_dir_user: str | None = None
    first_task_index: int | None = None
    last_task_index: int | None = None
    worker_init: str | None = None
    attribute_filters: AttributeFilters
    type_filters: dict[str, bool]

    @field_serializer("start_timestamp")
    def serialize_datetime_start(v: datetime) -> str:
        return v.isoformat()

    @field_serializer("end_timestamp")
    def serialize_datetime_end(v: datetime | None) -> str | None:
        if v is None:
            return None
        else:
            return v.isoformat()


class JobUpdateV2(BaseModel):
    model_config = ConfigDict(extra="forbid")

    status: JobStatusTypeV2
