from enum import StrEnum

from pydantic import BaseModel
from pydantic import Field


class WorkflowTaskStatusTypeV2(StrEnum):
    """
    Define the available values for the status of a `WorkflowTask`.

    This model is used within the `Dataset.history` attribute, which is
    constructed in the runner and then used in the API (e.g. in the
    `api/v2/project/{project_id}/dataset/{dataset_id}/status` endpoint).

    Attributes:
        SUBMITTED: The `WorkflowTask` is part of a running job.
        DONE: The most-recent execution of this `WorkflowTask` was successful.
        FAILED: The most-recent execution of this `WorkflowTask` failed.
    """

    SUBMITTED = "submitted"
    DONE = "done"
    FAILED = "failed"


class LegacyStatusReadV2(BaseModel):
    """
    Response type for the
    `/project/{project_id}/status/` endpoint
    """

    status: dict[
        str,
        WorkflowTaskStatusTypeV2,
    ] = Field(default_factory=dict)
