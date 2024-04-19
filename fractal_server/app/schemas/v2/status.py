from pydantic import BaseModel
from pydantic import Field

from .workflowtask import WorkflowTaskStatusTypeV2


class StatusReadV2(BaseModel):
    """
    Response type for the
    `/project/{project_id}/status/` endpoint
    """

    status: dict[
        str,
        WorkflowTaskStatusTypeV2,
    ] = Field(default_factory=dict)
