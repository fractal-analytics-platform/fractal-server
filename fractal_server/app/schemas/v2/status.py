from typing import Optional

from pydantic import BaseModel

from .workflowtask import WorkflowTaskStatusTypeV2


class StatusReadV2(BaseModel):
    """
    Response type for the
    `/project/{project_id}/status/` endpoint
    """

    status: Optional[
        dict[
            str,
            WorkflowTaskStatusTypeV2,
        ]
    ] = None
