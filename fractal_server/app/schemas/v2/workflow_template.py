from pydantic import BaseModel
from pydantic.types import AwareDatetime

from fractal_server.app.schemas.v2.workflow import WorkflowExport


class WorkflowTemplateRead(BaseModel):
    id: int

    user_email: str
    name: str
    version: int

    timestamp_created: AwareDatetime

    user_group_id: int | None = None

    description: str | None = None
    data: WorkflowExport
