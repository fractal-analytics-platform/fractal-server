from pydantic import BaseModel
from pydantic.types import AwareDatetime
from pydantic.types import PositiveInt

from fractal_server.app.schemas.v2.workflow import WorkflowExport
from fractal_server.types import NonEmptyStr


class WorkflowTemplateRead(BaseModel):
    id: int

    user_email: str
    name: str
    version: int

    timestamp_created: AwareDatetime

    user_group_id: int | None = None

    description: str | None = None
    data: WorkflowExport


class WorkflowTemplateCreate(BaseModel):
    name: NonEmptyStr
    version: PositiveInt
    description: NonEmptyStr | None = None


class WorkflowTemplateFile(WorkflowTemplateCreate):
    data: WorkflowExport


class WorkflowTemplateUpdate(BaseModel):
    user_group_id: int | None = None
    description: NonEmptyStr | None = None
