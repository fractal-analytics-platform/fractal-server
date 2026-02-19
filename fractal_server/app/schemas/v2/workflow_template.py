from pydantic import BaseModel
from pydantic.types import AwareDatetime
from pydantic.types import PositiveInt

from fractal_server.app.schemas.v2 import WorkflowExport
from fractal_server.app.schemas.v2 import WorkflowImport
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


class WorkflowTemplateImport(BaseModel):
    name: NonEmptyStr
    version: PositiveInt
    description: NonEmptyStr | None = None
    data: WorkflowImport


class WorkflowTemplateExport(BaseModel):
    name: str
    version: int
    description: str | None = None
    data: WorkflowExport


class WorkflowTemplateUpdate(BaseModel):
    user_group_id: int | None = None
    description: NonEmptyStr | None = None
