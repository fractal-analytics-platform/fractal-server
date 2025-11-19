from enum import StrEnum

from pydantic import BaseModel
from pydantic import EmailStr


class ProjectPermissions(StrEnum):
    """
    Available permissions for accessing Project
    Attributes:
        READ:
        WRITE:
        EXECUTE:
    """

    READ = "r"
    WRITE = "rw"
    EXECUTE = "rwx"


class ProjectShareCreate(BaseModel):
    permissions: ProjectPermissions = ProjectPermissions.READ


class ProjectShareReadOwner(BaseModel):
    user_email: EmailStr
    is_verified: bool
    permissions: ProjectPermissions


class ProjectShareReadGuest(BaseModel):
    project_name: str
    project_id: int
    owner_email: EmailStr
    permissions: ProjectPermissions


class ProjectShareUpdatePermissions(BaseModel):
    permissions: ProjectPermissions = None


class ProjectShareUpdateAccept(BaseModel):
    is_verified: bool = None
