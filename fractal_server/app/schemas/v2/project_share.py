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


class ProjectShareRead(BaseModel):
    user_email: EmailStr
    is_verified: bool
    permissions: ProjectPermissions


class ProjectShareUpdate(BaseModel):
    permissions: ProjectPermissions = None
