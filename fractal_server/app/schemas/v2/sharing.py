from enum import StrEnum

from pydantic import BaseModel


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
    permissions: ProjectPermissions


class ProjectShareReadOwner(BaseModel):
    guest_email: str
    is_verified: bool
    permissions: str


class ProjectShareReadGuest(BaseModel):
    project_id: int
    project_name: str
    owner_email: str
    guest_permissions: str


class ProjectShareUpdatePermissions(BaseModel):
    permissions: ProjectPermissions = None
