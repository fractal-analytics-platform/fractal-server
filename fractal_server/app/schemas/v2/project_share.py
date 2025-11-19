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


class ProjectInvitation(BaseModel):
    user_email: EmailStr
    permissions: ProjectPermissions = ProjectPermissions.READ
