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


class ProjectGuestCreate(BaseModel):
    """
    Request body for project-sharing invitation.

    Attributes:
        permissions:
    """

    permissions: ProjectPermissions


class ProjectGuestRead(BaseModel):
    """
    Information about a guest.

    Attributes:
        email: Guest email.
        is_verified: Project/guest verification status.
        permissions: Guest permissions for project.
    """

    email: str
    is_verified: bool
    permissions: str


class ProjectGuestUpdate(BaseModel):
    """
    Request body for updating permissions of an existing guest.

    Attributes:
        permissions: New permissions for guest.
    """

    permissions: ProjectPermissions


class ProjectAccessRead(BaseModel):
    """
    Project-access information for current user.

    Attributes:
        is_owner: Whether current user is owner.
        permissions: Current user permissions.
        owner_email: Email of project owner
    """

    is_owner: bool
    permissions: str
    owner_email: str


class ProjectInvitationRead(BaseModel):
    """
    Info about a pending invitation.

    Attributes:
        project_id:
        project_name:
        owner_email:
        guest_permissions:
    """

    project_id: int
    project_name: str
    owner_email: str
    guest_permissions: str


class LinkUserProjectRead(BaseModel):
    # User info
    user_id: int
    user_email: str
    # Project info
    project_id: int
    project_name: str
    # Permissions
    is_verified: bool
    is_owner: bool
    permissions: str
