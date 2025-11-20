from sqlmodel import BOOLEAN
from sqlmodel import CheckConstraint
from sqlmodel import Column
from sqlmodel import Field
from sqlmodel import Index
from sqlmodel import SQLModel
from sqlmodel import String
from sqlmodel import column


class LinkUserProjectV2(SQLModel, table=True):
    """
    Crossing table between User and ProjectV2
    """

    project_id: int = Field(
        foreign_key="projectv2.id", primary_key=True, ondelete="CASCADE"
    )
    user_id: int = Field(foreign_key="user_oauth.id", primary_key=True)

    # TODO-2.18.1 drop server_default
    is_owner: bool = Field(
        sa_column=Column(BOOLEAN, server_default="true", nullable=False)
    )
    # TODO-2.18.1 drop server_default
    is_verified: bool = Field(
        sa_column=Column(BOOLEAN, server_default="true", nullable=False)
    )
    # TODO-2.18.1 drop server_default
    permissions: str = Field(
        sa_column=Column(String, server_default="'rwx'", nullable=False)
    )

    __table_args__ = (
        Index(
            "ix_linkuserprojectv2_one_owner_per_project",
            "project_id",
            unique=True,
            postgresql_where=column("is_owner").is_(True),
        ),
        CheckConstraint(
            "NOT (is_owner AND NOT is_verified)",
            name="owner_is_verified",
        ),
        CheckConstraint(
            "NOT (is_owner AND permissions <> 'rwx')",
            name="owner_full_permissions",
        ),
        CheckConstraint(
            "permissions IN ('r', 'rw', 'rwx')",
            name="valid_permissions",
        ),
    )
