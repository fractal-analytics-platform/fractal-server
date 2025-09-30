from sqlmodel import Boolean
from sqlmodel import CheckConstraint
from sqlmodel import Column
from sqlmodel import column
from sqlmodel import Field
from sqlmodel import Index
from sqlmodel import SQLModel
from sqlmodel import true


class LinkUserProjectV2(SQLModel, table=True):
    """
    Crossing table between User and ProjectV2
    """

    project_id: int = Field(foreign_key="projectv2.id", primary_key=True)
    user_id: int = Field(foreign_key="user_oauth.id", primary_key=True)

    is_owner: bool = Field(
        sa_column=Column(Boolean, nullable=False, server_default=true()),
    )
    is_verified: bool = Field(
        sa_column=Column(Boolean, nullable=False, server_default=true()),
    )
    can_write: bool = Field(
        sa_column=Column(Boolean, nullable=False, server_default=true()),
    )
    can_execute: bool = Field(
        sa_column=Column(Boolean, nullable=False, server_default=true()),
    )

    __table_args__ = (
        Index(
            "idx_max_one_owner_per_project",
            "project_id",
            unique=True,
            postgresql_where=column("is_owner").is_(True),
        ),
        CheckConstraint(
            "NOT (is_owner AND NOT is_verified)",
            name="chk_owner_must_be_verified",
        ),
        CheckConstraint(
            "NOT (can_execute AND NOT can_write)",
            name="chk_execute_implies_write",
        ),
    )
