from sqlmodel import Field
from sqlmodel import SQLModel


class LinkUserProjectV2(SQLModel, table=True):
    """
    Crossing table between User and ProjectV2
    """

    project_id: int = Field(foreign_key="projectv2.id", primary_key=True)
    user_id: int = Field(foreign_key="user_oauth.id", primary_key=True)
