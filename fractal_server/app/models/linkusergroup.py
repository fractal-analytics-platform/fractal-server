from sqlmodel import Field
from sqlmodel import SQLModel


class LinkUserGroup(SQLModel, table=True):
    """
    Crossing table between User and UserGroup
    """

    group_id: int = Field(foreign_key="usergroup.id", primary_key=True)
    user_id: int = Field(foreign_key="user_oauth.id", primary_key=True)
