from sqlalchemy import column
from sqlmodel import Field
from sqlmodel import Index
from sqlmodel import SQLModel


class MyTable(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)
    a: int
    b: bool

    __table_args__ = (
        Index(
            "custom_index_name",
            "a",
            unique=True,
            postgresql_where=column("b").is_(True),
        ),
    )
