from sqlalchemy import text
from sqlmodel import Field
from sqlmodel import Index
from sqlmodel import SQLModel


class FakeTable(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)
    a: int
    b: bool

    __table_args__ = (
        Index(
            "fancy_name",
            "a",
            unique=True,
            postgresql_where=text("b IS TRUE"),
        ),
    )
