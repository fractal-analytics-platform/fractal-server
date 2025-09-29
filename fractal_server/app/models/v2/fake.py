from sqlmodel import Field
from sqlmodel import SQLModel


class FakeTable(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)
    a: int
    b: bool
