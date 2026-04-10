from sqlmodel import Field
from sqlmodel import SQLModel


class Example(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)
    example_path: str
