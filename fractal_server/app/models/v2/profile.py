from sqlmodel import Field
from sqlmodel import SQLModel


class Profile(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)
    resource_id: int = Field(foreign_key="resource.id", ondelete="CASCADE")

    username: str | None = None
    ssh_key_path: str | None = None

    jobs_remote_dir: str | None = None
    tasks_remote_dir: str | None = None
