from sqlmodel import Field
from sqlmodel import SQLModel


class Profile(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)
    resource_id: int = Field(foreign_key="resource.id", ondelete="CASCADE")

    username: str | None = None
    ssh_key_path: str | None = None

    remote_jobs_dir: str | None = None
    remote_tasks_dir: str | None = None
