from typing import Annotated
from typing import Any

from pydantic import BaseModel
from pydantic import Discriminator
from pydantic import Tag
from pydantic import validate_call

from .resource import ResourceType
from fractal_server.types import AbsolutePathStr
from fractal_server.types import NonEmptyStr


class ValidProfileLocal(BaseModel):
    name: NonEmptyStr
    resource_type: ResourceType
    username: None = None
    ssh_key_path: None = None
    jobs_remote_dir: None = None
    tasks_remote_dir: None = None


class ValidProfileSlurmSudo(BaseModel):
    name: NonEmptyStr
    resource_type: ResourceType
    username: NonEmptyStr
    ssh_key_path: None = None
    jobs_remote_dir: None = None
    tasks_remote_dir: None = None


class ValidProfileSlurmSSH(BaseModel):
    name: NonEmptyStr
    resource_type: ResourceType
    username: NonEmptyStr
    ssh_key_path: AbsolutePathStr
    jobs_remote_dir: AbsolutePathStr
    tasks_remote_dir: AbsolutePathStr


def get_discriminator_value(v: Any) -> str:
    # See https://github.com/fastapi/fastapi/discussions/12941
    if isinstance(v, dict):
        return v.get("resource_type", None)
    return getattr(v, "resource_type", None)


ProfileCreate = Annotated[
    Annotated[ValidProfileLocal, Tag(ResourceType.LOCAL)]
    | Annotated[ValidProfileSlurmSudo, Tag(ResourceType.SLURM_SUDO)]
    | Annotated[ValidProfileSlurmSSH, Tag(ResourceType.SLURM_SSH)],
    Discriminator(get_discriminator_value),
]


class ProfileRead(BaseModel):
    id: int
    name: str
    resource_id: int
    resource_type: str
    username: str | None = None
    ssh_key_path: str | None = None
    jobs_remote_dir: str | None = None
    tasks_remote_dir: str | None = None


@validate_call
def cast_serialize_profile(_data: ProfileCreate) -> dict[str, Any]:
    """
    Cast/serialize round-trip for `Profile` data.

    We use `@validate_call` because `ProfileeCreate` is a `Union` type and it
    cannot be instantiated directly.

    Return:
        Serialized version of a valid profile object.
    """
    return _data.model_dump()
