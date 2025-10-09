from pydantic import BaseModel

from fractal_server.types import AbsolutePathStr
from fractal_server.types import NonEmptyStr


class _ValidProfileBase(BaseModel):
    pass


class ValidProfileLocal(_ValidProfileBase):
    pass


class ValidProfileSlurmSudo(_ValidProfileBase):
    username: NonEmptyStr


class ValidProfileSlurmSSH(_ValidProfileBase):
    username: NonEmptyStr
    ssh_key_path: AbsolutePathStr
