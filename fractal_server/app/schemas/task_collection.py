from pathlib import Path
from typing import Literal
from typing import Optional

from pydantic import BaseModel
from pydantic import Field
from pydantic import field_validator
from pydantic import validator

from ._validators import valstr
from .task import TaskRead

__all__ = (
    "TaskCollectPip",
    "TaskCollectStatus",
)


class _TaskCollectBase(BaseModel):
    """
    Base class for `TaskCollectPip`.
    """

    pass


class TaskCollectPip(_TaskCollectBase):
    """
    TaskCollectPip class

    This class only encodes the attributes required to trigger a
    task-collection operation. Other attributes (that are assigned *during*
    task collection) are defined as part of fractal-server.

    Two cases are supported:

        1. `package` is the path of a local wheel file;
        2. `package` is the name of a package that can be installed via `pip`.


    Attributes:
        package:
            The name of a `pip`-installable package, or the path to a local
            wheel file.
        package_version: Version of the package
        package_extras: Package extras to include in the `pip install` command
        python_version: Python version to install and run the package tasks
        pinned_package_versions:
            dictionary 'package':'version' used to pin versions for specific
            packages.

    """

    package: str
    package_version: Optional[str] = None
    package_extras: Optional[str] = None
    python_version: Optional[str] = None
    pinned_package_versions: Optional[dict[str, str]] = None

    _package_version = validator("package_version", allow_reuse=True)(
        valstr("package_version")
    )
    _package_extras = validator("package_extras", allow_reuse=True)(
        valstr("package_extras")
    )
    _python_version = validator("python_version", allow_reuse=True)(
        valstr("python_version")
    )

    @field_validator("package")
    @classmethod
    def package_validator(cls, value):
        if "/" in value:
            if not value.endswith(".whl"):
                raise ValueError(
                    "Local-package path must be a wheel file "
                    f"(given {value})."
                )
            if not Path(value).is_absolute():
                raise ValueError(
                    f"Local-package path must be absolute: (given {value})."
                )
        return value

    # TODO[pydantic]: We couldn't refactor the `validator`, please replace it
    # by `field_validator` manually.
    # Check https://docs.pydantic.dev/dev-v2/migration/#changes-to-validators
    # for more information.
    @validator("package_version")
    def package_version_validator(cls, v, values):
        if v is not None:
            if values["package"].endswith(".whl"):
                raise ValueError(
                    "Cannot provide version when package is a Wheel file."
                )
        return v


class TaskCollectStatus(_TaskCollectBase):
    """
    TaskCollectStatus class

    Attributes:
        status:
        package:
        venv_path:
        task_list:
        log:
        info:
    """

    status: Literal["pending", "installing", "collecting", "fail", "OK"]
    package: str
    venv_path: Path
    task_list: Optional[list[TaskRead]] = Field(default=[])
    log: Optional[str] = None
    info: Optional[str] = None

    def sanitised_dict(self):
        """
        Return `self.dict()` after casting `self.venv_path` to a string
        """
        d = self.dict()
        d["venv_path"] = str(self.venv_path)
        return d
