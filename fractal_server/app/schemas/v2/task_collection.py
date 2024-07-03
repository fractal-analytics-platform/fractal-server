import logging
from pathlib import Path
from typing import Literal
from typing import Optional

from pydantic import BaseModel
from pydantic import validator

from .._validators import valdictkeys
from .._validators import valstr


class TaskCollectPipV2(BaseModel):
    """
    TaskCollectPipV2 class

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
    python_version: Optional[Literal["3.9", "3.10", "3.11", "3.12"]] = None
    pinned_package_versions: Optional[dict[str, str]] = None

    _pinned_package_versions = validator(
        "pinned_package_versions", allow_reuse=True
    )(valdictkeys("pinned_package_versions"))
    _package_extras = validator("package_extras", allow_reuse=True)(
        valstr("package_extras")
    )
    _python_version = validator("python_version", allow_reuse=True)(
        valstr("python_version")
    )

    @validator("package")
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

    @validator("package_version")
    def package_version_validator(cls, v, values):
        v = valstr("package_version")(v)
        if values["package"].endswith(".whl"):
            logging.warning(
                "Cannot provide version when package is a Wheel file."
            )
        return v
