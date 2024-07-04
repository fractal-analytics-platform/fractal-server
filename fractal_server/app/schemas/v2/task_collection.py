import logging
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any
from typing import Literal
from typing import Optional

from pydantic import BaseModel
from pydantic import validator

from .._validators import valdictkeys
from .._validators import valstr
from fractal_server.app.schemas._validators import valutc
from fractal_server.app.schemas.v2 import ManifestV2


class CollectionStatusV2(str, Enum):
    PENDING = "pending"
    INSTALLING = "installing"
    COLLECTING = "collecting"
    FAIL = "fail"
    OK = "OK"


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


class TaskCollectCustomV2(BaseModel):
    manifest: ManifestV2
    python_interpreter: str
    source: str
    package_root: Optional[str]
    package_name: Optional[str]
    version: Optional[str]

    _package_name = validator("package_name", allow_reuse=True)(
        valstr("package_name", accept_none=True)
    )

    @validator("package_name")
    def package_name_prevent_injection(cls, value):
        if (value is not None) and (";" in value):
            raise ValueError(f"Invalid package_name: {value}")
        return value

    @validator("python_interpreter")
    def package_validator(cls, value):
        if not Path(value).is_absolute():
            raise ValueError(
                f"Python interpreter path must be absolute: (given {value})."
            )
        return value


class CollectionStateReadV2(BaseModel):

    id: Optional[int]
    data: dict[str, Any]
    timestamp: datetime

    _timestamp = validator("timestamp", allow_reuse=True)(valutc("timestamp"))
