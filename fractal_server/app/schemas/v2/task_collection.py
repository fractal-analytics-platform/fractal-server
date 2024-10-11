from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any
from typing import Literal
from typing import Optional

from pydantic import BaseModel
from pydantic import Extra
from pydantic import root_validator
from pydantic import validator

from .._validators import valdictkeys
from .._validators import valstr
from fractal_server.app.schemas._validators import valutc
from fractal_server.app.schemas.v2 import ManifestV2
from fractal_server.string_tools import validate_cmd


class CollectionStatusV2(str, Enum):
    PENDING = "pending"
    INSTALLING = "installing"
    COLLECTING = "collecting"
    FAIL = "fail"
    OK = "OK"


class TaskCollectPipV2(BaseModel, extra=Extra.forbid):
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

    _package = validator("package", allow_reuse=True)(valstr("package"))
    _package_version = validator("package_version", allow_reuse=True)(
        valstr("package_version")
    )
    _pinned_package_versions = validator(
        "pinned_package_versions", allow_reuse=True
    )(valdictkeys("pinned_package_versions"))
    _package_extras = validator("package_extras", allow_reuse=True)(
        valstr("package_extras")
    )

    @validator("package")
    def package_validator(cls, value):
        if "/" in value or value.endswith(".whl"):
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
            raise ValueError(
                "Cannot provide package version when package is a wheel file."
            )
        return v


class TaskCollectCustomV2(BaseModel, extra=Extra.forbid):
    """
    Attributes:
        manifest: Manifest of a Fractal task package (this is typically the
            content of `__FRACTAL_MANIFEST__.json`).
        python_interpreter: Absolute path to the Python interpreter to be used
            for running tasks.
        name: A name identifying this package, that will fill the
            `TaskGroupV2.pkg_name` column.
        package_root: The folder where the package is installed.
            If not provided, it will be extracted via `pip show`
            (requires `package_name` to be set).
        package_name: Name of the package, as used for `import <package_name>`;
            this is then used to extract the package directory (`package_root`)
            via `pip show <package_name>`.
        version: Optional version of tasks to be collected.
    """

    manifest: ManifestV2
    python_interpreter: str
    label: str
    package_root: Optional[str]
    package_name: Optional[str]
    version: Optional[str]

    # Valstr
    _python_interpreter = validator("python_interpreter", allow_reuse=True)(
        valstr("python_interpreter")
    )
    _label = validator("label", allow_reuse=True)(valstr("label"))
    _package_root = validator("package_root", allow_reuse=True)(
        valstr("package_root", accept_none=True)
    )
    _package_name = validator("package_name", allow_reuse=True)(
        valstr("package_name", accept_none=True)
    )
    _version = validator("version", allow_reuse=True)(
        valstr("version", accept_none=True)
    )

    @root_validator(pre=True)
    def one_of_package_root_or_name(cls, values):
        package_root = values.get("package_root")
        package_name = values.get("package_name")
        if (package_root is None and package_name is None) or (
            package_root is not None and package_name is not None
        ):
            raise ValueError(
                "One and only one must be set between "
                "'package_root' and 'package_name'"
            )
        return values

    @validator("package_name")
    def package_name_validator(cls, value: str):
        """
        Remove all whitespace characters, then check for invalid code.
        """
        if value is not None:
            validate_cmd(value)
            value = valstr("package_name")(value)
            value = value.replace(" ", "")
        return value

    @validator("package_root")
    def package_root_validator(cls, value):
        if (value is not None) and (not Path(value).is_absolute()):
            raise ValueError(
                f"'package_root' must be an absolute path: (given {value})."
            )
        return value

    @validator("python_interpreter")
    def python_interpreter_validator(cls, value):
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
