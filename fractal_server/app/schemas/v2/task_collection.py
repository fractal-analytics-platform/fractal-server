from typing import Literal

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import field_validator
from pydantic import model_validator

from fractal_server.app.schemas.v2 import ManifestV2
from fractal_server.string_tools import validate_cmd
from fractal_server.types import AbsolutePathStr
from fractal_server.types import DictStrStr
from fractal_server.types import NonEmptyStr


class WheelFile(BaseModel):
    """
    Model for data sent from the endpoint to the background task.
    """

    filename: str
    contents: bytes


class TaskCollectPipV2(BaseModel):
    """
    TaskCollectPipV2 class

    This class only encodes the attributes required to trigger a
    task-collection operation. Other attributes (that are assigned *during*
    task collection) are defined as part of fractal-server.

    Two cases are supported:

        1. `package` is the name of a package that can be installed via `pip`.
        1. `package=None`, and a wheel file is uploaded within the API request.

    Attributes:
        package: The name of a `pip`-installable package, or `None`.
        package_version: Version of the package
        package_extras: Package extras to include in the `pip install` command
        python_version: Python version to install and run the package tasks
        pinned_package_versions:
            dictionary 'package':'version' used to pin versions for specific
            packages.

    """

    model_config = ConfigDict(extra="forbid")
    package: NonEmptyStr | None = None
    package_version: NonEmptyStr | None = None
    package_extras: NonEmptyStr | None = None
    python_version: Literal["3.9", "3.10", "3.11", "3.12"] | None = None
    pinned_package_versions: DictStrStr | None = None

    @field_validator(
        "package", "package_version", "package_extras", mode="after"
    )
    @classmethod
    def validate_commands(cls, value):
        if value is not None:
            validate_cmd(value)
        return value

    @field_validator("pinned_package_versions", mode="after")
    @classmethod
    def validate_pinned_package_versions(cls, value):
        if value is not None:
            for pkg, version in value.items():
                validate_cmd(pkg)
                validate_cmd(version)
        return value


class TaskCollectCustomV2(BaseModel):
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

    model_config = ConfigDict(extra="forbid")
    manifest: ManifestV2
    python_interpreter: AbsolutePathStr
    label: NonEmptyStr
    package_root: AbsolutePathStr | None = None
    package_name: NonEmptyStr | None = None
    version: NonEmptyStr | None = None

    @field_validator("package_name", mode="after")
    @classmethod
    def validate_package_name(cls, value):
        if value is not None:
            validate_cmd(value)
        return value

    @model_validator(mode="before")
    @classmethod
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
