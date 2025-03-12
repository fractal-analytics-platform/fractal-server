from pathlib import Path
from typing import Literal
from typing import Optional

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import field_validator
from pydantic import model_validator

from .._validators import NonEmptyString
from fractal_server.app.schemas.v2 import ManifestV2
from fractal_server.string_tools import validate_cmd


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
    package: Optional[NonEmptyString] = None
    package_version: Optional[NonEmptyString] = None
    package_extras: Optional[NonEmptyString] = None
    python_version: Optional[Literal["3.9", "3.10", "3.11", "3.12"]] = None
    pinned_package_versions: Optional[dict[str, str]] = None

    @field_validator("package")
    @classmethod
    def package_validator(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return value
        validate_cmd(value, attribute_name="package")
        return value

    @field_validator("package_version")
    @classmethod
    def package_version_validator(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return value
        validate_cmd(value, attribute_name="package_version")
        return value

    @field_validator("pinned_package_versions")
    @classmethod
    def pinned_package_versions_validator(cls, value):
        if value is None:
            return value

        old_keys = list(value.keys())
        new_keys = [key.strip() for key in old_keys]
        if any(k == "" for k in new_keys):
            raise ValueError(f"Empty string in {new_keys}.")
        if len(new_keys) != len(set(new_keys)):
            raise ValueError(
                f"Dictionary contains multiple identical keys: {value}."
            )
        for old_key, new_key in zip(old_keys, new_keys):
            if new_key != old_key:
                value[new_key] = value.pop(old_key)

        for pkg, version in value.items():
            validate_cmd(pkg)
            validate_cmd(version)
        return value

    @field_validator("package_extras")
    @classmethod
    def package_extras_validator(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return value
        validate_cmd(value, attribute_name="package_extras")
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
    python_interpreter: NonEmptyString
    label: NonEmptyString
    package_root: Optional[NonEmptyString] = None
    package_name: Optional[NonEmptyString] = None
    version: Optional[NonEmptyString] = None

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

    @field_validator("package_name")
    @classmethod
    def package_name_validator(cls, value: str):
        """
        Remove all whitespace characters, then check for invalid code.
        """
        if value is not None:
            validate_cmd(value)
            value = value.replace(" ", "")
        return value

    @field_validator("package_root")
    @classmethod
    def package_root_validator(cls, value):
        if (value is not None) and (not Path(value).is_absolute()):
            raise ValueError(
                f"'package_root' must be an absolute path: (given {value})."
            )
        return value

    @field_validator("python_interpreter")
    @classmethod
    def python_interpreter_validator(cls, value):
        if not Path(value).is_absolute():
            raise ValueError(
                f"Python interpreter path must be absolute: (given {value})."
            )
        return value
