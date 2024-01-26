import re
import shutil
import sys
from pathlib import Path
from typing import Optional

from pydantic import root_validator

from fractal_server.app.schemas import ManifestV1
from fractal_server.app.schemas import TaskCollectPip
from fractal_server.config import get_settings
from fractal_server.syringe import Inject


class _TaskCollectPip(TaskCollectPip):
    """
    Internal TaskCollectPip schema

    Differences with its parent class (`TaskCollectPip`):

        1. We check if the package corresponds to a path in the filesystem, and
           whether it exists (via new validator `check_local_package`, new
           method `is_local_package` and new attribute `package_path`).
        2. We include an additional `package_manifest` attribute.
        3. We expose an additional attribute `package_name`, which is filled
           during task collection.
    """

    package_name: Optional[str] = None
    package_path: Optional[Path] = None
    package_manifest: Optional[ManifestV1] = None

    @property
    def is_local_package(self) -> bool:
        return bool(self.package_path)

    @root_validator(pre=True)
    def check_local_package(cls, values):
        """
        Checks if package corresponds to an existing path on the filesystem

        In this case, the user is providing directly a package file, rather
        than a remote one from PyPI. We set the `package_path` attribute and
        get the actual package name and version from the package file name.
        """
        if "/" in values["package"]:
            package_path = Path(values["package"])
            if not package_path.is_absolute():
                raise ValueError("Package path must be absolute")
            if package_path.exists():
                values["package_path"] = package_path
                (
                    values["package"],
                    values["version"],
                    *_,
                ) = package_path.name.split("-")
            else:
                raise ValueError(f"Package {package_path} does not exist.")
        return values

    @property
    def package_source(self) -> str:
        """
        NOTE: As of PR #1188 in `fractal-server`, the attribute
        `self.package_name` is normalized; this means e.g. that `_` is
        replaced by `-`. To guarantee backwards compatibility with
        `Task.source` attributes created before this change, we still replace
        `-` with `_` upon generation of the `source` attribute, in this
        method.
        """
        if not self.package_name or not self.package_version:
            raise ValueError(
                "Cannot construct `package_source` property with "
                f"{self.package_name=} and {self.package_version=}."
            )
        if self.is_local_package:
            collection_type = "pip_local"
        else:
            collection_type = "pip_remote"

        package_extras = self.package_extras or ""
        if self.python_version:
            python_version = f"py{self.python_version}"
        else:
            python_version = ""  # FIXME: can we allow this?

        source = ":".join(
            (
                collection_type,
                self.package_name.replace("-", "_"),  # see method docstring
                self.package_version,
                package_extras,
                python_version,
            )
        )
        return source

    def check(self):
        """
        Verify that the package has all attributes that are needed to continue
        with task collection
        """
        if not self.package_name:
            raise ValueError("`package_name` attribute is not set")
        if not self.package_version:
            raise ValueError("`package_version` attribute is not set")
        if not self.package_manifest:
            raise ValueError("`package_manifest` attribute is not set")


def get_python_interpreter(version: Optional[str] = None) -> str:
    """
    Return the path to the python interpreter

    Args:
        version: Python version

    Raises:
        ValueError: If the python version requested is not available on the
                    host.

    Returns:
        interpreter: string representing the python executable or its path
    """
    if version:
        interpreter = shutil.which(f"python{version}")
        if not interpreter:
            raise ValueError(
                f"Python version {version} not available on host."
            )
    else:
        interpreter = sys.executable

    return interpreter


def slugify_task_name(task_name: str) -> str:
    return task_name.replace(" ", "_").lower()


def get_absolute_venv_path(venv_path: Path) -> Path:
    """
    Note:
    In Python 3.9 it would be safer to do:

        if venv_path.is_relative_to(settings.FRACTAL_TASKS_DIR):
            package_path = venv_path
        else:
            package_path = settings.FRACTAL_TASKS_DIR / venv_path
    """
    if venv_path.is_absolute():
        package_path = venv_path
    else:
        settings = Inject(get_settings)
        package_path = settings.FRACTAL_TASKS_DIR / venv_path
    return package_path


def get_collection_path(base: Path) -> Path:
    return base / "collection.json"


def _normalize_package_name(name: str) -> str:
    """
    Implement PyPa specifications for package-name normalization

    The name should be lowercased with all runs of the characters `.`, `-`,
    or `_` replaced with a single `-` character. This can be implemented in
    Python with the re module.
    (https://packaging.python.org/en/latest/specifications/name-normalization)

    Args:
        name: The non-normalized package name.

    Returns:
        The normalized package name.
    """
    return re.sub(r"[-_.]+", "-", name).lower()
