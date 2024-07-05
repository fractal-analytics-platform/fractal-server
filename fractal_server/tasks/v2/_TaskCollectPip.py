import logging
from pathlib import Path
from typing import Optional

from pydantic import BaseModel
from pydantic import Extra
from pydantic import Field
from pydantic import root_validator
from pydantic import validator

from fractal_server.app.schemas._validators import valdictkeys
from fractal_server.app.schemas._validators import valstr
from fractal_server.app.schemas.v2 import ManifestV2
from fractal_server.tasks.utils import _normalize_package_name
from fractal_server.tasks.v2.utils import _parse_wheel_filename


class _TaskCollectPip(BaseModel, extra=Extra.forbid):
    """
    Internal task-collection model.

    This model is similar to the API request-body model (`TaskCollectPip`), but
    with enough differences that we keep them separated (and they do not have a
    common base).

    Attributes:
        package: Either a PyPI package name or the absolute path to a wheel
            file.
        package_name: The actual normalized name of the package, which is set
            internally through a validator.
        package_version: Package version. For local packages, it is set
            internally through a validator.
    """

    package: str
    package_name: str
    python_version: str
    package_extras: Optional[str] = None
    pinned_package_versions: dict[str, str] = Field(default_factory=dict)
    package_version: Optional[str] = None
    package_path: Optional[Path] = None
    package_manifest: Optional[ManifestV2] = None

    _pinned_package_versions = validator(
        "pinned_package_versions", allow_reuse=True
    )(valdictkeys("pinned_package_versions"))
    _package_extras = validator("package_extras", allow_reuse=True)(
        valstr("package_extras")
    )
    _python_version = validator("python_version", allow_reuse=True)(
        valstr("python_version")
    )

    @property
    def is_local_package(self) -> bool:
        return bool(self.package_path)

    @root_validator(pre=True)
    def set_package_info(cls, values):
        """
        Depending on whether the package is a local wheel file or a PyPI
        package, set some of its metadata.
        """
        if "/" in values["package"]:
            # Local package: parse wheel filename
            package_path = Path(values["package"])
            if not package_path.is_absolute():
                raise ValueError("Package path must be absolute")
            if not package_path.exists():
                logging.warning(
                    f"Package {package_path} does not exist locally."
                )
            values["package_path"] = package_path
            wheel_metadata = _parse_wheel_filename(package_path.name)
            values["package_name"] = _normalize_package_name(
                wheel_metadata["distribution"]
            )
            values["package_version"] = wheel_metadata["version"]
        else:
            # Remote package: use `package` as `package_name`
            _package = values["package"]
            if _package.endswith(".whl"):
                raise ValueError(
                    f"ERROR: package={_package} ends with '.whl' "
                    "but it is not the absolute path to a wheel file."
                )
            values["package_name"] = _normalize_package_name(values["package"])
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
        python_version = f"py{self.python_version}"

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
        if not self.package_version:
            raise ValueError("`package_version` attribute is not set")
        if not self.package_manifest:
            raise ValueError("`package_manifest` attribute is not set")
