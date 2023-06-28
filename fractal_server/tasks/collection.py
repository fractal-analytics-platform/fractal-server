# Copyright 2022 (C) Friedrich Miescher Institute for Biomedical Research and
# University of Zurich
#
# Original authors:
# Jacopo Nespolo <jacopo.nespolo@exact-lab.it>
# Tommaso Comparin <tommaso.comparin@exact-lab.it>
# Yuri Chiucconi <yuri.chiucconi@exact-lab.it>
#
# This file is part of Fractal and was originally developed by eXact lab S.r.l.
# <exact-lab.it> under contract with Liberali Lab from the Friedrich Miescher
# Institute for Biomedical Research and Pelkmans Lab from the University of
# Zurich.
"""
This module takes care of installing tasks so that fractal can execute them

Tasks are installed under `Settings.FRACTAL_TASKS_DIR/{username}`, with
`username = ".fractal"`.
"""
import json
import shutil
import sys
from pathlib import Path
from typing import Optional
from typing import Union
from zipfile import ZipFile

from pydantic import root_validator

from ..common.schemas import ManifestV1
from ..common.schemas import TaskCollectPip
from ..common.schemas import TaskCollectStatus
from ..common.schemas import TaskCreate
from ..config import get_settings
from ..logger import get_logger
from ..syringe import Inject
from ..utils import execute_command


FRACTAL_PUBLIC_TASK_SUBDIR = ".fractal"


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
        package_path = settings.FRACTAL_TASKS_DIR / venv_path  # type: ignore
    return package_path


def get_collection_path(base: Path) -> Path:
    return base / "collection.json"


def get_log_path(base: Path) -> Path:
    return base / "collection.log"


def get_collection_log(venv_path: Path) -> str:
    package_path = get_absolute_venv_path(venv_path)
    log_path = get_log_path(package_path)
    log = log_path.open().read()
    return log


def get_collection_data(venv_path: Path) -> TaskCollectStatus:
    package_path = get_absolute_venv_path(venv_path)
    collection_path = get_collection_path(package_path)
    with collection_path.open() as f:
        data = json.load(f)
    return TaskCollectStatus(**data)


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
    def package_source(self):
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
                self.package_name,
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


def create_package_dir_pip(
    *,
    task_pkg: _TaskCollectPip,
    create: bool = True,
) -> Path:
    """
    Create venv folder for a task package and return corresponding Path object
    """
    settings = Inject(get_settings)
    user = FRACTAL_PUBLIC_TASK_SUBDIR
    if task_pkg.package_version is None:
        raise ValueError(
            f"Cannot create venv folder for package `{task_pkg.package}` "
            "with `version=None`."
        )
    package_dir = f"{task_pkg.package}{task_pkg.package_version}"
    venv_path = settings.FRACTAL_TASKS_DIR / user / package_dir  # type: ignore
    if create:
        venv_path.mkdir(exist_ok=False, parents=True)
    return venv_path


async def download_package(
    *,
    task_pkg: _TaskCollectPip,
    dest: Union[str, Path],
):
    """
    Download package to destination
    """
    interpreter = get_python_interpreter(version=task_pkg.python_version)
    pip = f"{interpreter} -m pip"
    version = (
        f"=={task_pkg.package_version}" if task_pkg.package_version else ""
    )
    package_and_version = f"{task_pkg.package}{version}"
    cmd = f"{pip} download --no-deps {package_and_version} -d {dest}"
    stdout = await execute_command(command=cmd, cwd=Path("."))
    pkg_file = next(
        line.split()[-1] for line in stdout.split("\n") if "Saved" in line
    )
    return Path(pkg_file)


def _load_manifest_from_wheel(
    path: Path, wheel: ZipFile, logger_name: Optional[str] = None
) -> ManifestV1:
    logger = get_logger(logger_name)
    namelist = wheel.namelist()
    try:
        manifest = next(
            name for name in namelist if "__FRACTAL_MANIFEST__.json" in name
        )
    except StopIteration:
        msg = f"{path.as_posix()} does not include __FRACTAL_MANIFEST__.json"
        logger.error(msg)
        raise ValueError(msg)
    with wheel.open(manifest) as manifest_fd:
        manifest_dict = json.load(manifest_fd)
    manifest_version = str(manifest_dict["manifest_version"])
    if manifest_version == "1":
        pkg_manifest = ManifestV1(**manifest_dict)
        return pkg_manifest
    else:
        msg = f"Manifest version {manifest_version=} not supported"
        logger.error(msg)
        raise ValueError(msg)


def inspect_package(path: Path, logger_name: Optional[str] = None) -> dict:
    """
    Inspect task package to extract version, name and manifest

    Note that this only works with wheel files, which have a well-defined
    dist-info section. If we need to generalize to to tar.gz archives, we would
    need to go and look for `PKG-INFO`.

    Note: package name is normalized by replacing `{-,.}` with `_`.

    Args:
        path: Path
            the path in which the package is saved

    Returns:
        version_manifest: A dictionary containing `version`, the version of the
        pacakge, and `manifest`, the Fractal manifest object relative to the
        tasks.
    """

    logger = get_logger(logger_name)

    if not path.as_posix().endswith(".whl"):
        raise ValueError(
            f"Only wheel packages are supported, given {path.as_posix()}."
        )

    with ZipFile(path) as wheel:
        namelist = wheel.namelist()

        # Read and validate task manifest
        logger.debug(f"Now reading manifest for {path.as_posix()}")
        pkg_manifest = _load_manifest_from_wheel(
            path, wheel, logger_name=logger_name
        )
        logger.debug("Manifest read correctly.")

        # Read package name and version from *.dist-info/METADATA
        logger.debug(
            f"Now reading package name and version for {path.as_posix()}"
        )
        metadata = next(
            name for name in namelist if "dist-info/METADATA" in name
        )
        with wheel.open(metadata) as metadata_fd:
            meta = metadata_fd.read().decode("utf-8")
            pkg_name = next(
                line.split()[-1]
                for line in meta.splitlines()
                if line.startswith("Name")
            )
            pkg_version = next(
                line.split()[-1]
                for line in meta.splitlines()
                if line.startswith("Version")
            )
        logger.debug("Package name and version read correctly.")

    # Normalize package name:
    pkg_name = pkg_name.replace("-", "_").replace(".", "_")

    info = dict(
        pkg_name=pkg_name,
        pkg_version=pkg_version,
        pkg_manifest=pkg_manifest,
    )
    return info


async def create_package_environment_pip(
    *,
    task_pkg: _TaskCollectPip,
    venv_path: Path,
    logger_name: str,
) -> list[TaskCreate]:
    """
    Create environment, install package, and prepare task list
    """

    logger = get_logger(logger_name)

    # Only proceed if package, version and manifest attributes are set
    task_pkg.check()

    try:

        logger.debug("Creating venv and installing package")
        python_bin, package_root = await _create_venv_install_package(
            path=venv_path,
            task_pkg=task_pkg,
            logger_name=logger_name,
        )
        logger.debug("Venv creation and package installation ended correctly.")

        # Prepare task_list with appropriate metadata
        logger.debug("Creating task list from manifest")
        task_list = []
        for t in task_pkg.package_manifest.task_list:
            # Fill in attributes for TaskCreate
            task_executable = package_root / t.executable
            cmd = f"{python_bin.as_posix()} {task_executable.as_posix()}"
            task_name_slug = t.name.replace(" ", "_").lower()
            task_source = f"{task_pkg.package_source}:{task_name_slug}"
            if not task_executable.exists():
                raise FileNotFoundError(
                    f"Cannot find executable `{task_executable}` "
                    f"for task `{t.name}`"
                )
            manifest = task_pkg.package_manifest
            if manifest.has_args_schemas:
                additional_attrs = dict(
                    args_schema_version=manifest.args_schema_version
                )
            else:
                additional_attrs = {}
            this_task = TaskCreate(
                **t.dict(),
                command=cmd,
                version=task_pkg.package_version,
                **additional_attrs,
                source=task_source,
            )
            task_list.append(this_task)
        logger.debug("Task list created correctly")
    except Exception as e:
        logger.error("Task manifest loading failed")
        raise e
    return task_list


async def _create_venv_install_package(
    *,
    task_pkg: _TaskCollectPip,
    path: Path,
    logger_name: str,
) -> tuple[Path, Path]:
    """Create venv and install package

    Args:
        path: the directory in which to create the environment
        task_pkg: object containing the different metadata required to install
            the package

    Returns:
        python_bin: path to venv's python interpreter
        package_root: the location of the package manifest
    """
    python_bin = await _init_venv(
        path=path,
        python_version=task_pkg.python_version,
        logger_name=logger_name,
    )
    package_root = await _pip_install(
        venv_path=path, task_pkg=task_pkg, logger_name=logger_name
    )
    return python_bin, package_root


async def _init_venv(
    *,
    path: Path,
    python_version: Optional[str] = None,
    logger_name: str,
) -> Path:
    """
    Set a virtual environment at `path/venv`

    Args:
        path : Path
            path to directory in which to set up the virtual environment
        python_version : default=None
            Python version the virtual environment will be based upon

    Returns:
        python_bin : Path
            path to python interpreter
    """
    interpreter = get_python_interpreter(version=python_version)
    await execute_command(
        cwd=path,
        command=f"{interpreter} -m venv venv",
        logger_name=logger_name,
    )
    return path / "venv/bin/python"


async def _pip_install(
    venv_path: Path,
    task_pkg: _TaskCollectPip,
    logger_name: str,
) -> Path:
    """
    Install package in venv

    Returns:
        package_root : Path
            the location of the package manifest
    """

    logger = get_logger(logger_name)

    pip = venv_path / "venv/bin/pip"

    extras = f"[{task_pkg.package_extras}]" if task_pkg.package_extras else ""

    if task_pkg.is_local_package:
        pip_install_str = f"{task_pkg.package_path.as_posix()}{extras}"
    else:
        version_string = (
            f"=={task_pkg.package_version}" if task_pkg.package_version else ""
        )
        pip_install_str = f"{task_pkg.package}{extras}{version_string}"

    cmd_install = f"{pip} install {pip_install_str}"
    cmd_inspect = f"{pip} show {task_pkg.package}"

    await execute_command(
        cwd=venv_path,
        command=f"{pip} install --upgrade pip",
        logger_name=logger_name,
    )
    await execute_command(
        cwd=venv_path, command=cmd_install, logger_name=logger_name
    )
    if task_pkg.pinned_package_versions:
        for (
            pinned_pkg_name,
            pinned_pkg_version,
        ) in task_pkg.pinned_package_versions.items():

            logger.debug(
                "Specific version required: "
                f"{pinned_pkg_name}=={pinned_pkg_version}"
            )
            logger.debug(
                "Preliminary check: verify that "
                f"{pinned_pkg_version} is already installed"
            )
            stdout_inspect = await execute_command(
                cwd=venv_path,
                command=f"{pip} show {pinned_pkg_name}",
                logger_name=logger_name,
            )
            current_version = next(
                line.split()[-1]
                for line in stdout_inspect.split("\n")
                if line.startswith("Version:")
            )
            if current_version != pinned_pkg_version:
                logger.debug(
                    f"Currently installed version of {pinned_pkg_name} "
                    f"({current_version}) differs from pinned version "
                    f"({pinned_pkg_version}); "
                    f"install version {pinned_pkg_version}."
                )
                await execute_command(
                    cwd=venv_path,
                    command=(
                        f"{pip} install "
                        f"{pinned_pkg_name}=={pinned_pkg_version}"
                    ),
                    logger_name=logger_name,
                )
            else:
                logger.debug(
                    f"Currently installed version of {pinned_pkg_name} "
                    f"({current_version}) already matches the pinned version."
                )

    # Extract package installation path from `pip show`
    stdout_inspect = await execute_command(
        cwd=venv_path, command=cmd_inspect, logger_name=logger_name
    )

    location = Path(
        next(
            line.split()[-1]
            for line in stdout_inspect.split("\n")
            if line.startswith("Location:")
        )
    )
    package_root = location / task_pkg.package.replace("-", "_")
    if not package_root.exists():
        raise RuntimeError(
            "Could not determine package installation location."
        )
    return package_root
