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
from pathlib import Path
from typing import Optional
from typing import Union
from zipfile import ZipFile

from ..app.schemas import ManifestV1
from ..app.schemas import TaskCollectStatus
from ..app.schemas import TaskCreate
from ..config import get_settings
from ..logger import get_logger
from ..syringe import Inject
from ..utils import execute_command
from .naming import _normalize_package_name
from fractal_server.tasks.background_operations import (
    _create_venv_install_package,
)
from fractal_server.tasks.utils import _TaskCollectPip
from fractal_server.tasks.utils import get_python_interpreter


FRACTAL_PUBLIC_TASK_SUBDIR = ".fractal"


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
    normalized_package = _normalize_package_name(task_pkg.package)
    package_dir = f"{normalized_package}{task_pkg.package_version}"
    venv_path = settings.FRACTAL_TASKS_DIR / user / package_dir
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

    Note: package name is normalized via `_normalize_package_name`.

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
    pkg_name = _normalize_package_name(pkg_name)

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

    # Normalize package name
    task_pkg.package_name = _normalize_package_name(task_pkg.package_name)
    task_pkg.package = _normalize_package_name(task_pkg.package)

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
            task_name_slug = slugify_task_name(t.name)
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
