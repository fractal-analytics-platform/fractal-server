import json
from pathlib import Path
from typing import Optional
from typing import Union
from zipfile import ZipFile

from ..utils import _normalize_package_name
from ._TaskCollectPip import _TaskCollectPip as _TaskCollectPipV1
from .utils import get_python_interpreter_v1
from fractal_server.app.schemas.v1 import ManifestV1
from fractal_server.config import get_settings
from fractal_server.logger import get_logger
from fractal_server.syringe import Inject
from fractal_server.utils import execute_command


FRACTAL_PUBLIC_TASK_SUBDIR = ".fractal"


async def download_package(
    *,
    task_pkg: _TaskCollectPipV1,
    dest: Union[str, Path],
) -> Path:
    """
    Download package to destination
    """
    interpreter = get_python_interpreter_v1(version=task_pkg.python_version)
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


def create_package_dir_pip(
    *,
    task_pkg: _TaskCollectPipV1,
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
