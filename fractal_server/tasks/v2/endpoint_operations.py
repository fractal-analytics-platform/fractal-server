import json
from pathlib import Path
from typing import Literal
from typing import Optional
from typing import Union
from zipfile import ZipFile

from ._TaskCollectPip import _TaskCollectPip
from .utils import _parse_wheel_filename
from .utils import get_python_interpreter_v2
from fractal_server.app.schemas.v2 import ManifestV2
from fractal_server.config import get_settings
from fractal_server.logger import get_logger
from fractal_server.syringe import Inject
from fractal_server.utils import execute_command


FRACTAL_PUBLIC_TASK_SUBDIR = ".fractal"


async def download_package(
    *,
    task_pkg: _TaskCollectPip,
    dest: Union[str, Path],
) -> Path:
    """
    Download package to destination and return wheel-file path.
    """
    interpreter = get_python_interpreter_v2(
        python_version=task_pkg.python_version
    )
    pip = f"{interpreter} -m pip"
    if task_pkg.package_version is None:
        package_and_version = f"{task_pkg.package_name}"
    else:
        package_and_version = (
            f"{task_pkg.package_name}=={task_pkg.package_version}"
        )
    cmd = f"{pip} download --no-deps {package_and_version} -d {dest}"
    stdout = await execute_command(command=cmd, cwd=Path("."))
    pkg_file = next(
        line.split()[-1] for line in stdout.split("\n") if "Saved" in line
    )
    return Path(pkg_file)


def _load_manifest_from_wheel(
    path: Path, wheel: ZipFile, logger_name: Optional[str] = None
) -> ManifestV2:
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
    if manifest_version == "2":
        pkg_manifest = ManifestV2(**manifest_dict)
        return pkg_manifest
    else:
        msg = f"Manifest version {manifest_version=} not supported"
        logger.error(msg)
        raise ValueError(msg)


def inspect_package(
    path: Path, logger_name: Optional[str] = None
) -> dict[Literal["pkg_version", "pkg_manifest"], str]:
    """
    Inspect task package to extract version and manifest

    Note that this only works with wheel files, which have a well-defined
    dist-info section. If we need to generalize to to tar.gz archives, we would
    need to go and look for `PKG-INFO`.

    Args:
        path: Path of the package wheel file.
        logger_name:

    Returns:
        A dictionary with keys `pkg_version` and `pkg_manifest`.
    """

    logger = get_logger(logger_name)

    if not path.as_posix().endswith(".whl"):
        raise ValueError(
            "Only wheel packages are supported in Fractal "
            f"(given {path.name})."
        )

    # Extract package name and version from wheel filename
    _info = _parse_wheel_filename(wheel_filename=path.name)
    pkg_version = _info["version"]

    # Read and validate task manifest
    logger.debug(f"Now reading manifest for {path.as_posix()}")
    with ZipFile(path) as wheel:
        pkg_manifest = _load_manifest_from_wheel(
            path, wheel, logger_name=logger_name
        )
    logger.debug("Manifest read correctly.")

    info = dict(
        pkg_version=pkg_version,
        pkg_manifest=pkg_manifest,
    )
    return info


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
    package_dir = f"{task_pkg.package_name}{task_pkg.package_version}"
    venv_path = settings.FRACTAL_TASKS_DIR / user / package_dir
    if create:
        venv_path.mkdir(exist_ok=False, parents=True)
    return venv_path
