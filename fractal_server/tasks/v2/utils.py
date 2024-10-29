from typing import Literal

from ..utils import normalize_package_name
from fractal_server.config import get_settings
from fractal_server.logger import get_logger
from fractal_server.syringe import Inject


def get_python_interpreter_v2(
    python_version: Literal["3.9", "3.10", "3.11", "3.12"]
) -> str:
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

    if python_version not in ["3.9", "3.10", "3.11", "3.12"]:
        raise ValueError(f"Invalid {python_version=}.")

    settings = Inject(get_settings)
    version_underscore = python_version.replace(".", "_")
    key = f"FRACTAL_TASKS_PYTHON_{version_underscore}"
    value = getattr(settings, key)
    if value is None:
        raise ValueError(f"Requested {python_version=}, but {key}={value}.")
    return value


def _parse_wheel_filename(wheel_filename: str) -> dict[str, str]:
    """
    Extract distribution and version from a wheel filename.

    The structure of a wheel filename is fixed, and it must start with
    `{distribution}-{version}` (see
    https://packaging.python.org/en/latest/specifications/binary-distribution-format
    ).

    Note that we transform exceptions in `ValueError`s, since this function is
    also used within Pydantic validators.
    """
    if "/" in wheel_filename:
        raise ValueError(
            "[_parse_wheel_filename] Input must be a filename, not a full "
            f"path (given: {wheel_filename})."
        )
    try:
        parts = wheel_filename.split("-")
        return dict(distribution=parts[0], version=parts[1])
    except Exception as e:
        raise ValueError(
            f"Invalid {wheel_filename=}. Original error: {str(e)}."
        )


def compare_package_names(
    *,
    pkg_name_pip_show: str,
    pkg_name_task_group: str,
    logger_name: str,
) -> None:
    """
    Compare the package names from `pip show` and from the db.
    """
    logger = get_logger(logger_name)

    if pkg_name_pip_show == pkg_name_task_group:
        return

    logger.warning(
        f"Package name mismatch: "
        f"{pkg_name_task_group=}, {pkg_name_pip_show=}."
    )
    normalized_pkg_name_pip = normalize_package_name(pkg_name_pip_show)
    normalized_pkg_name_taskgroup = normalize_package_name(pkg_name_task_group)
    if normalized_pkg_name_pip != normalized_pkg_name_taskgroup:
        error_msg = (
            f"Package name mismatch persists, after normalization: "
            f"{pkg_name_task_group=}, "
            f"{pkg_name_pip_show=}."
        )
        logger.error(error_msg)
        raise ValueError(error_msg)
