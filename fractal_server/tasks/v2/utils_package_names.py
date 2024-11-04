import re

from fractal_server.logger import get_logger


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


def normalize_package_name(name: str) -> str:
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
