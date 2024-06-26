from typing import Literal

from fractal_server.config import get_settings
from fractal_server.syringe import Inject


def get_python_interpreter_v2(
    version: Literal["3.9", "3.10", "3.11", "3.12"]
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

    if version not in ["3.9", "3.10", "3.11", "3.12"]:
        raise ValueError(f"Invalid {version=}.")

    settings = Inject(get_settings)
    version_underscore = version.replace(".", "_")
    key = f"FRACTAL_TASKS_PYTHON_{version_underscore}"
    value = getattr(settings, key)
    if value is None:
        raise ValueError(f"Requested {version=}, but {key}={value}.")
    return value


def _parse_wheel_filename(wheel_filename: str) -> dict[str, str]:
    """
    Note that the first part of a wheel filename is `{distribution}-{version}`
    (see
    https://packaging.python.org/en/latest/specifications/binary-distribution-format
    ).
    """
    parts = wheel_filename.split("-")
    return dict(distribution=parts[0], version=parts[1])
