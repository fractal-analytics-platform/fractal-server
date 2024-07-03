from typing import Literal

from fractal_server.config import get_settings
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
    try:
        parts = wheel_filename.split("-")
        return dict(distribution=parts[0], version=parts[1])
    except Exception as e:
        raise ValueError(
            f"Invalid {wheel_filename=}. Original error: {str(e)}."
        )
