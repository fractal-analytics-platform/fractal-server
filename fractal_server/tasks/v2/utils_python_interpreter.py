from typing import Literal

from fractal_server.config import get_settings
from fractal_server.syringe import Inject


def get_python_interpreter_v2(
    python_version: Literal["3.9", "3.10", "3.11", "3.12"],
) -> str:
    """
    Return the path to the Python interpreter

    Args:
        python_version: Python version

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
