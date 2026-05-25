from fastapi import HTTPException
from fastapi import status

from fractal_server.app.models import Resource
from fractal_server.tasks.v2.utils_python_interpreter import (
    get_python_interpreter,
)


def get_python_interpreter_or_422(
    *,
    python_version: str,
    resource: Resource,
) -> str:
    """
    Verify that the requested Python version is available for this resource.

    Args:
        python_version:
        resource:
    """
    try:
        return get_python_interpreter(
            python_version=python_version,
            resource=resource,
        )
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail=(
                f"Python version {python_version} "
                "is not available for this Fractal resource."
            ),
        )
