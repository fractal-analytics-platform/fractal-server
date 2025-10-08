from fractal_server.app.models import Resource


def get_python_interpreter(
    python_version: str,
    resource: Resource,
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

    python_path = resource.tasks_python_config["versions"].get(python_version)
    if python_path is None:
        raise ValueError(f"Requested {python_version=} is not available.")
    return python_path
