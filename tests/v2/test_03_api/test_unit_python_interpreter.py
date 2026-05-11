import pytest
from fastapi import HTTPException

from fractal_server.app.routes.aux._python_interpreter import (
    get_python_interpreter_or_422,
)


def test_get_python_interpreter_or_422(
    local_resource_profile_db,
    current_py_version,
):
    resource, _ = local_resource_profile_db
    get_python_interpreter_or_422(
        python_version=current_py_version, resource=resource
    )
    with pytest.raises(HTTPException):
        get_python_interpreter_or_422(python_version="1.2", resource=resource)
