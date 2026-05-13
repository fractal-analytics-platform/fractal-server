import pytest
from fastapi import HTTPException

from fractal_server.app.routes.aux._python_interpreter import (
    get_python_interpreter_or_422,
)
from fractal_server.app.routes.aux.pixi_version import check_pixi_version
from fractal_server.tasks.config import TasksPixiSettings


def test_get_python_interpreter_or_422(
    local_resource_profile_objects,
    current_py_version,
):
    resource, _ = local_resource_profile_objects
    get_python_interpreter_or_422(
        python_version=current_py_version, resource=resource
    )
    with pytest.raises(HTTPException):
        get_python_interpreter_or_422(python_version="1.2", resource=resource)


def test_check_pixi_version(
    local_resource_profile_objects,
    pixi: TasksPixiSettings,
):
    resource, _ = local_resource_profile_objects
    with pytest.raises(
        HTTPException,
        match="Pixi task collection is not available",
    ):
        check_pixi_version(pixi_version="0.1.2", resource=resource)

    resource.tasks_pixi_config = pixi.model_dump()

    with pytest.raises(
        HTTPException,
        match="Pixi version '0.1.2' is not available",
    ):
        check_pixi_version(pixi_version="0.1.2", resource=resource)

    check_pixi_version(pixi_version=pixi.default_version, resource=resource)
