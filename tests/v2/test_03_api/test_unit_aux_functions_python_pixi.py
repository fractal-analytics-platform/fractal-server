import pytest
from fastapi import HTTPException

from fractal_server.app.routes.aux._python_interpreter import (
    get_python_interpreter_or_422,
)
from fractal_server.app.routes.aux.pixi_version import get_pixi_version_or_422
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


def test_get_pixi_version_or_422(local_resource_profile_objects):
    default_version = "0.54.1"
    another_version = "10.11.12"

    pixi = TasksPixiSettings(
        default_version=default_version,
        versions={
            default_version: f"/something/{default_version}",
            another_version: f"/something/{another_version}",
        },
    )

    resource, _ = local_resource_profile_objects
    with pytest.raises(
        HTTPException,
        match="Pixi task collection is not available",
    ):
        get_pixi_version_or_422(pixi_version="0.1.2", resource=resource)

    resource.tasks_pixi_config = pixi.model_dump()

    with pytest.raises(
        HTTPException,
        match="Pixi version '0.1.2' is not available",
    ):
        get_pixi_version_or_422(pixi_version="0.1.2", resource=resource)

    assert (
        get_pixi_version_or_422(pixi_version=another_version, resource=resource)
        == another_version
    )

    assert (
        get_pixi_version_or_422(pixi_version=None, resource=resource)
        == default_version
    )
