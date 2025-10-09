import pytest

from fractal_server.app.models import Profile
from fractal_server.app.models import Resource
from fractal_server.tasks.v2.utils_python_interpreter import (
    get_python_interpreter,
)


async def test_get_python_interpreter(
    local_resource_profile_objects: tuple[Resource, Profile],
):
    resource, profile = local_resource_profile_objects[:]

    PYTHON311 = "/fake-3.11/bin/python"
    PYTHON312 = "/fake-3.12/bin/python"

    resource.tasks_python_config = {
        "default_version": "3.11",
        "versions": {
            "3.11": PYTHON311,
            "3.12": PYTHON312,
        },
    }

    assert (
        get_python_interpreter(
            python_version="3.11",
            resource=resource,
        )
        == PYTHON311
    )

    with pytest.raises(
        ValueError,
        match="is not available",
    ):
        get_python_interpreter(
            python_version="3.5",
            resource=resource,
        )
