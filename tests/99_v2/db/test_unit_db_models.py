import pytest

from fractal_server.app.models import Project


async def test_project_version():
    project = Project(name="project")  # default is "v1"
    assert project.version == "v1"

    project = Project(name="project", version="v2")
    assert project.version == "v2"

    with pytest.raises(RuntimeError):
        Project(name="project", version="v3")

    with pytest.raises(RuntimeError):
        Project(name="project", version=None)
