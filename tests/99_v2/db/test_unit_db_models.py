from fractal_server.app.models import Project


async def test_project_version():
    project = Project(name="project")  # default is "v1"
    assert project.version == "v1"

    project = Project(name="project", version="v2")
    assert project.version == "v2"

    p = Project(name="project", version="v3")
    assert p.version is None

    p = Project(name="project", version=None)
    assert p.version is None
