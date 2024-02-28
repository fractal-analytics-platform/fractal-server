from fractal_server.app.models import Project


async def test_project_version_attribute(db):
    project1 = Project(name="project")  # model default is "v2"
    assert project1.version == "v2"

    project2 = Project(name="project", version="v1")
    assert project2.version == "v1"

    project3 = Project(name="project", version="anystring")
    assert project3.version == "anystring"

    project_none = Project(name="project", version=None)
    assert project_none.version is None

    db.add(project1)
    db.add(project2)
    db.add(project3)
    db.add(project_none)
    await db.commit()

    assert project_none.version == "v2"  # DB default is "v2"
