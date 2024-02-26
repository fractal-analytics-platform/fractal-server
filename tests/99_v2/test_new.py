from devtools import debug

from fractal_server.app.models import Project
from fractal_server.app.models.v2 import DatasetV2
from fractal_server.app.schemas.v2 import DatasetCreateV2
from fractal_server.app.schemas.v2 import DatasetReadV2  # noqa F401
from fractal_server.app.schemas.v2 import DatasetUpdateV2  # noqa F401


async def test_project_version_attribute(db):
    project1 = Project(name="project")  # model default is "v1"
    assert project1.version == "v1"

    project2 = Project(name="project", version="v2")
    assert project2.version == "v2"

    project3 = Project(name="project", version="anystring")
    assert project3.version == "anystring"

    project_none = Project(name="project", version=None)
    assert project_none.version is None

    db.add(project1)
    db.add(project2)
    db.add(project3)
    db.add(project_none)
    await db.commit()

    assert project_none.version == "v1"  # DB default is "v1"


async def test_unit_dataset_v2(db):

    project = Project(name="project")
    debug(project)
    db.add(project)
    await db.commit()

    dataset = DatasetCreateV2(
        project_id=project.id,
        name="ds",
        history=[],
        images=[],
        filters=[],
    )
    debug(dataset)

    db_dataset = DatasetV2(**dataset.dict(), project_id=project.id)
    debug(db_dataset)
    db.add(db_dataset)
    await db.commit()
