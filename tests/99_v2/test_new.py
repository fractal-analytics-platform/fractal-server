from devtools import debug

from fractal_server.app.models import Project
from fractal_server.app.models.v2 import DatasetV2
from fractal_server.app.schemas.v2 import DatasetCreateV2
from fractal_server.app.schemas.v2 import DatasetReadV2  # noqa F401
from fractal_server.app.schemas.v2 import DatasetUpdateV2  # noqa F401


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


async def test_unit_dataset_v2(db):

    project = Project(name="project")
    debug(project)
    db.add(project)
    await db.commit()

    # Create
    dataset_create = DatasetCreateV2(name="name")
    debug(dataset_create)

    dataset = DatasetV2(**dataset_create.dict(), project_id=project.id)
    debug(dataset)

    db.add(dataset)
    await db.commit()

    # Read
    debug(dataset)
    debug(dataset.project)

    dataset_read = DatasetReadV2(
        **dataset.model_dump(), project=dataset.project
    )
    debug(dataset_read)

    # Update
    dataset_update = DatasetUpdateV2(name="new name")
    debug(dataset_update)

    for key, value in dataset_update.dict(exclude_unset=True).items():
        setattr(dataset, key, value)

    db.add(dataset)
    await db.commit()
    debug(dataset)
