import pytest
from devtools import debug
from pydantic import ValidationError

from fractal_server.app.models.v2 import DatasetV2
from fractal_server.app.models.v2 import ProjectV2
from fractal_server.app.schemas.v2 import DatasetCreateV2
from fractal_server.app.schemas.v2 import DatasetReadV2
from fractal_server.app.schemas.v2 import DatasetUpdateV2


async def test_schemas_dataset_v2(db):

    project = ProjectV2(name="project")
    debug(project)
    db.add(project)
    await db.commit()

    # Create

    with pytest.raises(ValidationError):
        # Non-scalar attribute
        DatasetCreateV2(
            name="name",
            zarr_dir="/zarr",
            filters={"attributes": {"x": [1, 0]}},
        )
    with pytest.raises(ValidationError):
        # Non-boolean types
        DatasetCreateV2(
            name="name", zarr_dir="/zarr", filters={"types": {"a": "b"}}
        )

    dataset_create = DatasetCreateV2(
        name="name",
        filters={"attributes": {"x": 10}},
        zarr_dir="/tmp/",
    )
    assert dataset_create.zarr_dir == "/tmp"
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

    await db.commit()
    assert dataset.name == "new name"
    debug(dataset)
